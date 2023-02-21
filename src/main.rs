use std::{collections::HashMap, sync::Arc};

use anyhow::{Error, Result};
use async_trait::async_trait;
use clap::{Parser};
use serde::{Deserialize, Serialize};
use sqs::Sqs;
use spin_trigger::{cli::{NoArgs, TriggerExecutorCommand}, TriggerAppEngine, TriggerExecutor};

wit_bindgen_wasmtime::import!({paths: ["sqs.wit"], async: *});

pub(crate) type RuntimeData = sqs::SqsData;

type Command = TriggerExecutorCommand<SqsTrigger>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let t = Command::parse();
    t.run().await
}

pub struct SqsTrigger {
    engine: TriggerAppEngine<Self>,
    queue_components: Vec<Component>, // HashMap<String, String>,  // Queue URL -> component ID
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqsTriggerConfig {
    pub component: String,
    pub queue_url: String,
    pub max_messages: Option<u32>,
    pub idle_wait_seconds: Option<u64>,
    // TODO: visibility timeout?
}

#[derive(Clone, Debug)]
struct Component {
    pub id: String,
    pub queue_url: String,
    pub max_messages: i32,  // Should be usize but AWS
    pub idle_wait: tokio::time::Duration,
    // TODO: visibility timeout?
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TriggerMetadata {
    r#type: String,
}

#[async_trait]
impl TriggerExecutor for SqsTrigger {
    const TRIGGER_TYPE: &'static str = "sqs";
    type RuntimeData = RuntimeData;
    type TriggerConfig = SqsTriggerConfig;
    type RunConfig = NoArgs;

    fn new(engine: TriggerAppEngine<Self>) -> Result<Self> {
        let queue_components = engine
            .trigger_configs()
            .map(|(_, config)| Component {
                id: config.component.clone(),
                queue_url: config.queue_url.clone(),
                max_messages: config.max_messages.unwrap_or(10).try_into().unwrap(),   // TODO: HA HA HA... YES!!!
                idle_wait: tokio::time::Duration::from_secs(config.idle_wait_seconds.unwrap_or(2)),
            })
            .collect();

        Ok(Self {
            engine,
            queue_components,
        })
    }

    async fn run(self, _config: Self::RunConfig) -> Result<()> {
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            std::process::exit(0);
        });

        let config = aws_config::load_from_env().await;

        let client = aws_sdk_sqs::Client::new(&config);
        let engine = Arc::new(self.engine);

        let loops = self.queue_components.iter().map(|component| {
            Self::start_receive_loop(engine.clone(), &client, component)
        });

        let (r, _, rest) = futures::future::select_all(loops).await;
        drop(rest);

        r?
    }
}

impl SqsTrigger {
    // TODO: would this work better returning a stream to allow easy multiplexing etc?
    fn start_receive_loop(engine: Arc<TriggerAppEngine<Self>>, client: &aws_sdk_sqs::Client, component: &Component) -> tokio::task::JoinHandle<Result<()>> {
        let future = Self::receive(engine, client.clone(), component.clone());
        tokio::task::spawn(future)
    }

    async fn receive(engine: Arc<TriggerAppEngine<Self>>, client: aws_sdk_sqs::Client, component: Component) -> Result<()> {
        loop {
            println!("Attempting to receive up to {} from {}...", component.max_messages, component.queue_url);
            // Okay seems like we have to explicitly ask for the attr and message_attr names we want
            let rmo = client
                .receive_message()
                .max_number_of_messages(component.max_messages)
                .queue_url(&component.queue_url)
                .attribute_names(aws_sdk_sqs::model::QueueAttributeName::All)
                .send()
                .await?;
            if let Some(msgs) = rmo.messages() {
                println!("...received {} message(s) from {}", msgs.len(), component.queue_url);
                for m in msgs {
                    let empty = HashMap::new();
                    let attrs = m.attributes()
                        .unwrap_or(&empty)
                        .iter()
                        .map(|(k, v)| sqs::MessageAttribute { name: k.as_str(), value: sqs::MessageAttributeValue::Str(v.as_str()), data_type: None })
                        .collect::<Vec<_>>();
                    let message = sqs::Message {
                        id: m.message_id(),
                        message_attributes: &attrs,
                        body: m.body(),
                    };
                    let action = Self::execute(&engine, &component.id, message).await?;
                    println!("...action is to {action:?}");
                    if action == sqs::MessageAction::Delete {
                        if let Some(receipt_handle) = m.receipt_handle() {
                            match client.delete_message().queue_url(&component.queue_url).receipt_handle(receipt_handle).send().await {
                                Ok(_) => (),
                                Err(e) => eprintln!("TRIG: err deleting {receipt_handle}: {e:?}"),
                            }
                        }
                    }
                }
            } else {
                tokio::time::sleep(component.idle_wait).await;
            }
        }
    }

    async fn execute(engine: &Arc<TriggerAppEngine<Self>>, component_id: &str, message: sqs::Message<'_>) -> Result<sqs::MessageAction> {
        println!("Executing component {component_id}");
        let (instance, mut store) = engine.prepare_instance(component_id).await?;
        let sqs_engine = Sqs::new(&mut store, &instance, |data| data.as_mut())?;
        match sqs_engine.handle_queue_message(&mut store, message).await {
            Ok(Ok(action)) => Ok(action),
            // TODO: BUTTLOAD OF LOGGING
            // TODO: DETECT FATALNESS
            Ok(Err(_e)) => Ok(sqs::MessageAction::Leave),
            Err(_e) => Ok(sqs::MessageAction::Leave),
        }
    }
}
