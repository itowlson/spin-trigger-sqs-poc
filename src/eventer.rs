use std::{collections::HashMap};

use anyhow::{Error, Result};
use async_trait::async_trait;
use clap::{Parser};
use serde::{Deserialize, Serialize};
use spin_trigger::{cli::{NoArgs, TriggerExecutorCommand}, TriggerAppEngine, TriggerExecutor};

mod pb {
    tonic::include_proto!("sqs");
}

type Command = TriggerExecutorCommand<SqsEventer>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let t = Command::parse();
    t.run().await
}

pub struct SqsEventer {
    // engine: TriggerAppEngine<Self>,
    queue_components: HashMap<String, String>,  // Queue URL -> component ID
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqsEventerConfig {
    pub component: String,
    pub queue_url: String,
    // TODO: max num messages?  visibility timeout?  wait time?
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct EventerMetadata {
    r#type: String,
}

#[async_trait]
impl TriggerExecutor for SqsEventer {
    const TRIGGER_TYPE: &'static str = "sqs";
    type RuntimeData = ();
    type TriggerConfig = SqsEventerConfig;
    type RunConfig = NoArgs;

    fn new(engine: TriggerAppEngine<Self>) -> Result<Self> {
        let queue_components = engine
            .trigger_configs()
            .map(|(_, config)| (config.queue_url.clone(), config.component.clone()))
            .collect();

        Ok(Self {
            // engine,
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

        let loops = self.queue_components.iter().map(|(queue_url, component)| {
            Self::start_receive_loop(&client, queue_url, component)
        });

        let (r, _, rest) = futures::future::select_all(loops).await;
        drop(rest);

        r?
    }
}

impl SqsEventer {
    // TODO: would this work better returning a stream to allow easy multiplexing etc?
    fn start_receive_loop(client: &aws_sdk_sqs::Client, queue_url: &str, component: &str) -> tokio::task::JoinHandle<Result<()>> {
        let future = Self::receive(client.clone(), queue_url.to_owned(), component.to_owned());
        tokio::task::spawn(future)
    }

    async fn receive(client: aws_sdk_sqs::Client, queue_url: String, component: String) -> Result<()> {
        loop {
            println!("Attempting to receive from {queue_url}...");
            // Okay seems like we have to explicitly ask for the attr and message_attr names we want
            let rmo = client
                .receive_message()
                .queue_url(&queue_url)
                .attribute_names(aws_sdk_sqs::model::QueueAttributeName::All)
                .send()
                .await?;
            if let Some(msgs) = rmo.messages() {
                println!("...received from {queue_url}");
                for m in msgs {
                    let grpc_message = pb::SqsMessage {
                        queue_url: queue_url.clone(),
                        component: component.clone(),
                        id: m.message_id().unwrap_or("HOW DO I MAKE AN OPTION").to_owned(),
                        body: m.body().unwrap_or("HOW DO I MAKE AN OPTION").to_owned(),
                        attributes: vec![],  // TODO: <-- this
                        receipt_handle: m.receipt_handle().unwrap_or("HOW DO I MAKE AN OPTION").to_owned(),
                    };
                    let mut client = pb::sqs_message_executor_client::SqsMessageExecutorClient::connect("http://[::1]:50051").await?;

                    let request = tonic::Request::new(grpc_message);
                    _ = client.execute(request);  // We don't want to wait on a response - just send and forget
                    
                    // let empty = HashMap::new();
                    // let attrs = m.attributes()
                    //     .unwrap_or(&empty)
                    //     .iter()
                    //     .map(|(k, v)| sqs::MessageAttribute { name: k.as_str(), value: sqs::MessageAttributeValue::Str(v.as_str()), data_type: None })
                    //     .collect::<Vec<_>>();
                    // let message = sqs::Message {
                    //     id: m.message_id(),
                    //     message_attributes: &attrs,
                    //     body: m.body(),
                    // };
                    // let action = Self::execute(&engine, &component, message).await?;
                    // println!("...action is to {action:?}");
                    // if action == sqs::MessageAction::Delete {
                    //     if let Some(receipt_handle) = m.receipt_handle() {
                    //         match client.delete_message().queue_url(&queue_url).receipt_handle(receipt_handle).send().await {
                    //             Ok(_) => (),
                    //             Err(e) => eprintln!("TRIG: err deleting {receipt_handle}: {e:?}"),
                    //         }
                    //     }
                    // }
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    }
}
