use std::{path::PathBuf};

use anyhow::{Error, Result};
use async_trait::async_trait;
use clap::{Parser};
use serde::{Deserialize, Serialize};
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
    // engine: TriggerAppEngine<Self>,
    // queue_components: HashMap<String, String>,  // Queue URL -> component ID
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqsTriggerConfig {
    pub component: String,
    pub queue_url: String,
    // TODO: max num messages?  visibility timeout?  wait time?
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

    fn new(_engine: TriggerAppEngine<Self>) -> Result<Self> {
        // let queue_components = engine
        //     .trigger_configs()
        //     .map(|(_, config)| (config.queue_url.clone(), config.component.clone()))
        //     .collect();

        Ok(Self {
            // engine,
            // queue_components,
        })
    }

    async fn run(self, _config: Self::RunConfig) -> Result<()> {
        let ctrlc = tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            // std::process::exit(0);
        });

        let bindir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/release");
        let args: Vec<_> = std::env::args().skip(1).collect();

        //println!("{:?}", args);

        let mut executor = tokio::process::Command::new(bindir.join("trigger-sqs-executor")).args(&args).spawn()?;
        let mut eventer = tokio::process::Command::new(bindir.join("trigger-sqs-eventer")).args(&args).spawn()?;

        tokio::select! {
            r = executor.wait() => {
                println!("executor exited with code {:?}", r?.code());
            }
            r = eventer.wait() => {
                println!("eventer exited with code {:?}", r?.code());
            }
            _ = ctrlc => {
                _ = eventer.kill().await;
                _ = executor.kill().await;
            }
        };

        Ok(())

        // let config = aws_config::load_from_env().await;

        // let client = aws_sdk_sqs::Client::new(&config);
        // let engine = Arc::new(self.engine);

        // let loops = self.queue_components.iter().map(|(queue_url, component)| {
        //     Self::start_receive_loop(engine.clone(), &client, queue_url, component)
        // });

        // let (r, _, rest) = futures::future::select_all(loops).await;
        // drop(rest);

        // r?
    }
}

impl SqsTrigger {
    // // TODO: would this work better returning a stream to allow easy multiplexing etc?
    // fn start_receive_loop(engine: Arc<TriggerAppEngine<Self>>, client: &aws_sdk_sqs::Client, queue_url: &str, component: &str) -> tokio::task::JoinHandle<Result<()>> {
    //     let future = Self::receive(engine, client.clone(), queue_url.to_owned(), component.to_owned());
    //     tokio::task::spawn(future)
    // }

    // async fn receive(engine: Arc<TriggerAppEngine<Self>>, client: aws_sdk_sqs::Client, queue_url: String, component: String) -> Result<()> {
    //     loop {
    //         println!("Attempting to receive from {queue_url}...");
    //         // Okay seems like we have to explicitly ask for the attr and message_attr names we want
    //         let rmo = client
    //             .receive_message()
    //             .queue_url(&queue_url)
    //             .attribute_names(aws_sdk_sqs::model::QueueAttributeName::All)
    //             .send()
    //             .await?;
    //         if let Some(msgs) = rmo.messages() {
    //             println!("...received from {queue_url}");
    //             for m in msgs {
    //                 let empty = HashMap::new();
    //                 let attrs = m.attributes()
    //                     .unwrap_or(&empty)
    //                     .iter()
    //                     .map(|(k, v)| sqs::MessageAttribute { name: k.as_str(), value: sqs::MessageAttributeValue::Str(v.as_str()), data_type: None })
    //                     .collect::<Vec<_>>();
    //                 let message = sqs::Message {
    //                     id: m.message_id(),
    //                     message_attributes: &attrs,
    //                     body: m.body(),
    //                 };
    //                 let action = Self::execute(&engine, &component, message).await?;
    //                 println!("...action is to {action:?}");
    //                 if action == sqs::MessageAction::Delete {
    //                     if let Some(receipt_handle) = m.receipt_handle() {
    //                         match client.delete_message().queue_url(&queue_url).receipt_handle(receipt_handle).send().await {
    //                             Ok(_) => (),
    //                             Err(e) => eprintln!("TRIG: err deleting {receipt_handle}: {e:?}"),
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //         tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    //     }
    // }

    // async fn execute(engine: &Arc<TriggerAppEngine<Self>>, component_id: &str, message: sqs::Message<'_>) -> Result<sqs::MessageAction> {
    //     println!("Executing component {component_id}");
    //     let (instance, mut store) = engine.prepare_instance(component_id).await?;
    //     let sqs_engine = Sqs::new(&mut store, &instance, |data| data.as_mut())?;
    //     match sqs_engine.handle_queue_message(&mut store, message).await {
    //         Ok(Ok(action)) => Ok(action),
    //         // TODO: BUTTLOAD OF LOGGING
    //         // TODO: DETECT FATALNESS
    //         Ok(Err(_e)) => Ok(sqs::MessageAction::Leave),
    //         Err(_e) => Ok(sqs::MessageAction::Leave),
    //     }
    // }
}
