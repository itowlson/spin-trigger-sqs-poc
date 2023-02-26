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
        
        println!("EVENTER: running");

        let config = aws_config::load_from_env().await;

        let client = aws_sdk_sqs::Client::new(&config);

        let loops = self.queue_components.iter().map(|(queue_url, _)| {
            Self::start_receive_loop(&client, queue_url)
        });

        let (r, _, rest) = futures::future::select_all(loops).await;
        drop(rest);

        r?
    }
}

impl SqsEventer {
    // TODO: would this work better returning a stream to allow easy multiplexing etc?
    fn start_receive_loop(client: &aws_sdk_sqs::Client, queue_url: &str) -> tokio::task::JoinHandle<Result<()>> {
        let future = Self::receive(client.clone(), queue_url.to_owned());
        tokio::task::spawn(future)
    }

    async fn receive(client: aws_sdk_sqs::Client, queue_url: String) -> Result<()> {
        let mut executor_client = pb::sqs_message_executor_client::SqsMessageExecutorClient::connect("http://[::1]:50051").await?;

        let stm = async_stream::stream! {
            loop {
                println!("EVENTER: Attempting to receive from {queue_url}...");
                // Okay seems like we have to explicitly ask for the attr and message_attr names we want
                let rmor = client
                    .receive_message()
                    .queue_url(&queue_url)
                    .attribute_names(aws_sdk_sqs::model::QueueAttributeName::All)
                    .send()
                    .await;
                let rmo = match rmor {
                    Ok(rmo) => rmo,
                    Err(_) => continue,
                };
                if let Some(msgs) = rmo.messages() {
                    println!("EVENTER: ...received from {queue_url}");
                    for m in msgs {
                        let grpc_message = pb::SqsMessage {
                            queue_url: queue_url.clone(),
                            id: m.message_id().unwrap_or("HOW DO I MAKE AN OPTION").to_owned(),
                            body: m.body().unwrap_or("HOW DO I MAKE AN OPTION").to_owned(),
                            attributes: vec![],  // TODO: <-- this
                            receipt_handle: m.receipt_handle().unwrap_or("HOW DO I MAKE AN OPTION").to_owned(),
                        };
                        println!("EVENTER: yielding event to stream {:?}", grpc_message.id);
                        yield grpc_message;
                    }
                } else {
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                }
            }
    
        };

        match executor_client.streaming_execute(stm).await {
            Ok(resp) => {
                let mut resp_stm = resp.into_inner();
                loop {
                    match resp_stm.message().await {
                        Ok(Some(r)) => println!("EVENTER: got a response: {r:?}"),
                        Ok(None) => {
                            println!("EVENTER: server is all outta connections. Hasta la vista");
                            break;
                        },
                        Err(e) => {
                            println!("EVENTER: server sent an error {e:?}");
                        }
                    }
                }
            },
            Err(e) => {
                println!("STREAMTASTROPHE");
                return Err(e.into());
            },
        };

        Ok(())
    }
}