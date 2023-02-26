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
                        yield grpc_message;
    
                        // let request = tonic::Request::new(grpc_message);
    
                        // So it turns out we can't fire and forget in this model - the client gets dropped
                        // and it seems like that aborts the send or causes the receiver to abandon or something.
                        // Awaiting the response seems tedious but I guess it is async rather than blocking
                        // so maybe it's benign?  Or maybe if we use a shared long-lived client somehow
                        // then it will all Just Work.  WHO KNOWS.
                        //
                        // Again maybe if our client is long lived then it will Just Work.
                        //
                        // Huh, the tonic people tell us to clone it, which doesn't seem like it will keep
                        // things around.  Oh but maybe it will result in the channel being long-lived?
                        //
                        // It did not work.
                        //
                        // WAIT WHAT IF WE USED STREAMING

    
                        // let mut executor_client_2 = executor_client.clone();
                        // _ = executor_client_2.execute(request);  // We don't want to wait on a response - just send and forget

                        // let r = client.execute(request).await;  // for diagnostics
                        // println!("EVENTER: sent event, response was {:?}", r);
                    }
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
    
        };

        match executor_client.streaming_execute(stm).await {
            Ok(resp) => {
                let mut resp_stm = resp.into_inner();
                loop {
                    match resp_stm.message().await {
                        Ok(Some(r)) => println!("got a response from streaming server, {r:?}"),
                        Ok(None) => {
                            println!("TERM TERM TERM");
                            break;
                        },
                        Err(e) => {
                            println!("server sent an error {e:?}");
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
