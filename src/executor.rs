use std::{collections::HashMap, sync::Arc};

use anyhow::{Error, Result};
use async_trait::async_trait;
use clap::{Parser};
use serde::{Deserialize, Serialize};
use sqs::Sqs;
use spin_trigger::{cli::{NoArgs, TriggerExecutorCommand}, TriggerAppEngine, TriggerExecutor};

wit_bindgen_wasmtime::import!({paths: ["sqs.wit"], async: *});

pub(crate) type RuntimeData = sqs::SqsData;

mod pb {
    tonic::include_proto!("sqs");
}

type Command = TriggerExecutorCommand<SqsExecutor>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let t = Command::parse();
    t.run().await
}

pub struct SqsExecutor {
    engine: Arc<TriggerAppEngine<Self>>,
    queue_components: Arc<HashMap<String, String>>,  // Queue URL -> component ID
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqsExecutorConfig {
    pub component: String,
    pub queue_url: String,
    // TODO: max num messages?  visibility timeout?  wait time?
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ExecutorMetadata {
    r#type: String,
}

#[async_trait]
impl TriggerExecutor for SqsExecutor {
    const TRIGGER_TYPE: &'static str = "sqs";
    type RuntimeData = RuntimeData;
    type TriggerConfig = SqsExecutorConfig;
    type RunConfig = NoArgs;

    fn new(engine: TriggerAppEngine<Self>) -> Result<Self> {
        let queue_components = engine
            .trigger_configs()
            .map(|(_, config)| (config.queue_url.clone(), config.component.clone()))
            .collect();

        Ok(Self {
            engine: Arc::new(engine),
            queue_components: Arc::new(queue_components),
        })
    }

    async fn run(self, _config: Self::RunConfig) -> Result<()> {
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            std::process::exit(0);
        });

        println!("EXECUTOR: running");

        let addr = "[::1]:50051".parse().unwrap();
        println!("EXECUTOR: preparing to serve");
        tonic::transport::Server::builder()
            .add_service(pb::sqs_message_executor_server::SqsMessageExecutorServer::new(self))
            .serve(addr)
            .await?;

        println!("EXECUTOR: ended serve");

        Ok(())
    }
}

impl SqsExecutor {
    // async fn execute_wasm(&self, component_id: &str, message: sqs::Message<'_>) -> Result<sqs::MessageAction> {
    //     // println!("Executing component {component_id}");
    //     // let (instance, mut store) = self.engine.prepare_instance(component_id).await?;
    //     // let sqs_engine = Sqs::new(&mut store, &instance, |data| data.as_mut())?;
    //     // match sqs_engine.handle_queue_message(&mut store, message).await {
    //     //     Ok(Ok(action)) => Ok(action),
    //     //     // TODO: BUTTLOAD OF LOGGING
    //     //     // TODO: DETECT FATALNESS
    //     //     Ok(Err(_e)) => Ok(sqs::MessageAction::Leave),
    //     //     Err(_e) => Ok(sqs::MessageAction::Leave),
    //     // }
    // }

    // async fn execute_wasm_for_grpc_message(&self, message: &pb::SqsMessage) -> anyhow::Result<sqs::MessageAction> {
    //     // println!("EXECUTOR: processing GRPC message for {}", message.id);
    //     // let wit_message = sqs::Message {
    //     //     id: Some(&message.id),
    //     //     message_attributes: &vec![],
    //     //     body: Some(&message.body),
    //     // };
    //     // let component = self.queue_components.get(&message.queue_url).ok_or_else(|| tonic::Status::unknown(format!("No mapping for queue {}", message.queue_url)))?;
    //     // let result = self.execute_wasm(component, wit_message).await;
    //     // println!("EXECUTOR: processed GRPC message for {}", message.id);
    //     // result
    // }
}

async fn execute_wasm(engine: &Arc<TriggerAppEngine<SqsExecutor>>, component_id: &str, message: sqs::Message<'_>) -> Result<sqs::MessageAction> {
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

async fn execute_wasm_for_grpc_message(engine: &Arc<TriggerAppEngine<SqsExecutor>>, queue_components: &Arc<HashMap<String, String>>, message: &pb::SqsMessage) -> anyhow::Result<sqs::MessageAction> {
    println!("EXECUTOR: processing GRPC message for {}", message.id);
    let wit_message = sqs::Message {
        id: Some(&message.id),
        message_attributes: &vec![],
        body: Some(&message.body),
    };
    let component = queue_components.get(&message.queue_url).ok_or_else(|| tonic::Status::unknown(format!("No mapping for queue {}", message.queue_url)))?;
    let result = execute_wasm(engine, component, wit_message).await;
    println!("EXECUTOR: processed GRPC message for {}", message.id);
    result
}

#[tonic::async_trait]
impl pb::sqs_message_executor_server::SqsMessageExecutor for SqsExecutor {
    type StreamingExecuteStream = std::pin::Pin<Box<dyn futures::Stream<Item=Result<pb::SqsMessageResponse2, tonic::Status>> + Send>>;

    async fn execute(&self, req: tonic::Request<pb::SqsMessage>) -> Result<tonic::Response<pb::SqsMessageResponse>, tonic::Status> {
        let message = req.get_ref();
        println!("EXECUTOR: got GRPC message for {}", message.id);
        execute_wasm_for_grpc_message(&self.engine, &self.queue_components, message).await.map_err(|e|
            tonic::Status::unknown(e.to_string())  // TODO: elegant Ivan well done
        )?;
        println!("EXECUTOR: sending response GRPC message for {}", message.id);
        Ok(tonic::Response::new(pb::SqsMessageResponse{}))
    }

    async fn streaming_execute(&self, request: tonic::Request<tonic::Streaming<pb::SqsMessage>>) -> Result<tonic::Response<Self::StreamingExecuteStream>, tonic::Status> {
        let mut messages = request.into_inner();

        let (tx, rx) = tokio::sync::mpsc::channel(128);

        let engine = self.engine.clone();
        let queue_components = self.queue_components.clone();

        tokio::spawn(async move {
            loop {
                match messages.message().await {
                    Ok(Some(message)) => {
                        // OH NO
                        match execute_wasm_for_grpc_message(&engine, &queue_components, &message).await {
                            Ok(action) => {
                                tx.send(Ok(pb::SqsMessageResponse2 {
                                    queue_url: message.queue_url.clone(),
                                    receipt_handle: message.receipt_handle.clone(),
                                    action: grpc_action(action).into(),
                                })).await.unwrap();
                            },
                            Err(e) => {
                                // what, probably log it and... don't reply?
                            }
                        }
                    },
                    Ok(None) => {
                        // client disconnected, no more serving to this client
                        break;
                    },
                    Err(e) => {
                        // client sent us an error, what does that even mean
                        if let Err(_) = tx.send(Err(e)).await {
                            break;  // response was dropped. Apparently
                        }
                    },
                }
            }
        });

        let response_stm = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(tonic::Response::new(Box::pin(response_stm) as Self::StreamingExecuteStream))
    }
}

fn grpc_action(action: sqs::MessageAction) -> pb::sqs_message_response2::Action {
    match action {
        sqs::MessageAction::Leave => pb::sqs_message_response2::Action::Leave,
        sqs::MessageAction::Delete => pb::sqs_message_response2::Action::Delete,
    }
}
