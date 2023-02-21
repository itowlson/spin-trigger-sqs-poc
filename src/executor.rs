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

// This is giving a weird dead code warning that it's never used BUT IT TOTALLY IS
// I HAVE PROVED IT
#[tokio::main]
async fn main() -> Result<(), Error> {
    let t = Command::parse();
    t.run().await
}

pub struct SqsExecutor {
    engine: TriggerAppEngine<Self>,
    // queue_components: HashMap<String, String>,  // Queue URL -> component ID
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
        // let queue_components = engine
        //     .trigger_configs()
        //     .map(|(_, config)| (config.queue_url.clone(), config.component.clone()))
        //     .collect();

        Ok(Self {
            engine,
            // queue_components,
        })
    }

    async fn run(self, _config: Self::RunConfig) -> Result<()> {
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            std::process::exit(0);
        });

        println!("EXECUTOR: running");

        let addr = "[::1]:50051".parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(pb::sqs_message_executor_server::SqsMessageExecutorServer::new(self))
            .serve(addr)
            .await?;

        Ok(())
    }
}

impl SqsExecutor {
    async fn execute_wasm(&self, component_id: &str, message: sqs::Message<'_>) -> Result<sqs::MessageAction> {
        println!("Executing component {component_id}");
        let (instance, mut store) = self.engine.prepare_instance(component_id).await?;
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

#[tonic::async_trait]
impl pb::sqs_message_executor_server::SqsMessageExecutor for SqsExecutor {
    async fn execute(&self, req: tonic::Request<pb::SqsMessage>) -> Result<tonic::Response<pb::SqsMessageResponse>, tonic::Status> {
        let message = req.get_ref();
        println!("EXECUTOR: got GRPC message for {}", message.id);
        let wit_message = sqs::Message {
            id: Some(&message.id),
            message_attributes: &vec![],
            body: Some(&message.body),
        };
        self.execute_wasm(&message.component, wit_message).await.map_err(|e|
            tonic::Status::unknown(e.to_string())  // TODO: elegant Ivan well done
        )?;
        println!("EXECUTOR: processed GRPC message for {}", message.id);
        Ok(tonic::Response::new(pb::SqsMessageResponse{}))
    }
}