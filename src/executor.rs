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

        let addr = "[::1]:50051".parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(pb::sqs_message_executor_server::SqsMessageExecutorServer::new(self))
            .serve(addr)
            .await?;
        // let server = pb::sqs_message_executor_server::SqsMessageExecutorServer::connect("http://[::1]:50051").await?;
        // let engine = Arc::new(self.engine);

        // let loops = self.queue_components.iter().map(|(queue_url, component)| {
        //     Self::start_receive_loop(engine.clone(), &client, queue_url, component)
        // });

        // let (r, _, rest) = futures::future::select_all(loops).await;
        // drop(rest);

        // r?

        Ok(())
    }
}

impl SqsExecutor {
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
    //         tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    //     }
    // }

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
        let wit_message = sqs::Message {
            id: Some(&message.id),
            message_attributes: &vec![],
            body: Some(&message.body),
        };
        self.execute_wasm(&message.component, wit_message).await.map_err(|e|
            tonic::Status::unknown(e.to_string())  // TODO: elegant Ivan well done
        )?;
        Ok(tonic::Response::new(pb::SqsMessageResponse{}))
    }
}