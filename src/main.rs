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
        });

        let bindir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/release");
        let args: Vec<_> = std::env::args().skip(1).collect();

        let mut executor = tokio::process::Command::new(bindir.join("trigger-sqs-executor")).args(&args).spawn()?;
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // give it a bit of time to start serving
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
    }
}
