use core::fmt::Debug;
use serde::{Serialize, Deserialize};

#[async_trait::async_trait]
pub trait Sender<M> {
    async fn send(&self, message: M) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
pub trait Receiver<M> {
    // TODO: make it so this doesn't need to be mut?
    async fn receive(&mut self) -> anyhow::Result<M>;
}

pub fn in_memory_bus<'de, M: Serialize + Deserialize<'de> + Send + Sync + Debug + 'static>() -> (impl Sender<M> + Send + Sync + 'static, impl Receiver<M> + Send + Sync + 'static) {
    let (tx, rx) = tokio::sync::mpsc::channel(1000);
    (InMemorySender { tx }, InMemoryReceiver { rx })
}

struct InMemorySender<M> {
    tx: tokio::sync::mpsc::Sender<M>,
}

struct InMemoryReceiver<M> {
    rx: tokio::sync::mpsc::Receiver<M>,
}

#[async_trait::async_trait]
impl<M: Serialize + Send + Sync + Debug + 'static> Sender<M> for InMemorySender<M> {
    async fn send(&self, message: M) -> anyhow::Result<()> {
        self.tx.send(message).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl<'de, M: Deserialize<'de> + Send + Sync + Debug + 'static> Receiver<M> for InMemoryReceiver<M> {
    async fn receive(&mut self) -> anyhow::Result<M> {
        self.rx.recv().await.ok_or(anyhow::anyhow!("sender terminated"))
    }
}
