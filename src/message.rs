use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct IncomingSqsMessage {
    pub component_id: String,
    pub queue_url: String,
    pub body: Option<String>,
}

#[derive(Deserialize, Serialize)]
pub enum SqsActionMessage {
    Delete(SqsDeleteActionMessage)
}

#[derive(Deserialize, Serialize)]
pub struct SqsDeleteActionMessage {
    queue_url: String,
    receipt_handle: String,
}
