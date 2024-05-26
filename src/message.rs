use serde::{Deserialize, Serialize};

/// The Message a node can receive or send
#[derive(Deserialize, Serialize, Debug)]
pub struct Message<M> {
    /// The node this message came from
    #[serde(rename = "src")]
    pub source: String,
    /// The node this message was sent to
    #[serde(rename = "dest")]
    pub destination: String,
    /// The payload of the message
    #[serde(rename = "body")]
    pub payload: Payload<M>,
}

/// The payload of a [Message]
#[derive(Deserialize, Serialize, Debug)]
pub struct Payload<M> {
    #[serde(flatten)]
    pub message_type: M,
    /// Unique message identifier
    ///
    /// Each id should be unique on the node which sent them.
    #[serde(rename = "msg_id")]
    pub message_id: Option<u32>,
    /// The message id of the [Message] it is replying to
    pub in_reply_to: Option<u32>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct InitMessage {
    /// The id of the node which is receiving this [Message]
    pub node_id: String,
    /// The ids of all nodes in the cluster
    pub node_ids: Vec<String>,
}
