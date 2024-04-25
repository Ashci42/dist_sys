use std::io;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::{Deserializer, Value};

fn main() -> anyhow::Result<()> {
    let mut runtime = Runtime {
        echo_node: EchoNode::new(),
    };

    runtime.run().context("Node run failed")?;

    Ok(())
}

const TIMEOUT: u16 = 0;
const NODE_NOT_FOUND: u16 = 1;
const NOT_SUPPORTED: u16 = 10;
const TEMPORARILY_UNAVAILABLE: u16 = 11;
const MALFORMED_REQUEST: u16 = 12;
const CRASH: u16 = 13;
const ABORT: u16 = 14;
const KEY_DOES_NOT_EXIST: u16 = 20;
const KEY_ALREADY_EXISTS: u16 = 21;
const PRECONDITION_FAILED: u16 = 22;
const TXN_CONFLICT: u16 = 30;
const WRONG_NODE: u16 = 1001;
const UNINITIALISED: u16 = 1002;

/// The Message a node can receive or send
#[derive(Deserialize, Serialize, Debug)]
struct Message {
    /// The node this message came from
    #[serde(rename = "src")]
    source: String,
    /// The node this message was sent to
    #[serde(rename = "dest")]
    destination: String,
    /// The payload of the message
    #[serde(rename = "body")]
    payload: Payload,
}

/// The payload of a [Message]
#[derive(Deserialize, Serialize, Debug)]
struct Payload {
    #[serde(flatten)]
    message_type: MessageType,
    /// Unique message identifier
    ///
    /// Each id should be unique on the node which sent them.
    #[serde(rename = "msg_id")]
    message_id: Option<u32>,
    /// The message id of the [Message] it is replying to
    in_reply_to: Option<u32>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type")]
enum MessageType {
    #[serde(rename = "init")]
    Init {
        /// The id of the node which is receiving this [Message]
        node_id: String,
        /// The ids of all nodes in the cluster
        node_ids: Vec<String>,
    },
    #[serde(rename = "init_ok")]
    InitOk,
    #[serde(rename = "error")]
    Error {
        /// The error type
        code: u16,
        /// The error message
        text: Option<String>,
        #[serde(flatten)]
        other: Option<Value>,
    },
    #[serde(rename = "echo")]
    Echo { echo: String },
    #[serde(rename = "echo_ok")]
    EchoOk { echo: String },
}

struct EchoNode {
    message_id: u32,
    cluster_node_ids: Option<Vec<String>>,
    id: Option<String>,
}

impl EchoNode {
    fn new() -> Self {
        Self {
            message_id: 0,
            cluster_node_ids: None,
            id: None,
        }
    }

    fn handle(&mut self, message: Message) -> Message {
        if let Some(id) = &self.id {
            if *id != message.destination {
                let response = Message {
                    source: String::from(""),
                    destination: message.source,
                    payload: Payload {
                        message_type: MessageType::Error {
                            code: WRONG_NODE,
                            text: Some(format!(
                                "Destination was {}, but the recepient node was {:?}",
                                message.destination, self.id
                            )),
                            other: None,
                        },
                        message_id: Some(self.message_id),
                        in_reply_to: message.payload.message_id,
                    },
                };
                self.message_id += 1;

                return response;
            }
        }

        match message.payload.message_type {
            MessageType::Init { node_id, node_ids } => {
                self.id = Some(node_id.clone());
                self.cluster_node_ids = Some(node_ids);
                let response = Message {
                    source: node_id,
                    destination: message.source,
                    payload: Payload {
                        message_type: MessageType::InitOk,
                        message_id: Some(self.message_id),
                        in_reply_to: message.payload.message_id,
                    },
                };
                self.message_id += 1;

                return response;
            }
            MessageType::InitOk => unreachable!(),
            MessageType::Error { .. } => unimplemented!(),
            MessageType::Echo { echo } => {
                if self.id.is_none() {
                    let response = Message {
                        source: String::from(""),
                        destination: message.source,
                        payload: Payload {
                            message_type: MessageType::Error {
                                code: UNINITIALISED,
                                text: Some(String::from(
                                    "This node has not received an Init message yet",
                                )),
                                other: None,
                            },
                            message_id: Some(self.message_id),
                            in_reply_to: message.payload.message_id,
                        },
                    };
                    self.message_id += 1;

                    return response;
                }

                let response = Message {
                    source: self
                        .id
                        .as_ref()
                        .expect("Should have sent an error message if id is none")
                        .to_owned(),
                    destination: message.source,
                    payload: Payload {
                        message_type: MessageType::EchoOk { echo },
                        message_id: Some(self.message_id),
                        in_reply_to: message.payload.message_id,
                    },
                };
                self.message_id += 1;

                return response;
            }
            MessageType::EchoOk { .. } => unreachable!(),
        }
    }
}

struct Runtime {
    echo_node: EchoNode,
}

impl Runtime {
    fn run(&mut self) -> anyhow::Result<()> {
        let stdin = io::stdin().lock();
        let message_deserialiser = Deserializer::from_reader(stdin).into_iter::<Message>();
        for message in message_deserialiser {
            let message = message.context("Could not deserialise message from STDIN")?;
            let out_message = self.echo_node.handle(message);
            let out_message = serde_json::to_string(&out_message)
                .context("Could not serialise response message")?;
            println!("{out_message}");
        }

        Ok(())
    }
}
