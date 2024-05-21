use std::io;

use anyhow::Context;
use message::{
    BroadcastMessage, EchoMessage, ErrorMessage, GenerateOkMessage, InitMessage, Message,
    MessageType, Payload, ReadOkMessage, TopologyMessage,
};
use serde_json::Deserializer;

pub trait Node {
    fn get_cluster_information(&self) -> Option<&ClusterInformation>;
    fn get_message_id(&mut self) -> &mut MessageId;
    fn set_cluster_infromation(&mut self, cluster_infromation: ClusterInformation);

    fn handle_message(&mut self, message: Message,  message_sender: &MessageSender) -> anyhow::Result<()> {
        if let Some(cluster_infromation) = self.get_cluster_information() {
            if cluster_infromation.get_node_id() != message.destination {
                let out_message = Message {
                    source: String::from(""),
                    destination: message.source,
                    payload: Payload {
                        message_type: MessageType::Error(ErrorMessage {
                            code: error::WRONG_NODE,
                            text: Some(format!(
                                "Destination was {}, but the recepient node was {:?}",
                                message.destination,
                                cluster_infromation.get_node_id()
                            )),
                            other: None,
                        }),
                        message_id: Some(self.get_message_id().get_next_id()),
                        in_reply_to: message.payload.message_id,
                    },
                };
                message_sender.send_message(&out_message)?;

                return Ok(());
            }
        }

        match message.payload.message_type {
            MessageType::Init(init_message) => {
                self.handle_init(MessageContext::new(
                    message.payload.message_id,
                    message.source,
                    init_message,
                ), message_sender);
            },
            MessageType::InitOk => {
                self.handle_init_ok(MessageContext::new(
                message.payload.message_id,
                message.source,
                (),
            ), message_sender);},
            MessageType::Error(error_message) => {
                if let Some(m) = self.check_initialised(&message.source, message.payload.message_id)
                {
                    message_sender.send_message(&m)?;
                    return Ok(());
                }

                self.handle_error(MessageContext::new(
                    message.payload.message_id,
                    message.source,
                    error_message,
                ), message_sender);
            }
            MessageType::Echo(echo_message) => {
                if let Some(m) = self.check_initialised(&message.source, message.payload.message_id)
                {
                    message_sender.send_message(&m)?;
                    return Ok(());
                }

                self.handle_echo(MessageContext::new(
                    message.payload.message_id,
                    message.source,
                    echo_message,
                ), message_sender);
            }
            MessageType::EchoOk(echo_ok_message) => {
                self.handle_echo_ok(MessageContext::new(
                message.payload.message_id,
                message.source,
                echo_ok_message,
            ), message_sender);},
            MessageType::Generage => {
                if let Some(m) = self.check_initialised(&message.source, message.payload.message_id)
                {
                    message_sender.send_message(&m)?;
                    return Ok(());
                }

                self.handle_generate(MessageContext::new(
                    message.payload.message_id,
                    message.source,
                    (),
                ), message_sender);
            }
            MessageType::GenerateOk(generate_ok_message) => {
                self.handle_generate_ok(MessageContext::new(
                    message.payload.message_id,
                    message.source,
                    generate_ok_message,
                ), message_sender);
            }
            MessageType::Broadcast(broadcast_message) => {
                if let Some(m) = self.check_initialised(&message.source, message.payload.message_id)
                {
                    message_sender.send_message(&m)?;
                    return Ok(());
                }

                self.handle_broadcast(MessageContext::new(
                    message.payload.message_id,
                    message.source,
                    broadcast_message,
                ), message_sender);
            }
            MessageType::BroadcastOk => {
                self.handle_broadcast_ok(MessageContext::new(
                message.payload.message_id,
                message.source,
                (),
            ), message_sender);},
            MessageType::Read => {
                if let Some(m) = self.check_initialised(&message.source, message.payload.message_id)
                {
                    message_sender.send_message(&m)?;
                    return Ok(());
                }

                self.handle_read(MessageContext::new(
                    message.payload.message_id,
                    message.source,
                    (),
                ), message_sender);
            }
            MessageType::ReadOk(read_ok_message) => {
                self.handle_read_ok(MessageContext::new(
                message.payload.message_id,
                message.source,
                read_ok_message,
            ), message_sender);},
            MessageType::Topology(topology_message) => {
                if let Some(m) = self.check_initialised(&message.source, message.payload.message_id)
                {
                    message_sender.send_message(&m)?;
                    return Ok(());
                }

                self.handle_topology(MessageContext::new(
                    message.payload.message_id,
                    message.source,
                    topology_message,
                ), message_sender);
            }
            MessageType::TopologyOk => {
                self.handle_topology_ok(MessageContext::new(
                message.payload.message_id,
                message.source,
                (),
            ), message_sender);},
        }

        Ok(())
    }

    fn handle_init(&mut self, message_context: MessageContext<InitMessage>, message_sender: &MessageSender) {
        self.set_cluster_infromation(ClusterInformation::new(
            message_context.metadata.node_id.clone(),
            message_context.metadata.node_ids,
        ));
        let out_message = Message {
            source: message_context.metadata.node_id,
            destination: message_context.source,
            payload: Payload {
                message_type: MessageType::InitOk,
                message_id: Some(self.get_message_id().get_next_id()),
                in_reply_to: message_context.id,
            },
        };
        message_sender.send_message(&out_message).expect("Should send message");
    }

    fn handle_init_ok(&mut self, _message_context: MessageContext<()>, _message_sender: &MessageSender) {
        unreachable!();
    }

    fn handle_error(&mut self, _message_context: MessageContext<ErrorMessage>, _message_sender: &MessageSender) {
        unimplemented!();
    }

    fn handle_echo(&mut self, _message_context: MessageContext<EchoMessage>, _message_sender: &MessageSender) {
        unimplemented!();
    }

    fn handle_echo_ok(&mut self, _message_context: MessageContext<EchoMessage>, _message_sender: &MessageSender) {
        unreachable!();
    }

    fn handle_generate(&mut self, _message_context: MessageContext<()>, _message_sender: &MessageSender) {
        unimplemented!();
    }

    fn handle_generate_ok(
        &mut self,
        _message_context: MessageContext<GenerateOkMessage>,
        _message_sender: &MessageSender
    ) -> Message {
        unreachable!();
    }

    fn handle_broadcast(&mut self, _message_context: MessageContext<BroadcastMessage>, _message_sender: &MessageSender) {
        unimplemented!();
    }

    fn handle_broadcast_ok(&mut self, _message_context: MessageContext<()>, _message_sender: &MessageSender) {
        unreachable!();
    }

    fn handle_read(&mut self, _message_context: MessageContext<()>, _message_sender: &MessageSender) {
        unimplemented!();
    }

    fn handle_read_ok(&mut self, _message_context: MessageContext<ReadOkMessage>, _message_sender: &MessageSender) {
        unreachable!();
    }

    fn handle_topology(&mut self, _message_context: MessageContext<TopologyMessage>, _message_sender: &MessageSender) {
        unimplemented!();
    }

    fn handle_topology_ok(&mut self, _message_context: MessageContext<()>, _message_sender: &MessageSender) {
        unreachable!();
    }

    fn check_initialised(&mut self, source: &str, id: Option<u32>) -> Option<Message> {
        if self.get_cluster_information().is_none() {
            Some(Message {
                source: String::from(""),
                destination: source.to_owned(),
                payload: Payload {
                    message_type: MessageType::Error(ErrorMessage {
                        code: error::UNINITIALISED,
                        text: Some(String::from(
                            "This node has not received an Init message yet",
                        )),
                        other: None,
                    }),
                    message_id: Some(self.get_message_id().get_next_id()),
                    in_reply_to: id,
                },
            })
        } else {
            None
        }
    }
}

pub trait MessageMetadata {}

impl MessageMetadata for () {}
impl MessageMetadata for InitMessage {}
impl MessageMetadata for ErrorMessage {}
impl MessageMetadata for EchoMessage {}
impl MessageMetadata for GenerateOkMessage {}
impl MessageMetadata for BroadcastMessage {}
impl MessageMetadata for ReadOkMessage {}
impl MessageMetadata for TopologyMessage {}

pub struct Runtime<'node> {
    node: &'node mut dyn Node,
}

impl<'node> Runtime<'node> {
    pub fn new(node: &'node mut dyn Node) -> Self {
        Self { node }
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        let stdin = io::stdin().lock();
        let message_deserialiser = Deserializer::from_reader(stdin).into_iter::<Message>();
        let message_sender = MessageSender;
        for message in message_deserialiser {
            let message = message.context("Could not deserialise message from STDIN")?;
            self.node.handle_message(message, &message_sender)?;
        }

        Ok(())
    }
}

pub struct MessageId(u32);

impl MessageId {
    pub fn new() -> Self {
        Self(0)
    }

    pub fn get_next_id(&mut self) -> u32 {
        self.0 += 1;

        self.0
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ClusterInformation {
    node_id: String,
    node_ids: Vec<String>,
}

impl ClusterInformation {
    pub fn new(node_id: String, node_ids: Vec<String>) -> Self {
        Self { node_id, node_ids }
    }

    pub fn get_node_id(&self) -> &str {
        &self.node_id
    }
}

pub struct MessageContext<M>
where
    M: MessageMetadata,
{
    id: Option<u32>,
    source: String,
    metadata: M,
}

impl<M> MessageContext<M>
where
    M: MessageMetadata,
{
    fn new(id: Option<u32>, source: String, metadata: M) -> Self {
        Self {
            id,
            source,
            metadata,
        }
    }

    pub fn get_source(&self) -> &str {
        &self.source
    }

    pub fn get_id(&self) -> &Option<u32> {
        &self.id
    }

    pub fn get_metadata(&self) -> &M {
        &self.metadata
    }

    pub fn create_reply(
        &self,
        cluster_information: &ClusterInformation,
        message_id: u32,
        message_type: MessageType,
    ) -> Message {
        Message {
            source: cluster_information.get_node_id().to_owned(),
            destination: self.get_source().to_string(),
            payload: Payload {
                message_type,
                message_id: Some(message_id),
                in_reply_to: self.get_id().map(|id| id),
            },
        }
    }
}

pub struct MessageSender;

impl MessageSender {
    pub fn send_message(&self, message: &Message) -> anyhow::Result<()> {
        let out_message = serde_json::to_string(message)
            .context("Could not serialise response message")?;
        println!("{out_message}");

        Ok(())
    }
}

pub mod message {
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    /// The Message a node can receive or send
    #[derive(Deserialize, Serialize, Debug)]
    pub struct Message {
        /// The node this message came from
        #[serde(rename = "src")]
        pub source: String,
        /// The node this message was sent to
        #[serde(rename = "dest")]
        pub destination: String,
        /// The payload of the message
        #[serde(rename = "body")]
        pub payload: Payload,
    }

    /// The payload of a [Message]
    #[derive(Deserialize, Serialize, Debug)]
    pub struct Payload {
        #[serde(flatten)]
        pub message_type: MessageType,
        /// Unique message identifier
        ///
        /// Each id should be unique on the node which sent them.
        #[serde(rename = "msg_id")]
        pub message_id: Option<u32>,
        /// The message id of the [Message] it is replying to
        pub in_reply_to: Option<u32>,
    }

    #[derive(Deserialize, Serialize, Debug)]
    #[serde(tag = "type")]
    pub enum MessageType {
        #[serde(rename = "init")]
        Init(InitMessage),
        #[serde(rename = "init_ok")]
        InitOk,
        #[serde(rename = "error")]
        Error(ErrorMessage),
        #[serde(rename = "echo")]
        Echo(EchoMessage),
        #[serde(rename = "echo_ok")]
        EchoOk(EchoMessage),
        #[serde(rename = "generate")]
        Generage,
        #[serde(rename = "generate_ok")]
        GenerateOk(GenerateOkMessage),
        #[serde(rename = "broadcast")]
        Broadcast(BroadcastMessage),
        #[serde(rename = "broadcast_ok")]
        BroadcastOk,
        #[serde(rename = "read")]
        Read,
        #[serde(rename = "read_ok")]
        ReadOk(ReadOkMessage),
        #[serde(rename = "topology")]
        Topology(TopologyMessage),
        #[serde(rename = "topology_ok")]
        TopologyOk,
    }

    #[derive(Deserialize, Serialize, Debug)]
    pub struct InitMessage {
        /// The id of the node which is receiving this [Message]
        pub node_id: String,
        /// The ids of all nodes in the cluster
        pub node_ids: Vec<String>,
    }

    #[derive(Deserialize, Serialize, Debug)]
    pub struct ErrorMessage {
        /// The error type
        pub code: u16,
        /// The error message
        pub text: Option<String>,
        #[serde(flatten)]
        pub other: Option<Value>,
    }

    #[derive(Deserialize, Serialize, Debug)]
    pub struct EchoMessage {
        pub echo: String,
    }

    #[derive(Deserialize, Serialize, Debug)]
    pub struct GenerateOkMessage {
        pub id: String,
    }

    #[derive(Deserialize, Serialize, Debug)]
    pub struct BroadcastMessage {
        pub message: i32,
    }

    #[derive(Deserialize, Serialize, Debug)]
    pub struct ReadOkMessage {
        pub messages: Vec<i32>,
    }

    #[derive(Deserialize, Serialize, Debug)]
    pub struct TopologyMessage {
        pub topology: HashMap<String, Vec<String>>,
    }
}

pub mod error {
    pub const TIMEOUT: u16 = 0;
    pub const NODE_NOT_FOUND: u16 = 1;
    pub const NOT_SUPPORTED: u16 = 10;
    pub const TEMPORARILY_UNAVAILABLE: u16 = 11;
    pub const MALFORMED_REQUEST: u16 = 12;
    pub const CRASH: u16 = 13;
    pub const ABORT: u16 = 14;
    pub const KEY_DOES_NOT_EXIST: u16 = 20;
    pub const KEY_ALREADY_EXISTS: u16 = 21;
    pub const PRECONDITION_FAILED: u16 = 22;
    pub const TXN_CONFLICT: u16 = 30;
    pub const WRONG_NODE: u16 = 1001;
    pub const UNINITIALISED: u16 = 1002;
}
