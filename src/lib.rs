pub mod error;

mod message;

pub use message::InitMessage;

use std::{io, marker::PhantomData};

use message::{Message, Payload};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

pub trait Node<InMessage, OutMessage>
where
    for<'de> InMessage: Deserialize<'de>,
    OutMessage: Serialize,
{
    fn handle_message(
        &mut self,
        message: InMessage,
        message_sender: &mut MessageSender<OutMessage>,
    );
}

pub struct Runtime<N, InMessage, OutMessage>
where
    N: Node<InMessage, OutMessage>,
    for<'de> InMessage: Deserialize<'de>,
    OutMessage: Serialize,
{
    node: N,
    in_message: PhantomData<InMessage>,
    out_message: PhantomData<OutMessage>,
}

impl<N, InMessage, OutMessage> Runtime<N, InMessage, OutMessage>
where
    N: Node<InMessage, OutMessage>,
    for<'de> InMessage: Deserialize<'de>,
    OutMessage: Serialize,
{
    pub fn new(node: N) -> Self {
        Self {
            node,
            in_message: PhantomData,
            out_message: PhantomData,
        }
    }

    pub fn run(&mut self) {
        let stdin = io::stdin().lock();
        let message_deserialiser =
            Deserializer::from_reader(stdin).into_iter::<Message<InMessage>>();
        let mut message_sender = MessageSender {
            out_message: PhantomData,
            node_information: None,
            sender_information: None,
            message_id: 0,
        };
        for message in message_deserialiser {
            let message = message.expect("Failed to serialize message from stdout");
            message_sender.register_sender_information(SenderInformation {
                source: message.source.clone(),
                message_id: message.payload.message_id,
            });

            self.node
                .handle_message(message.payload.message_type, &mut message_sender);
        }
    }
}

pub struct MessageSender<OutMessage>
where
    OutMessage: Serialize,
{
    out_message: PhantomData<OutMessage>,
    node_information: Option<NodeInformation>,
    sender_information: Option<SenderInformation>,
    message_id: u32,
}

impl<OutMessage> MessageSender<OutMessage>
where
    OutMessage: Serialize,
{
    pub fn reply(&mut self, payload: OutMessage) {
        if self.sender_information.is_none() {
            panic!("Cannot send message without sender information");
        }

        self.check_node_information();
        let sender_information = self.sender_information.take().unwrap();
        self.send(
            sender_information.source,
            payload,
            sender_information.message_id,
        );
    }

    pub fn register_node_information(&mut self, node_information: NodeInformation) {
        self.node_information = Some(node_information);
    }

    pub fn node_id(&self) -> Option<&str> {
        self.node_information
            .as_ref()
            .map(|node_information| node_information.id.as_str())
    }

    pub fn send_to(&mut self, destination: String, payload: OutMessage) {
        self.check_node_information();
        self.send(destination, payload, None);
    }

    fn get_next_message_id(&mut self) -> u32 {
        self.message_id += 1;

        self.message_id
    }

    fn register_sender_information(&mut self, sender_information: SenderInformation) {
        self.sender_information = Some(sender_information);
    }

    fn check_node_information(&self) {
        if self.node_information.is_none() {
            panic!("Cannot send message without node information");
        }
    }

    fn send(&mut self, destination: String, payload: OutMessage, in_reply_to: Option<u32>) {
        let out_message = Message {
            source: self.node_information.as_ref().unwrap().id.clone(),
            destination,
            payload: Payload {
                message_type: payload,
                message_id: Some(self.get_next_message_id()),
                in_reply_to,
            },
        };
        let out_message = serde_json::to_string(&out_message).expect("Failed to serialise message");
        println!("{out_message}");
    }
}

pub struct NodeInformation {
    id: String,
}

impl From<InitMessage> for NodeInformation {
    fn from(value: InitMessage) -> Self {
        Self { id: value.node_id }
    }
}

struct SenderInformation {
    pub source: String,
    pub message_id: Option<u32>,
}
