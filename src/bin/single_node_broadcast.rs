use std::collections::HashMap;

use dist_sys::{InitMessage, MessageSender, Node, NodeInformation, Runtime};
use serde::{Deserialize, Serialize};

fn main() {
    let single_node_broadcast_node = SingleNodeBroadcastNode::new();
    let mut runtime = Runtime::new(single_node_broadcast_node);
    runtime.run();
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type")]
enum InSingleNodeBroadcastMessage {
    #[serde(rename = "init")]
    Init(InitMessage),
    #[serde(rename = "broadcast")]
    Broadcast(BroadcastPayload),
    #[serde(rename = "read")]
    Read,
    #[serde(rename = "topology")]
    Topology(TopologyPayload),
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type")]
enum OutSingleNodeBroadcastMessage {
    #[serde(rename = "init_ok")]
    Init,
    #[serde(rename = "broadcast_ok")]
    Broadcast,
    #[serde(rename = "read_ok")]
    Read(ReadOkPayload),
    #[serde(rename = "topology_ok")]
    Topology,
}

struct SingleNodeBroadcastNode {
    messages: Vec<i32>,
}

impl SingleNodeBroadcastNode {
    fn new() -> Self {
        Self { messages: vec![] }
    }
}

impl Node<InSingleNodeBroadcastMessage, OutSingleNodeBroadcastMessage> for SingleNodeBroadcastNode {
    fn handle_message(
        &mut self,
        message: InSingleNodeBroadcastMessage,
        message_sender: &mut MessageSender<OutSingleNodeBroadcastMessage>,
    ) {
        match message {
            InSingleNodeBroadcastMessage::Init(payload) => {
                message_sender.register_node_information(NodeInformation::from(payload));
                message_sender.reply(OutSingleNodeBroadcastMessage::Init);
            }
            InSingleNodeBroadcastMessage::Broadcast(payload) => {
                self.messages.push(payload.message);
                message_sender.reply(OutSingleNodeBroadcastMessage::Broadcast);
            }
            InSingleNodeBroadcastMessage::Read => {
                message_sender.reply(OutSingleNodeBroadcastMessage::Read(ReadOkPayload {
                    messages: self.messages.clone(),
                }));
            }
            InSingleNodeBroadcastMessage::Topology(_payload) => {
                message_sender.reply(OutSingleNodeBroadcastMessage::Topology);
            }
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
struct BroadcastPayload {
    message: i32,
}

#[derive(Deserialize, Serialize, Debug)]
struct ReadOkPayload {
    messages: Vec<i32>,
}

#[derive(Deserialize, Serialize, Debug)]
struct TopologyPayload {
    topology: HashMap<String, Vec<String>>,
}
