use std::collections::{HashMap, HashSet};

use dist_sys::{InitMessage, MessageSender, Node, NodeInformation, Runtime};
use serde::{Deserialize, Serialize};

fn main() {
    let multi_node_broadcast_node = MultiNodeBroadcastNode::new();
    let mut runtime = Runtime::new(multi_node_broadcast_node);
    runtime.run();
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type")]
enum InMultiNodeBroadcastMessage {
    #[serde(rename = "init")]
    Init(InitMessage),
    #[serde(rename = "broadcast")]
    Broadcast(BroadcastPayload),
    #[serde(rename = "read")]
    Read,
    #[serde(rename = "topology")]
    Topology(TopologyPayload),
    #[serde(rename = "gossip")]
    Gossip(GossipPayload),
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type")]
enum OutMultiNodeBroadcastMessage {
    #[serde(rename = "init_ok")]
    Init,
    #[serde(rename = "broadcast_ok")]
    Broadcast,
    #[serde(rename = "read_ok")]
    Read(ReadOkPayload),
    #[serde(rename = "topology_ok")]
    Topology,
    #[serde(rename = "gossip")]
    Gossip(GossipPayload),
}

struct MultiNodeBroadcastNode {
    messages: HashSet<i32>,
    neighbours: Option<Vec<String>>,
}

impl MultiNodeBroadcastNode {
    fn new() -> Self {
        Self {
            messages: HashSet::new(),
            neighbours: None,
        }
    }
}

impl Node<InMultiNodeBroadcastMessage, OutMultiNodeBroadcastMessage> for MultiNodeBroadcastNode {
    fn handle_message(
        &mut self,
        message: InMultiNodeBroadcastMessage,
        message_sender: &mut MessageSender<OutMultiNodeBroadcastMessage>,
    ) {
        match message {
            InMultiNodeBroadcastMessage::Init(payload) => {
                message_sender.register_node_information(NodeInformation::from(payload));
                message_sender.reply(OutMultiNodeBroadcastMessage::Init);
            }
            InMultiNodeBroadcastMessage::Broadcast(payload) => {
                self.messages.insert(payload.message);
                let neighbours = self.neighbours.as_ref().expect("Did not receive topology");
                for neighbour in neighbours {
                    message_sender.send_to(
                        neighbour.clone(),
                        OutMultiNodeBroadcastMessage::Gossip(GossipPayload {
                            messages: self.messages.clone(),
                            informed: HashSet::from_iter(neighbours.to_owned()),
                        }),
                    );
                }
                message_sender.reply(OutMultiNodeBroadcastMessage::Broadcast);
            }
            InMultiNodeBroadcastMessage::Read => {
                message_sender.reply(OutMultiNodeBroadcastMessage::Read(ReadOkPayload {
                    messages: self.messages.clone(),
                }));
            }
            InMultiNodeBroadcastMessage::Topology(mut payload) => {
                let node_id = message_sender
                    .node_id()
                    .expect("Did not get an id assigned");
                self.neighbours = payload.topology.remove(node_id);
                message_sender.reply(OutMultiNodeBroadcastMessage::Topology);
            }
            InMultiNodeBroadcastMessage::Gossip(payload) => {
                self.messages.extend(payload.messages);
                let neighbours = self.neighbours.as_ref().expect("Did not receive topology");
                let mut informed = payload.informed.clone();
                informed.extend(neighbours.to_owned());
                for neighbour in neighbours
                    .iter()
                    .filter(|neighbour| !payload.informed.contains(neighbour.as_str()))
                {
                    message_sender.send_to(
                        neighbour.clone(),
                        OutMultiNodeBroadcastMessage::Gossip(GossipPayload {
                            messages: self.messages.clone(),
                            informed: informed.clone(),
                        }),
                    );
                }
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
    messages: HashSet<i32>,
}

#[derive(Deserialize, Serialize, Debug)]
struct TopologyPayload {
    topology: HashMap<String, Vec<String>>,
}

#[derive(Deserialize, Serialize, Debug)]
struct GossipPayload {
    messages: HashSet<i32>,
    informed: HashSet<String>,
}
