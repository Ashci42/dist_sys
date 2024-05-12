use anyhow::Context;
use dist_sys::{
    message::{BroadcastMessage, Message, MessageType, ReadOkMessage},
    ClusterInformation, MessageContext, MessageId, Node, Runtime,
};

fn main() -> anyhow::Result<()> {
    let mut single_node_broadcast_node = SingleNodeBroadcastNode::new();
    let mut runtime = Runtime::new(&mut single_node_broadcast_node);
    runtime.run().context("Node run failed")?;

    Ok(())
}

struct SingleNodeBroadcastNode {
    message_id: MessageId,
    cluster_information: Option<ClusterInformation>,
    messages: Vec<i32>,
}

impl SingleNodeBroadcastNode {
    fn new() -> Self {
        Self {
            message_id: MessageId::new(),
            cluster_information: None,
            messages: vec![],
        }
    }
}

impl Node for SingleNodeBroadcastNode {
    fn get_cluster_information(&self) -> Option<&ClusterInformation> {
        self.cluster_information.as_ref()
    }

    fn get_message_id(&mut self) -> &mut MessageId {
        &mut self.message_id
    }

    fn set_cluster_infromation(&mut self, cluster_infromation: ClusterInformation) {
        self.cluster_information = Some(cluster_infromation);
    }

    fn handle_broadcast(&mut self, message_context: MessageContext<BroadcastMessage>) -> Message {
        self.messages.push(message_context.get_metadata().message);
        let next_message_id = self.get_message_id().get_next_id();
        let cluster_information = self
            .cluster_information
            .as_ref()
            .expect("Should have sent an error message if cluster information is none");

        message_context.create_reply(
            cluster_information,
            next_message_id,
            MessageType::BroadcastOk,
        )
    }

    fn handle_read(&mut self, message_context: MessageContext<()>) -> Message {
        let next_message_id = self.get_message_id().get_next_id();
        let cluster_information = self
            .cluster_information
            .as_ref()
            .expect("Should have sent an error message if cluster information is none");

        message_context.create_reply(
            cluster_information,
            next_message_id,
            MessageType::ReadOk(ReadOkMessage {
                messages: self.messages.clone(),
            }),
        )
    }

    fn handle_topology(
        &mut self,
        message_context: MessageContext<dist_sys::message::TopologyMessage>,
    ) -> Message {
        let next_message_id = self.get_message_id().get_next_id();
        let cluster_information = self
            .cluster_information
            .as_ref()
            .expect("Should have sent an error message if cluster information is none");

        message_context.create_reply(
            cluster_information,
            next_message_id,
            MessageType::TopologyOk,
        )
    }
}
