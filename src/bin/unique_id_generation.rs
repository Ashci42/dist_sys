use anyhow::Context;
use dist_sys::{
    message::{GenerateOkMessage, Message, MessageType},
    ClusterInformation, MessageContext, MessageId, Node, Runtime,
};

fn main() -> anyhow::Result<()> {
    let mut unique_id_generation_node = UniqueIdGenerationNode::new();
    let mut runtime = Runtime::new(&mut unique_id_generation_node);

    runtime.run().context("Node run failed")?;

    Ok(())
}

struct UniqueIdGenerationNode {
    message_id: MessageId,
    cluster_information: Option<ClusterInformation>,
}

impl UniqueIdGenerationNode {
    fn new() -> Self {
        Self {
            message_id: MessageId::new(),
            cluster_information: None,
        }
    }
}

impl Node for UniqueIdGenerationNode {
    fn get_cluster_information(&self) -> Option<&ClusterInformation> {
        self.cluster_information.as_ref()
    }

    fn get_message_id(&mut self) -> &mut MessageId {
        &mut self.message_id
    }

    fn set_cluster_infromation(&mut self, cluster_infromation: ClusterInformation) {
        self.cluster_information = Some(cluster_infromation)
    }

    fn handle_generate(&mut self, message_context: MessageContext<()>) -> Message {
        let next_message_id = self.get_message_id().get_next_id();
        let cluster_information = self
            .cluster_information
            .as_ref()
            .expect("Should have sent an error message if cluster information is none");
        message_context.create_reply(
            cluster_information,
            next_message_id,
            MessageType::GenerateOk(GenerateOkMessage {
                id: format!("{}-{}", cluster_information.get_node_id(), next_message_id),
            }),
        )
    }
}
