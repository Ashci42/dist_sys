use anyhow::Context;
use dist_sys::{
    message::{EchoMessage, MessageType}, ClusterInformation, MessageContext, MessageId, MessageSender, Node, Runtime
};

fn main() -> anyhow::Result<()> {
    let mut echo_node = EchoNode::new();
    let mut runtime = Runtime::new(&mut echo_node);

    runtime.run().context("Node run failed")?;

    Ok(())
}

struct EchoNode {
    message_id: MessageId,
    cluster_information: Option<ClusterInformation>,
}

impl EchoNode {
    fn new() -> Self {
        Self {
            message_id: MessageId::new(),
            cluster_information: None,
        }
    }
}

impl Node for EchoNode {
    fn get_cluster_information(&self) -> Option<&ClusterInformation> {
        self.cluster_information.as_ref()
    }

    fn get_message_id(&mut self) -> &mut MessageId {
        &mut self.message_id
    }

    fn set_cluster_infromation(&mut self, cluster_infromation: ClusterInformation) {
        self.cluster_information = Some(cluster_infromation);
    }

    fn handle_echo(&mut self, message_context: MessageContext<EchoMessage>, message_sender: &MessageSender) {
        let next_message_id = self.get_message_id().get_next_id();
        let cluster_information = self
            .cluster_information
            .as_ref()
            .expect("Should have sent an error message if cluster information is none");
        let out_message = message_context.create_reply(
            cluster_information,
            next_message_id,
            MessageType::EchoOk(EchoMessage {
                echo: message_context.get_metadata().echo.clone(),
            }),
        );
        message_sender.send_message(&out_message).unwrap();
    }
}
