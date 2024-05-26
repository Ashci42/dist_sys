use dist_sys::{InitMessage, MessageSender, Node, NodeInformation, Runtime};
use serde::{Deserialize, Serialize};

fn main() {
    let echo_node = EchoNode;
    let mut runtime = Runtime::new(echo_node);
    runtime.run();
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type")]
enum InEchoMessage {
    #[serde(rename = "init")]
    Init(InitMessage),
    #[serde(rename = "echo")]
    Echo(EchoPayload),
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type")]
enum OutEchoMessage {
    #[serde(rename = "init_ok")]
    Init,
    #[serde(rename = "echo_ok")]
    Echo(EchoPayload),
}

struct EchoNode;

impl Node<InEchoMessage, OutEchoMessage> for EchoNode {
    fn handle_message(
        &mut self,
        message: InEchoMessage,
        message_sender: &mut MessageSender<OutEchoMessage>,
    ) {
        match message {
            InEchoMessage::Init(payload) => {
                message_sender.register_node_information(NodeInformation::from(payload));
                message_sender.reply(OutEchoMessage::Init);
            }
            InEchoMessage::Echo(payload) => {
                message_sender.reply(OutEchoMessage::Echo(EchoPayload { echo: payload.echo }));
            }
        }
    }
}

#[derive(Deserialize, Serialize)]
struct EchoPayload {
    echo: String,
}
