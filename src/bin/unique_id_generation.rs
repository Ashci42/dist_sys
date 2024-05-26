use dist_sys::{InitMessage, MessageSender, Node, NodeInformation, Runtime};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

fn main() {
    let unique_id_generation_node = UniqueIdGenerationNode;
    let mut runtime = Runtime::new(unique_id_generation_node);
    runtime.run();
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type")]
enum InUniqueIdGenerationMessage {
    #[serde(rename = "init")]
    Init(InitMessage),
    #[serde(rename = "generate")]
    Generage,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type")]
enum OutUniqueIdGenerationMessage {
    #[serde(rename = "init_ok")]
    Init,
    #[serde(rename = "generate_ok")]
    Generate(GenerateOkPayload),
}

struct UniqueIdGenerationNode;

impl Node<InUniqueIdGenerationMessage, OutUniqueIdGenerationMessage> for UniqueIdGenerationNode {
    fn handle_message(
        &mut self,
        message: InUniqueIdGenerationMessage,
        message_sender: &mut MessageSender<OutUniqueIdGenerationMessage>,
    ) {
        match message {
            InUniqueIdGenerationMessage::Init(payload) => {
                message_sender.register_node_information(NodeInformation::from(payload));
                message_sender.reply(OutUniqueIdGenerationMessage::Init);
            }
            InUniqueIdGenerationMessage::Generage => {
                let id = Uuid::new_v4().to_string();
                message_sender.reply(OutUniqueIdGenerationMessage::Generate(GenerateOkPayload {
                    id,
                }));
            }
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
struct GenerateOkPayload {
    id: String,
}
