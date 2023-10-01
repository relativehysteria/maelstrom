use std::collections::HashMap;
use std::io::Write;
use serde::{Serialize, Deserialize};
use crate::message as msg;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
/// Payloads handled by the broadcast server
pub enum Payload {
    Topology { topology: Option<HashMap<String, Vec<String>>> },
    TopologyOk,

    Broadcast { message: usize },
    BroadcastOk,

    Read,
    ReadOk { messages: Vec<usize> },
}

/// A node in the broadcast service cluster
pub struct BroadcastNode {
    _id:        String,
    _neighbors: Option<Vec<String>>,
    msgs:       Vec<usize>,
}

impl msg::Node<Payload> for BroadcastNode {
    fn from_init(init: &msg::Init) -> anyhow::Result<Self> {
        Ok(Self {
            _id:        init.node_id.clone(),
            _neighbors: None,
            msgs:       Vec::with_capacity(1024),
        })
    }

    fn step(&mut self, input: msg::Message<Payload>, output: &mut dyn Write)
            -> anyhow::Result<()> {
        // We will change the input into a reply later on, so mark it mutable
        let mut input = input;
        let id = input.body.id;

        match input.body.payload {
            // Ignore *Ok messages
            Payload::TopologyOk | Payload::BroadcastOk |
                Payload::ReadOk { .. } => Ok(()),

            // Ignore topology
            Payload::Topology { .. } => {
                input.body.payload = Payload::TopologyOk;
                input.into_reply(id).send(output)
            },

            // Save the message that was broadcasted
            Payload::Broadcast { message } => {
                self.msgs.push(message);
                input.body.payload = Payload::BroadcastOk;
                input.into_reply(id).send(output)
            },

            // Save the messages that were received
            Payload::Read => {
                // XXX: Surely there is a better way than clone?
                input.body.payload = Payload::ReadOk {
                    messages: self.msgs.clone()
                };
                input.into_reply(id).send(output)
            }
        }
    }
}

pub fn main() -> anyhow::Result<()> {
    msg::main_loop::<Payload, BroadcastNode>()
}
