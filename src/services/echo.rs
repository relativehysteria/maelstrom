use std::io::Write;
use serde::{Serialize, Deserialize};
use crate::message as msg;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
/// Payloads handled by the echo server
pub enum Payload {
    Echo   { echo: String },
    EchoOk { echo: String },
}

/// A node in the echo service cluster
pub struct EchoNode {
    _id: String,
}

impl msg::Node<Payload> for EchoNode {
    fn from_init(init: &msg::Init) -> anyhow::Result<Self> {
        Ok(Self {
            _id: init.node_id.clone(),
        })
    }

    fn step(&mut self, input: msg::Message<Payload>, output: &mut dyn Write)
            -> anyhow::Result<()> {
        // We will change the input into a reply later on, so mark it mutable
        let mut input = input;
        let id = input.body.id;

        match input.body.payload {
            Payload::Echo { echo } => {
                input.body.payload = Payload::EchoOk { echo };
                input.into_reply(id).send(output)
            },
            Payload::EchoOk { .. } => Ok(()),
        }
    }
}

pub fn main() -> anyhow::Result<()> {
    msg::main_loop::<Payload, EchoNode>()
}
