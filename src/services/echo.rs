//! good old' echo

use std::io::Write;
use serde::{Serialize, Deserialize};
use crate::message::Message;
use crate::node::{Node, NodeInfo};


/// Payload for echo messages
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EchoPayload {
    Echo(EchoData),
    EchoOk(EchoData),
}

/// The data of an echo and echo ok message
#[derive(Serialize, Deserialize)]
pub struct EchoData {
    echo: String,
}

/// The Echo service implementation
pub struct EchoNode;

impl Node<EchoPayload> for EchoNode {
    fn from_init(_init: &NodeInfo) -> anyhow::Result<Self>
        where Self: Sized
    {
        Ok(EchoNode)
    }

    fn handle(&mut self, mut msg: Message<EchoPayload>, out: &mut impl Write)
        -> anyhow::Result<()>
    {
        // Take out the echo string
        let echo = match msg.body.payload.take() {
            Some(EchoPayload::Echo(data)) => data.echo,
            _ => return Ok(()),
        };

        // Create a reply and send it
        let reply = EchoPayload::EchoOk(EchoData { echo });
        msg.into_reply(reply).send(&mut *out)?;

        Ok(())
    }
}

pub fn run() {
    crate::node::main_loop::<EchoPayload, EchoNode>()
        .expect("Echo service failed");
}
