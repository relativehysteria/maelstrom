//! The UUID algo is simple: current timestamp + rdtsc.
//!
//! There's a chance of collision if at the same timestamp, two nodes have the
//! same rdtsc. This is impossible if multiple nodes run on one machines, and
//! extremely improbable if nodes run on multiple machines.
//!
//! A hypervisor is free to spoof both the timestamp and tsc :p
use std::time::SystemTime;
use std::arch::x86_64::_rdtsc;
use std::io::Write;
use serde::{Serialize, Deserialize};
use crate::message::Message;
use crate::node::{Node, NodeInfo};

/// Payload for UUID messages
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Payload {
    Generate,
    GenerateOk(Uuid)
}


/// Data returned to UUID requests
#[derive(Serialize, Deserialize)]
pub struct Uuid {
    id: u128,
}

/// The UUID service implementation
pub struct UuidNode;

impl UuidNode {
    /// Generate a UUID that is guaranteed to be unique on this machine.
    ///
    /// There's an almost impossible chance that this UUID will collide with
    /// another UUID returned by another node if the nodes have the same TSC at
    /// the same timestamp.
    fn uuid() -> u128 {
        let time: u128 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("SystemTime before unix epoch.")
            .as_secs()
            .into();
        let tsc: u128 = unsafe { _rdtsc().into() };
        time << 64 | tsc
    }
}

impl Node<Payload> for UuidNode {
    fn from_init(_init: NodeInfo) -> anyhow::Result<Self>
        where Self: Sized
    {
        Ok(UuidNode)
    }

    fn handle(&mut self, msg: Message<Payload>, out: &mut impl Write)
        -> anyhow::Result<()>
    {
        // Only reply to `generate` requests
        match msg.body.payload {
            Some(Payload::Generate) => (),
            _ => return Ok(()),
        };

        // Create a reply and send it
        let reply = Payload::GenerateOk(Uuid { id: UuidNode::uuid() });
        msg.into_reply(reply).send(&mut *out)?;

        Ok(())
    }
}

pub fn run() {
    crate::node::main_loop::<Payload, UuidNode>()
        .expect("Uuid service failed");
}
