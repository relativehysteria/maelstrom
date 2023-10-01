use core::arch::x86_64::_rdtsc;
use std::io::Write;
use serde::{Serialize, Deserialize};
use crate::message as msg;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
/// Payloads handled by the UUID server
pub enum Payload {
    Generate,
    GenerateOk { id: u128 },
}

/// A node in the UUID service cluster
pub struct UUIDNode {
    _id: String,

    /// Internal PRNG state
    state: u128,
}

impl UUIDNode {
    /// Get the next 128bit pseudo-random integer. This implements 128b xorshift
    fn next_rng(&mut self) -> u128 {
        let ret = self.state;
        self.state ^= self.state << 31;
        self.state ^= self.state >> 11;
        self.state ^= self.state << 63;
        ret
    }
}

impl msg::Node<Payload> for UUIDNode {
    fn from_init(init: &msg::Init) -> anyhow::Result<Self> {
        Ok(Self {
            _id: init.node_id.clone(),
            state: unsafe { ((_rdtsc() as u128) << 64) + (_rdtsc() as u128) },
        })
    }

    fn step(&mut self, input: msg::Message<Payload>, output: &mut dyn Write)
            -> anyhow::Result<()> {
        // We will change the input into a reply later on, so mark it mutable
        let mut input = input;
        let id = input.body.id;

        match input.body.payload {
            Payload::Generate => {
                input.body.payload = Payload::GenerateOk {
                    id: self.next_rng(),
                };
                input.into_reply(id).send(output)
            },
            Payload::GenerateOk{ .. } => Ok(()),
        }
    }
}

pub fn main() -> anyhow::Result<()> {
    msg::main_loop::<Payload, UUIDNode>()
}
