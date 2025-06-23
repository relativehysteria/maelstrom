//! Broadcast broadcast broadcast broadcast broadcast broadcast
//!
//! This implementation does NOT handle lost packets well! In a real
//! microservice cluster, all nodes would be connected in such a way that a lost
//! message from one node would not hinder the network, because that same
//! message would be sent by another node. There are many corner cases, the
//! simplest one is a leaf node, i.e. a node with only one neighbor. If a
//! broadcast packet is lost in this connection, that broadcast will be lost
//! on the recipient forever, because we choose to ignore `BroadcastOk` packets.
//!
use std::io::Write;
use std::collections::{HashMap, HashSet};
use serde::{Serialize, Deserialize};
use crate::message::Message;
use crate::node::{Node, NodeInfo};

/// Payload for the broadcast part of the challenge. This includes all of the
/// possible message payloads
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Payload {
    Read,
    ReadOk(ReadData),
    Topology(TopologyData),
    TopologyOk,
    Broadcast(BroadcastData),
    BroadcastOk,
}

/// Data sent to a node requesting a read
#[derive(Serialize, Deserialize)]
pub struct ReadData {
    /// The messages this node has seen as of replying to the request
    messages: HashSet<u32>,
}

/// Data sent to this node to define its neighbors
#[derive(Serialize, Deserialize)]
pub struct TopologyData {
    /// Table of all nodes and their neighbors
    topology: HashMap<String, HashSet<String>>,
}

/// Data sent in a broadcast
#[derive(Serialize, Deserialize, Clone)]
pub struct BroadcastData {
    message: u32,
}

/// The broadcast service implementation
pub struct BroadcastNode {
    /// ID of this node
    id: String,

    /// Neighboring nodes. All outgoing broadcast messages are sent to each of
    /// these neighbors
    topology: HashSet<String>,

    /// A list of messages that have been seen and processed by this node. This
    /// vector is used to prevent re-broadcasting
    seen: HashSet<u32>,
}

impl Node<Payload> for BroadcastNode {
    fn from_init(init: NodeInfo) -> anyhow::Result<Self>
    where
        Self: Sized
    {
        Ok(BroadcastNode {
            id:       init.id,
            topology: HashSet::with_capacity(init.nodes.len()),
            seen:     HashSet::new(),
        })
    }

    fn handle(&mut self, mut msg: Message<Payload>, out: &mut impl Write)
        -> anyhow::Result<()>
    {
        // Take out the payload from this message
        let payload = match msg.body.payload.take() {
            Some(payload) => payload,
            _ => return Ok(()),
        };

        // Handle the request and also ignore `Ok`s. In reality, we would make
        // sure that all nodes are synchronized, but since that's not the part
        // of the problem, we are safe to ignore these
        match payload {
            // Ignore OK
            Payload::ReadOk(_) | Payload::TopologyOk
                | Payload::BroadcastOk => return Ok(()),

            // If we got a topology, change our topology
            Payload::Topology(mut data) => {
                if let Some(our_topology) = data.topology.remove(&self.id) {
                    self.topology = our_topology;
                };
                msg.into_reply(Payload::TopologyOk).send(out)?;
            },

            // If we got a read request, send out the messages we've seen
            Payload::Read => {
                let data = ReadData { messages: self.seen.clone() };
                msg.into_reply(Payload::ReadOk(data)).send(out)?;
            },

            // If we got a broadcast, save it and broadcast it to our neighbors,
            // unless we've seen this message already
            Payload::Broadcast(data) => {
                // Reply to the sender immediately
                msg.into_reply(Payload::BroadcastOk).send(out)?;

                // If we haven't seen it yet, broadcast it to neighbors
                if self.seen.insert(data.message) {
                    for neighbor in self.topology.iter() {
                        Message::new(self.id.clone(), neighbor.clone(),
                                     None, Payload::Broadcast(data.clone()))
                            .send(out)?;
                    }
                }
            },
        }

        Ok(())
    }
}

pub fn run() {
    crate::node::main_loop::<Payload, BroadcastNode>()
        .expect("Uuid service failed");
}
