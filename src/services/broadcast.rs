//! Broadcast broadcast broadcast broadcast broadcast broadcast
//!
//! This implementation is fault tolerant but not perfectly optimized.
//! Every `GOSSIP_TIME`, each node sends out a read request to its neighbors,
//! which causes the neighbors to send out _all_ of the messages they've seen up
//! to that point. There are better ways to implement this; the best i can think
//! of is to simply retry unacknowledged broadcasts.
//! This implementation however is much more simpler and aligns well with our
//! architecture up to this point. And it works!

use std::time::{Instant, Duration};
use std::io::Write;
use std::collections::{HashMap, HashSet};
use serde::{Serialize, Deserialize};
use crate::message::Message;
use crate::node::{Node, NodeInfo};

/// How often a node gossips with its neighbors.
///
/// Every `GOSSIP_TIME` interval, a node sends a read request to its neighbors
/// to sync their latest seen values.
const GOSSIP_TIME: Duration = Duration::from_millis(300);

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

    /// The grace period for which a gossip won't be triggered.
    ///
    /// When handling a message, if the current time is greater than this timer,
    /// a gossip event will be triggered within the handle function and a new
    /// grace period will be calculated.
    gossip_timer: Instant,
}

impl BroadcastNode {
    /// Send a `Read` request to all neighbors of this node
    fn gossip(&mut self, out: &mut impl Write) -> anyhow::Result<()> {
        // If the grace period isn't over yet, don't gossip
        if self.gossip_timer > Instant::now() {
            return Ok(());
        }

        // Grace period is over; send out the reads to our neighbors
        for neighbor in self.topology.iter() {
            Message::new(self.id.clone(), neighbor.clone(), None, Payload::Read)
                .send(out)?;
        }

        // Calculate a new grace period
        self.gossip_timer = Instant::now().checked_add(GOSSIP_TIME)
            .expect("Gossip grace period overflow");
        Ok(())
    }
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
            gossip_timer: Instant::now().checked_add(GOSSIP_TIME)
                .expect("Gossip grace period overflow"),
        })
    }

    fn handle(&mut self, mut msg: Message<Payload>, out: &mut impl Write)
        -> anyhow::Result<()>
    {
        // Trigger a gossip if the timer has elapsed
        self.gossip(out)?;

        // Take out the payload from this message
        let payload = match msg.body.payload.take() {
            Some(payload) => payload,
            _ => return Ok(()),
        };

        // Handle the request
        match payload {
            // Ignore `OK`s
            Payload::TopologyOk | Payload::BroadcastOk => return Ok(()),

            // The gossip event generates read requests to our neighbors. If we
            // get a `ReadOk`, synchronize the seen values
            Payload::ReadOk(data) => {
                self.seen.extend(data.messages.iter());
            },

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
