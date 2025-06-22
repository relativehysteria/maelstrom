use std::io::{Write, BufRead};
use serde::{Serialize, Deserialize};
use serde::de::DeserializeOwned;
use serde_json;
use crate::message::Message;


/// Messages sent between nodes on initialization
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InitPayload {
    /// Payload that is sent to nodes on initialization
    Init(NodeInfo),

    /// Payload that is sent by initialized nodes
    InitOk,
}

/// Payload data of the initial message
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeInfo {
    /// The ID of the node which is receiving this message
    #[serde(rename = "node_id")]
    pub id: String,

    /// Nodes in the cluster, including the recipient
    #[serde(rename = "node_ids")]
    pub nodes: Vec<String>,
}

pub trait Node<Payload> {
    /// Given the `init` node information, creates a new node in the cluster
    fn from_init(init: &NodeInfo) -> anyhow::Result<Self>
        where Self: Sized;

    /// Handle an input `msg` and send a reply to `out`
    ///
    /// Taking a reference to output makes it impossible to parallelize workload
    /// using threads. A fix for this would be returning a message, message
    /// queue or a serialized string which the caller would then handle for us.
    ///
    /// This is unnecessary in a microservice architecture where services are
    /// scaled by nodes (i.e. OS processes), not by threads.
    fn handle(&mut self, msg: Message<Payload>, out: &mut impl Write)
        -> anyhow::Result<()>;
}

/// The main loop of this node generic over the payload expected to be handled
/// by this node
pub fn main_loop<P, N>() -> anyhow::Result<()>
where
    N: Node<P>,
    P: DeserializeOwned,
{
    // Get the input and output handles
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    // Wait for the init payload to come
    let init = stdin.lines().next().expect("Couldn't read line from stdin")?;
    let init: Message<InitPayload> = serde_json::from_str(&init)
        .expect("Couldn't parse the initial message");

    // Create the node
    let mut node = match init.body.payload {
        Some(InitPayload::Init(ref data)) => N::from_init(data)?,
        _ => panic!("Initial is has no valid init payload"),
    };

    // Reply with OK
    init.into_reply(InitPayload::InitOk).send(&mut stdout)?;

    // Because we have used `stdin.lines()` above, we have to re-lock stdin
    let stdin = std::io::stdin().lock();

    // Start the service loop
    for line in stdin.lines() {
        let msg = serde_json::from_str(&line?)?;
        node.handle(msg, &mut stdout)?;
    }

    Ok(())
}
