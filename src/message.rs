use std::io::{Write, BufRead};
use serde::{de::DeserializeOwned, Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
/// Message passed around the network. This message is generic over all services
pub struct Message<Payload> {
    /// A string identifying the node this message came from
    pub src: String,

    /// A string identifying the node this message is to
    #[serde(rename = "dest")]
    pub dst: String,

    /// The body and payload of the message
    pub body: Body<Payload>,
}

impl<Payload> Message<Payload> {
    /// Build a reply out of this message, replying to `id`
    pub fn into_reply(mut self, id: Option<usize>) -> Self {
        // Switch the source and destinations
        let src = self.src;
        self.src = self.dst;
        self.dst = src;

        // Set the correct IDs
        self.body.id = self.body.id.map(|sid| sid + 1);
        self.body.reply_id = id;

        self
    }

    /// Send the message through `out`
    pub fn send(&self, out: &mut dyn Write) -> anyhow::Result<()>
        where Payload: Serialize,
    {
        serde_json::to_writer(&mut *out, self)?;
        out.write_all("\n".as_bytes())?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
/// Internal body of the message; ID metadata and the internal payload
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    /// A unique integer identifier
    pub id: Option<usize>,

    #[serde(rename = "in_reply_to")]
    /// For req/response, the msg_id of the request
    pub reply_id: Option<usize>,

    #[serde(flatten, rename = "type")]
    /// A string identifying the type of message this is
    pub payload: Payload,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
/// Init payload. Used on node initialization
enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Debug, Serialize, Deserialize)]
/// Initialization metadata
pub struct Init {
    /// ID of the node which is receiving this message
    pub node_id: String,

    /// All nodes in the cluster, including the recipient
    pub node_ids: Vec<String>,
}

/// Trait generic over `Payload` that makes it possible to build
/// distributed systems.
pub trait Node<Payload> {
    /// Given the `init` struct, creates a new `Node` in the cluster
    fn from_init(init: &Init) -> anyhow::Result<Self>
        where Self: Sized;

    /// Single steps through the main event loop of the node.
    /// The node should handle the incoming `input` message and send the
    /// appropriate responses through `output`
    fn step(&mut self, input: Message<Payload>, output: &mut dyn Write)
        -> anyhow::Result<()>;
}

/// Implementation of the main loop generic over a service `Node<Payload>` impl
pub fn main_loop<P, N>() -> anyhow::Result<()>
where
    P: DeserializeOwned + core::fmt::Debug,
    N: Node<P>,
{
    // Lock the IO
    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    let mut stdout = std::io::stdout().lock();

    // Get the init message
    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin.next().expect("no init msg received")?)?;

    // Build the node from the init message
    let InitPayload::Init(init) = init_msg.body.payload else {
        panic!("First message must be init!");
    };
    let mut node = N::from_init(&init)?;

    // Reply to the init message
    Message {
        src: init_msg.dst,
        dst: init_msg.src,
        body: Body {
            id: Some(0),
            reply_id: init_msg.body.id,
            payload: InitPayload::InitOk,
        },
    }.send(&mut stdout)?;

    // Re-lock the input
    drop(stdin);
    let stdin = std::io::stdin().lock();

    // Go through each message received and handle it
    for line in stdin.lines() {
        let msg: Message<P> = serde_json::from_str(&line?)?;
        node.step(msg, &mut stdout)?;
    }

    Ok(())
}
