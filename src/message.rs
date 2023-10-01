use std::io::{Write, BufRead};
use serde::{de::DeserializeOwned, Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message<Payload> {
    /// A string identifying the node this message came from
    pub src: String,

    /// A string identifying the node this message is to
    #[serde(rename = "dest")]
    pub dst: String,

    /// The payload of the message
    pub body: Body<Payload>,
}

impl<Payload> Message<Payload> {
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

    pub fn send(&self, out: &mut dyn Write) -> anyhow::Result<()>
        where Payload: Serialize,
    {
        serde_json::to_writer(&mut *out, self)?;
        out.write_all("\n".as_bytes())?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
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
enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

pub trait Node<Payload> {
    fn from_init(init: &Init) -> anyhow::Result<Self>
        where Self: Sized;

    fn step(&mut self, input: Message<Payload>, output: &mut dyn Write)
        -> anyhow::Result<()>;
}

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
