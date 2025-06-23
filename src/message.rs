use std::io::Write;
use std::sync::atomic::{AtomicU32, Ordering};
use serde::{Serialize, Deserialize};


/// Get a new message ID that is guaranteed to be unique for this node
pub fn get_unique_id() -> u32 {
    static ID: AtomicU32 = AtomicU32::new(0);
    ID.fetch_add(1, Ordering::SeqCst)
}

/// A message sent throughout the maelstrom network
#[derive(Serialize, Deserialize, Debug)]
pub struct Message<Payload> {
    /// A string identifying the node this message came from
    pub src: String,

    /// A string identifying the node this message is to
    #[serde(rename = "dest")]
    pub dst: String,

    /// Body and payload of the message
    pub body: Body<Payload>,
}

impl<Payload> Message<Payload> {
    /// Create a new message
    pub fn new(src: String, dst: String, reply_id: Option<u32>,
            payload: Payload) -> Self {
        Self {
            src: src,
            dst: dst,
            body: Body {
                payload: Some(payload),
                id: Some(get_unique_id()),
                reply_id,
            },
        }
    }

    /// Create a reply to this message
    pub fn into_reply<NewPayload>(self, p: NewPayload) -> Message<NewPayload> {
        Message::new(self.dst, self.src, self.body.id, p)
    }

    /// Send this message to `output`
    pub fn send(self, output: &mut impl Write) -> anyhow::Result<()>
    where
        Payload: Serialize
    {
        serde_json::to_writer(&mut *output, &self)?;
        output.write(b"\n")?;
        Ok(())
    }
}

/// The body of a message
#[derive(Serialize, Deserialize, Debug)]
pub struct Body<Payload> {
    /// Unique identifier of this message
    #[serde(rename = "msg_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<u32>,

    /// For request/response, the message ID of the request
    #[serde(rename = "in_reply_to", skip_serializing_if = "Option::is_none")]
    pub reply_id: Option<u32>,

    /// The payload of the packet.
    ///
    /// This is an option because that allows us to take out the payload from an
    /// incoming packet and then use `into_reply()` to create a response using
    /// the original payload.
    #[serde(flatten)]
    pub payload: Option<Payload>,
}
