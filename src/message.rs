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
    /// Create a reply to this message
    pub fn into_reply<NewPayload>(self, p: NewPayload) -> Message<NewPayload> {
        Message {
            src: self.dst,
            dst: self.src,
            body: Body {
                id: Some(get_unique_id()),
                reply_id: self.body.id,
                payload: p,
            },
        }
    }
}

/// The body of a message
#[derive(Serialize, Deserialize, Debug)]
pub struct Body<Payload> {
    /// Unique identifier of this message
    #[serde(rename = "msg_id")]
    pub id: Option<u32>,

    /// For request/response, the message ID of the request
    #[serde(rename = "in_reply_to")]
    pub reply_id: Option<u32>,

    /// Help with this
    #[serde(flatten)]
    pub payload: Payload,
}
