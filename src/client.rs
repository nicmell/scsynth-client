use std::fmt;

use scsynth_osc::{Command, DecodeError, Packet, RawMessage, Reply};

/// `Send` on a native target, and nothing on `wasm32`, where a value of
/// JavaScript is not `Send`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + ?Sized> MaybeSend for T {}

/// `Send` on a native target, and nothing on `wasm32`, where a value of
/// JavaScript is not `Send`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}

#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSend for T {}

/// `Sync` on a native target, and nothing on `wasm32`, where a value of
/// JavaScript is not `Sync`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSync: Sync {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Sync + ?Sized> MaybeSync for T {}

/// `Sync` on a native target, and nothing on `wasm32`, where a value of
/// JavaScript is not `Sync`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSync {}

#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSync for T {}

/// The function that gets the events of an [`OscClient`]: `Send` on a
/// native target.
#[cfg(not(target_arch = "wasm32"))]
pub type Listener = Box<dyn FnMut(Event) + Send>;

/// The function that gets the events of an [`OscClient`]: `Send` on a
/// native target.
#[cfg(target_arch = "wasm32")]
pub type Listener = Box<dyn FnMut(Event)>;

/// A connection to scsynth that carries the packets of scsynth-osc: a
/// transport.
pub trait OscClient: MaybeSend + MaybeSync + 'static {
    /// Queues one packet for the other end. It waits for no network and no
    /// reply. A write that fails later comes to the listener as an
    /// [`Event::Error`].
    ///
    /// # Errors
    ///
    /// [`SendError::TooLarge`] for a packet over the limit of the transport,
    /// and [`SendError::Closed`] after the close.
    fn send(&self, packet: &Packet<Command>) -> Result<(), SendError>;

    /// Sets the one listener, which gets the events in the order of their
    /// arrival, one call at a time. The transport keeps what arrives until a
    /// listener is set. A second call replaces the first listener.
    fn listen(&self, listener: Listener);

    /// Closes the connection. The listener gets [`Event::Closed`] once.
    fn close(&self);
}

/// What the listener of an [`OscClient`] gets.
#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    /// One message from the other end. A bundle arrives as its messages, in
    /// order.
    Message(Incoming),
    /// Bytes that are no OSC packet, or an error that the connection
    /// survives.
    Error(ClientError),
    /// The connection is closed, and no event follows.
    Closed,
}

/// One message from scsynth.
#[derive(Clone, Debug, PartialEq)]
pub enum Incoming {
    /// A reply that scsynth-osc types.
    Reply(Reply),
    /// A message that does not decode as a typed reply, whatever the error:
    /// an unknown address, such as the reply of `SendReply`, or arguments
    /// that the address does not take.
    Other(RawMessage),
}

/// Why [`OscClient::send`] refuses a packet.
#[derive(Clone, Debug, PartialEq)]
pub enum SendError {
    /// The packet is over the limit of the transport.
    TooLarge {
        /// The bytes of the packet.
        size: usize,
        /// The most bytes that the transport sends in one packet.
        max: usize,
    },
    /// The client is closed.
    Closed,
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge { size, max } => write!(
                f,
                "the packet has {size} bytes, over the limit of {max} bytes"
            ),
            Self::Closed => write!(f, "the client is closed"),
        }
    }
}

impl std::error::Error for SendError {}

/// An error that the connection survives, in an [`Event::Error`].
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ClientError {
    /// Bytes from the other end that do not decode: the error of
    /// scsynth-osc.
    Decode(DecodeError),
    /// An error of the transport, such as a failed write: its text.
    Transport(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(_) => write!(f, "bytes from the other end do not decode"),
            Self::Transport(message) => write!(f, "the transport failed: {message}"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Decode(error) => Some(error),
            Self::Transport(_) => None,
        }
    }
}

/// The messages in the bytes of one OSC packet, in order: each one typed as
/// a [`Reply`] when it decodes as one, and [`Incoming::Other`] when not.
///
/// # Errors
///
/// [`ClientError::Decode`] when the bytes are not one OSC packet.
pub fn decode(bytes: &[u8]) -> Result<Vec<Incoming>, ClientError> {
    if let Ok(packet) = Packet::<Reply>::decode(bytes) {
        return Ok(packet
            .into_messages()
            .into_iter()
            .map(Incoming::Reply)
            .collect());
    }
    // The typed decode refuses the whole packet for one untyped message.
    let packet = Packet::<RawMessage>::decode(bytes).map_err(ClientError::Decode)?;
    Ok(packet
        .into_messages()
        .into_iter()
        .map(|raw| match Reply::from_raw(&raw) {
            Ok(reply) => Incoming::Reply(reply),
            Err(_) => Incoming::Other(raw),
        })
        .collect())
}
