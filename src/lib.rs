#![cfg_attr(all(feature = "tokio", not(target_arch = "wasm32")), doc = include_str!("../README.md"))]
#![cfg_attr(
    not(all(feature = "tokio", not(target_arch = "wasm32"))),
    doc = "A client of scsynth, the synthesis server of SuperCollider. The README is its reference."
)]

mod channel;
mod client;
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "its one user, the client `Scsynth`, comes in the next step"
    )
)]
mod core;
mod error;

pub use channel::{ChannelClient, ChannelPeer};
pub use client::{
    ClientError, Event, Incoming, Listener, MaybeSend, MaybeSync, OscClient, SendError, decode,
};
pub use error::Error;

/// The OSC protocol of scsynth: the types of the commands and the replies.
pub use scsynth_osc;
/// The synth definitions of SuperCollider: the definitions that scsynth loads.
pub use scsynth_synthdef;
