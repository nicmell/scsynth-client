#![cfg_attr(all(feature = "tokio", not(target_arch = "wasm32")), doc = include_str!("../README.md"))]
#![cfg_attr(
    not(all(feature = "tokio", not(target_arch = "wasm32"))),
    doc = "A client of scsynth, the synthesis server of SuperCollider. The README is its reference."
)]

mod channel;
mod client;
mod core;
mod error;
mod ids;
mod options;
mod scsynth;
#[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
mod socket;
#[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
mod tcp;
mod timer;
#[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
mod udp;
mod wait;
#[cfg(all(feature = "web", target_arch = "wasm32"))]
mod web;

pub use channel::{ChannelClient, ChannelPeer};
pub use client::{
    ClientError, Event, Incoming, Listener, MaybeSend, MaybeSync, OscClient, SendError, decode,
};
pub use error::Error;
pub use ids::IdKind;
pub use options::Options;
pub use scsynth::{Registration, Scsynth, Subscription, Target};
#[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
pub use tcp::TcpClient;
#[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
pub use timer::TokioTimer;
#[cfg(all(feature = "web", target_arch = "wasm32"))]
pub use timer::WebTimer;
pub use timer::{Sleep, Timer};
#[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
pub use udp::{DEFAULT_MAX_DATAGRAM, UdpClient};
#[cfg(all(feature = "web", target_arch = "wasm32"))]
pub use web::{PortClient, WebSocketClient};

/// The OSC protocol of scsynth: the types of the commands and the replies.
pub use scsynth_osc;
/// The synth definitions of SuperCollider: the definitions that scsynth loads.
pub use scsynth_synthdef;
