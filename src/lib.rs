#![cfg_attr(all(feature = "tokio", not(target_arch = "wasm32")), doc = include_str!("../README.md"))]
#![cfg_attr(
    not(all(feature = "tokio", not(target_arch = "wasm32"))),
    doc = "A client of scsynth, the synthesis server of SuperCollider. The README is its reference."
)]

/// The OSC protocol of scsynth: the types of the commands and the replies.
pub use scsynth_osc;
/// The synth definitions of SuperCollider: the definitions that scsynth loads.
pub use scsynth_synthdef;
