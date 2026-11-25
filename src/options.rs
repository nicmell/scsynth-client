use std::time::Duration;

/// The settings of a [`Scsynth`](crate::Scsynth).
///
/// No reply of scsynth gives the counts of the server: they must be those of
/// the flags that the server runs with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
    /// The limit of each awaited method: 3 seconds by default.
    pub timeout: Duration,
    /// The client id that [`register`](crate::Scsynth::register) asks for in
    /// `/notify`. `None`, the default, lets scsynth pick one.
    pub client_id: Option<i32>,
    /// The login limit that scsynth reports in the reply of `/notify`: its
    /// `-l`, rounded up to a power of two.
    /// [`register`](crate::Scsynth::register) reads it only when the reply of
    /// `/notify` does not give it. `None` by default.
    pub max_logins: Option<i32>,
    /// The number of buffers of the server, its `-b`: 1024 by default.
    pub buffers: i32,
    /// The number of control buses of the server, its `-c`: 16384 by
    /// default.
    pub control_buses: i32,
    /// The number of audio buses of the server, its `-a`, with the outputs
    /// and the inputs: 1024 by default.
    pub audio_buses: i32,
    /// The number of input buses of the server, its `-i`: 8 by default.
    pub inputs: i32,
    /// The number of output buses of the server, its `-o`: 8 by default.
    pub outputs: i32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(3),
            client_id: None,
            max_logins: None,
            buffers: 1024,
            control_buses: 16384,
            audio_buses: 1024,
            inputs: 8,
            outputs: 8,
        }
    }
}
