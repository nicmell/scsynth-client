use std::time::Duration;

/// The settings of a [`Scsynth`](crate::Scsynth).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
    /// The limit of each awaited method: 3 seconds by default.
    pub timeout: Duration,
    /// The client id that [`register`](crate::Scsynth::register) asks for in
    /// `/notify`. `None`, the default, lets scsynth pick one.
    pub client_id: Option<i32>,
    /// The login limit of the server, its `-l`.
    /// [`register`](crate::Scsynth::register) reads it only when the reply of
    /// `/notify` does not give it. `None` by default.
    pub max_logins: Option<i32>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(3),
            client_id: None,
            max_logins: None,
        }
    }
}
