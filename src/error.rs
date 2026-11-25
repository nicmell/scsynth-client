use std::fmt;

use crate::client::SendError;
use crate::ids::IdKind;

/// Why a request to scsynth gives no result.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Error {
    /// scsynth answered `/fail` for a command of the request.
    Failed {
        /// The command that failed, as scsynth names it.
        address: String,
        /// The error, verbatim.
        message: String,
        /// The number that scsynth adds to some failures: the buffer of a
        /// buffer command, or the client id of a sender that is already
        /// registered.
        value: Option<i32>,
    },
    /// The answer of scsynth did not come within the limit.
    Timeout,
    /// The transport refused the packet, or it is closed.
    Send(SendError),
    /// scsynth answered the call without the reply, or the value of a reply,
    /// that the method reads.
    NoReply {
        /// The command whose reply is missing.
        address: &'static str,
    },
    /// The method needs [`Scsynth::register`](crate::Scsynth::register)
    /// first.
    NotRegistered,
    /// The client id has no range of node ids: sclang and this crate split
    /// the node ids among 32 clients, 0 to 31.
    NoNodeRange {
        /// The client id that scsynth gave.
        client_id: i32,
    },
    /// No free id of this kind is left in the range of the client.
    Exhausted(IdKind),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // scsynth ends some messages with a newline.
            Self::Failed {
                address, message, ..
            } => write!(f, "scsynth failed {address}: {}", message.trim_end()),
            Self::Timeout => write!(f, "scsynth did not answer within the limit"),
            Self::Send(_) => write!(f, "the transport did not send the packet"),
            Self::NoReply { address } => {
                write!(f, "scsynth answered without the reply of {address}")
            }
            Self::NotRegistered => write!(f, "the client is not registered"),
            Self::NoNodeRange { client_id } => {
                write!(f, "the client id {client_id} has no range of node ids")
            }
            Self::Exhausted(kind) => {
                let kind = match kind {
                    IdKind::Buffer => "buffer",
                    IdKind::ControlBus => "control bus",
                    IdKind::AudioBus => "audio bus",
                };
                write!(f, "no free {kind} is left in the range of the client")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Send(error) => Some(error),
            Self::Failed { .. }
            | Self::Timeout
            | Self::NoReply { .. }
            | Self::NotRegistered
            | Self::NoNodeRange { .. }
            | Self::Exhausted(_) => None,
        }
    }
}
