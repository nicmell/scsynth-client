use std::fmt;

use crate::client::SendError;

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
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Send(error) => Some(error),
            Self::Failed { .. } | Self::Timeout | Self::NoReply { .. } => None,
        }
    }
}
