use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use scsynth_osc::{Command, Packet};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::client::{ClientError, Event, Listener, OscClient, SendError};
use crate::socket::{self, Reader, Tasks, Writer};

/// The largest length of a packet that a [`TcpClient`] reads: 64 MiB, a
/// limit of the client alone (see TCP in the README).
const MAX_PACKET: usize = 64 << 20;

/// The largest packet that a length of 32 bits frames: scsynth 3.14.1 reads
/// the length as a signed integer (`SC_ComPort.cpp:368`).
const MAX_FRAMED: usize = 0x7FFF_FFFF;

/// An [`OscClient`] over one TCP stream of tokio: each packet comes after
/// its length, 32 bits in network byte order, each way.
///
/// A length that is negative or too large comes as an [`Event::Error`], and
/// then the client closes: after it, the stream cannot be read. An I/O error
/// of a read or of a write does the same. The end of the stream from the
/// other end closes the client. Its tasks stop and its stream closes at
/// [`OscClient::close`], or when it drops.
pub struct TcpClient {
    tasks: Tasks,
}

impl TcpClient {
    /// A client of the TCP stream to `addr`, with `TCP_NODELAY` on.
    ///
    /// It must run inside a runtime of tokio, where it spawns its reader
    /// task and its writer task.
    ///
    /// # Errors
    ///
    /// The error of the connect, or of the setting of `TCP_NODELAY`.
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (read, write) = stream.into_split();
        let reader = Frames {
            half: read,
            pending: Vec::new(),
        };
        let tasks = Tasks::spawn(reader, Stream(write));
        Ok(Self { tasks })
    }
}

impl OscClient for TcpClient {
    fn send(&self, packet: &Packet<Command>) -> Result<(), SendError> {
        let bytes = packet.encode();
        let Ok(length) = i32::try_from(bytes.len()) else {
            return Err(SendError::TooLarge {
                size: bytes.len(),
                max: MAX_FRAMED,
            });
        };
        let mut frame = Vec::with_capacity(4 + bytes.len());
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&bytes);
        self.tasks.send(frame)
    }

    fn listen(&self, listener: Listener) {
        self.tasks.listen(listener);
    }

    fn close(&self) {
        self.tasks.close();
    }
}

/// The read half of the stream of a [`TcpClient`], with the bytes that
/// arrived after the last whole packet.
struct Frames {
    half: OwnedReadHalf,
    pending: Vec<u8>,
}

impl Reader for Frames {
    const ERRORS_CLOSE: bool = true;

    fn poll_read(&mut self, cx: &mut Context<'_>, buffer: &mut [u8]) -> Poll<io::Result<usize>> {
        let mut read = ReadBuf::new(buffer);
        ready!(Pin::new(&mut self.half).poll_read(cx, &mut read))?;
        Poll::Ready(Ok(read.filled().len()))
    }

    fn received(&mut self, bytes: &[u8], events: &mut Vec<Event>) -> bool {
        if bytes.is_empty() {
            return false;
        }
        self.pending.extend_from_slice(bytes);
        let mut start = 0;
        let open = loop {
            let Some(&[a, b, c, d]) = self.pending.get(start..start + 4) else {
                break true;
            };
            let length = i32::from_be_bytes([a, b, c, d]);
            let Some(size) = usize::try_from(length)
                .ok()
                .filter(|size| *size <= MAX_PACKET)
            else {
                let error = format!("a packet length of {length} bytes, outside 0 to {MAX_PACKET}");
                events.push(Event::Error(ClientError::Transport(error)));
                break false;
            };
            let Some(packet) = self.pending.get(start + 4..start + 4 + size) else {
                break true;
            };
            socket::push_messages(packet, events);
            start += 4 + size;
        };
        self.pending.drain(..start);
        open
    }
}

/// The write half of the stream of a [`TcpClient`].
struct Stream(OwnedWriteHalf);

impl Writer for Stream {
    fn poll_write(&mut self, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, bytes)
    }
}
