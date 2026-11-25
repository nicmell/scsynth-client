use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use scsynth_osc::{Command, Packet};
use tokio::io::ReadBuf;
use tokio::net::{ToSocketAddrs, UdpSocket, lookup_host};

use crate::client::{Event, Listener, OscClient, SendError};
use crate::socket::{self, Reader, Tasks, Writer};

/// The largest datagram that a [`UdpClient`] sends by default: 8,192 bytes.
pub const DEFAULT_MAX_DATAGRAM: usize = 8192;

/// An [`OscClient`] over one UDP socket of tokio: one datagram for each
/// packet, each way.
///
/// It refuses a packet over its largest datagram with
/// [`SendError::TooLarge`], before anything leaves. An I/O error of a send
/// or of a receive comes as an [`Event::Error`], and the client goes on. Its
/// tasks stop and its socket closes at [`OscClient::close`], or when it
/// drops.
pub struct UdpClient {
    tasks: Tasks,
    max_datagram: usize,
}

impl UdpClient {
    /// A client of the UDP socket at `addr`, whose datagrams have at most
    /// [`DEFAULT_MAX_DATAGRAM`] bytes.
    ///
    /// It binds port 0 of the unspecified address of the family of `addr`,
    /// and connects to `addr`. It must run inside a runtime of tokio, where
    /// it spawns its reader task and its writer task.
    ///
    /// # Errors
    ///
    /// The error of the lookup of `addr`, or the error of the bind or of the
    /// connect for the last address of the lookup.
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        Self::connect_with_max_datagram(addr, DEFAULT_MAX_DATAGRAM).await
    }

    /// As [`Self::connect`], with datagrams of at most `max_datagram` bytes.
    ///
    /// # Errors
    ///
    /// As [`Self::connect`].
    pub async fn connect_with_max_datagram(
        addr: impl ToSocketAddrs,
        max_datagram: usize,
    ) -> io::Result<Self> {
        let mut last_error = None;
        for target in lookup_host(addr).await? {
            match bind_and_connect(target).await {
                Ok(socket) => {
                    let socket = Arc::new(socket);
                    let tasks = Tasks::spawn(Datagrams(Arc::clone(&socket)), Datagrams(socket));
                    return Ok(Self {
                        tasks,
                        max_datagram,
                    });
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "the address resolves to no socket address",
            )
        }))
    }
}

impl OscClient for UdpClient {
    fn send(&self, packet: &Packet<Command>) -> Result<(), SendError> {
        let datagram = packet.encode();
        if datagram.len() > self.max_datagram {
            return Err(SendError::TooLarge {
                size: datagram.len(),
                max: self.max_datagram,
            });
        }
        self.tasks.send(datagram)
    }

    fn listen(&self, listener: Listener) {
        self.tasks.listen(listener);
    }

    fn close(&self) {
        self.tasks.close();
    }
}

/// A socket bound to port 0 of the unspecified address of the family of
/// `target`, and connected to `target`.
async fn bind_and_connect(target: SocketAddr) -> io::Result<UdpSocket> {
    let local = match target {
        SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
        SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
    };
    let socket = UdpSocket::bind(local).await?;
    socket.connect(target).await?;
    Ok(socket)
}

/// The socket of a [`UdpClient`], which its reader task and its writer task
/// share.
struct Datagrams(Arc<UdpSocket>);

impl Reader for Datagrams {
    const ERRORS_CLOSE: bool = false;

    fn poll_read(&mut self, cx: &mut Context<'_>, buffer: &mut [u8]) -> Poll<io::Result<usize>> {
        let mut datagram = ReadBuf::new(buffer);
        ready!(self.0.poll_recv(cx, &mut datagram))?;
        Poll::Ready(Ok(datagram.filled().len()))
    }

    fn received(&mut self, bytes: &[u8], events: &mut Vec<Event>) -> bool {
        socket::push_messages(bytes, events);
        true
    }
}

impl Writer for Datagrams {
    fn poll_write(&mut self, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        self.0.poll_send(cx, bytes)
    }
}
