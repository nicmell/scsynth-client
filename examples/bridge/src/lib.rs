//! The bridge of the examples of scsynth-client: a program that lets a page
//! reach scsynth. It accepts websockets, and pipes each one to its own TCP
//! connection to scsynth. Its README states its rules.

use std::fmt;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use scsynth_client::scsynth_osc::command::top_level::Notify;
use scsynth_client::scsynth_osc::{Command, Packet};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::http::header::ORIGIN;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

/// The address of the websockets without `--listen`.
pub const DEFAULT_LISTEN: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 57120));

/// The TCP address of scsynth without `--scsynth`.
pub const DEFAULT_SCSYNTH: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 57110));

/// The origin whose pages can connect without `--origin`: the dev server of
/// Vite.
pub const DEFAULT_ORIGIN: &str = "http://localhost:5173";

/// The text that `--help` prints.
pub const USAGE: &str = "\
Usage: scsynth-client-bridge [--listen <addr>] [--scsynth <addr>] [--origin <origin>]...

Lets a page reach scsynth: pipes each websocket to its own TCP connection to scsynth.

Options:
  --listen <addr>     the address of the websockets [default: 127.0.0.1:57120]
  --scsynth <addr>    the TCP address of scsynth, its -t [default: 127.0.0.1:57110]
  --origin <origin>   an origin whose pages can connect, repeatable [default: http://localhost:5173]
  --help              print this text
";

/// The largest length of a packet that the bridge reads from scsynth: 64
/// MiB, as the `TcpClient` of scsynth-client. scsynth sets no such limit.
const MAX_PACKET: usize = 64 << 20;

/// The bytes that the bridge asks to read from scsynth at a time.
const READ_SIZE: usize = 8192;

/// How long the bridge waits for each end of a pair to close, after it
/// starts the close.
const CLOSE_LIMIT: Duration = Duration::from_secs(1);

/// The largest reason of a close frame, in bytes: a control frame holds 125
/// bytes (RFC 6455, 5.5), and the code takes 2. tungstenite does not check
/// the frames that it sends.
const MAX_REASON: usize = 123;

/// What the bridge runs with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// The address of the websockets.
    pub listen: SocketAddr,
    /// The TCP address of scsynth.
    pub scsynth: SocketAddr,
    /// The origins whose pages can connect, as a browser sends them in the
    /// `Origin` header.
    pub origins: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: DEFAULT_LISTEN,
            scsynth: DEFAULT_SCSYNTH,
            origins: vec![DEFAULT_ORIGIN.to_owned()],
        }
    }
}

/// What the arguments ask for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Run the bridge.
    Run(Config),
    /// Print [`USAGE`].
    Help,
}

/// The action of the arguments, which come without the name of the
/// program. Each `--origin` adds one origin, and the first one replaces
/// [`DEFAULT_ORIGIN`].
///
/// # Errors
///
/// The text of the error: an unknown argument, a flag without its value,
/// or an address that does not parse.
pub fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Action, String> {
    let mut config = Config::default();
    let mut origins = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" => return Ok(Action::Help),
            "--listen" => config.listen = address(&arg, args.next())?,
            "--scsynth" => config.scsynth = address(&arg, args.next())?,
            "--origin" => origins.push(args.next().ok_or(format!("{arg} needs an origin"))?),
            _ => return Err(format!("unknown argument {arg}")),
        }
    }
    if !origins.is_empty() {
        config.origins = origins;
    }
    Ok(Action::Run(config))
}

/// The address that follows `flag`.
fn address(flag: &str, value: Option<String>) -> Result<SocketAddr, String> {
    let value = value.ok_or(format!("{flag} needs an address"))?;
    value
        .parse()
        .map_err(|error| format!("{flag} {value}: {error}"))
}

/// Accepts websockets on `listener`, and pipes each one to its own TCP
/// connection to `config.scsynth`, in a task of its own. It logs its start
/// to stderr, and runs until its task stops.
pub async fn serve(listener: TcpListener, config: Config) {
    let listen = listener.local_addr().unwrap_or(config.listen);
    eprintln!(
        "bridge: listening on ws://{listen} for the origins {}, to scsynth at {} over TCP",
        config.origins.join(", "),
        config.scsynth,
    );
    let config = Arc::new(config);
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                tokio::spawn(connection(stream, peer, Arc::clone(&config)));
            }
            Err(error) => {
                eprintln!("bridge: accept failed: {error}");
                // Without a free file descriptor, each accept fails at once.
                time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// A TCP connection to scsynth at `addr`, with `TCP_NODELAY` on.
///
/// # Errors
///
/// The error of the connect, or of the setting of `TCP_NODELAY`.
pub async fn connect(addr: SocketAddr) -> io::Result<TcpStream> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

/// The websocket of a page.
type WebSocket = WebSocketStream<TcpStream>;

/// One pair: the handshake of the websocket from `peer`, its TCP
/// connection to scsynth, the pipe, and the close.
async fn connection(stream: TcpStream, peer: SocketAddr, config: Arc<Config>) {
    let _ = stream.set_nodelay(true);
    let mut origin = None;
    let mut refused = false;
    #[allow(
        clippy::result_large_err,
        reason = "the `Callback` of tungstenite sets the type of the error"
    )]
    let callback = |request: &Request, response: Response| {
        let Some(value) = request.headers().get(ORIGIN) else {
            return Ok(response);
        };
        origin = Some(String::from_utf8_lossy(value.as_bytes()).into_owned());
        let allowed = config
            .origins
            .iter()
            .any(|allowed| allowed.as_bytes() == value.as_bytes());
        if allowed {
            Ok(response)
        } else {
            refused = true;
            let mut refusal = ErrorResponse::new(None);
            *refusal.status_mut() = StatusCode::FORBIDDEN;
            Err(refusal)
        }
    };
    let accepted = tokio_tungstenite::accept_hdr_async(stream, callback).await;
    let origin = origin.unwrap_or_else(|| "none".to_owned());
    let mut ws = match accepted {
        Ok(ws) => ws,
        Err(_) if refused => {
            eprintln!("bridge: {peer} refused: the origin {origin} is not allowed");
            return;
        }
        Err(error) => {
            eprintln!("bridge: {peer} refused: {error}");
            return;
        }
    };
    eprintln!("bridge: {peer} accepted, origin {origin}");
    let (end, tcp) = match connect(config.scsynth).await {
        Ok(mut tcp) => (pipe(&mut ws, &mut tcp).await, Some(tcp)),
        Err(error) => (End::Unreachable(config.scsynth, error), None),
    };
    let reason = end.to_string();
    let unregistered = async {
        if let Some(tcp) = tcp {
            let _ = time::timeout(CLOSE_LIMIT, unregister(tcp)).await;
        }
    };
    tokio::join!(close(ws, end.close_code(), &reason), unregistered);
    eprintln!("bridge: {peer} closed: {end}");
}

/// Why a pair ends.
#[derive(Debug)]
enum End {
    /// The connect to scsynth failed.
    Unreachable(SocketAddr, io::Error),
    /// The page closed its websocket.
    PageClosed,
    /// The websocket failed.
    PageFailed(WsError),
    /// The page sent a text frame.
    Text,
    /// scsynth ended the TCP stream.
    ScsynthClosed,
    /// The TCP connection to scsynth failed.
    ScsynthFailed(io::Error),
    /// scsynth sent a packet length outside 0 to [`MAX_PACKET`].
    BadLength(i32),
}

impl End {
    /// The code of the close frame that the bridge sends to the page, if it
    /// sends one.
    fn close_code(&self) -> Option<CloseCode> {
        match self {
            Self::PageClosed => None,
            Self::PageFailed(WsError::Capacity(_)) => Some(CloseCode::Size),
            Self::PageFailed(_) => Some(CloseCode::Protocol),
            Self::Text => Some(CloseCode::Unsupported),
            Self::ScsynthClosed => Some(CloseCode::Away),
            Self::Unreachable(..) | Self::ScsynthFailed(_) | Self::BadLength(_) => {
                Some(CloseCode::Error)
            }
        }
    }
}

impl fmt::Display for End {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreachable(addr, error) => {
                write!(f, "cannot connect to scsynth at {addr}: {error}")
            }
            Self::PageClosed => f.write_str("the page closed the websocket"),
            Self::PageFailed(error) => write!(f, "the websocket failed: {error}"),
            Self::Text => f.write_str("a text frame is no OSC packet"),
            Self::ScsynthClosed => f.write_str("scsynth closed the connection"),
            Self::ScsynthFailed(error) => write!(f, "the connection to scsynth failed: {error}"),
            Self::BadLength(length) => write!(
                f,
                "scsynth sent a packet length of {length} bytes, outside 0 to {MAX_PACKET}"
            ),
        }
    }
}

/// Pipes the websocket and the TCP connection until one of them ends.
async fn pipe(ws: &mut WebSocket, tcp: &mut TcpStream) -> End {
    let mut pending = Vec::new();
    loop {
        pending.reserve(READ_SIZE);
        let step = tokio::select! {
            message = ws.next() => from_page(message, tcp).await,
            read = tcp.read_buf(&mut pending) => match read {
                Ok(0) => Err(End::ScsynthClosed),
                Ok(_) => to_page(ws, &mut pending).await,
                Err(error) => Err(End::ScsynthFailed(error)),
            },
        };
        if let Err(end) = step {
            break end;
        }
    }
}

/// Hands one event of the websocket on to scsynth.
async fn from_page(
    message: Option<Result<Message, WsError>>,
    tcp: &mut TcpStream,
) -> Result<(), End> {
    match message {
        Some(Ok(Message::Binary(packet))) => {
            write_packet(tcp, &packet).await.map_err(End::ScsynthFailed)
        }
        Some(Ok(Message::Text(_))) => Err(End::Text),
        Some(Ok(Message::Close(_))) | None => Err(End::PageClosed),
        Some(Ok(_)) => Ok(()),
        Some(Err(error)) => Err(End::PageFailed(error)),
    }
}

/// Sends each whole packet of `pending` to the page, one binary frame each.
async fn to_page(ws: &mut WebSocket, pending: &mut Vec<u8>) -> Result<(), End> {
    let outcome = loop {
        match next_packet(pending) {
            Ok(Some(packet)) => {
                let frame = Message::binary(packet);
                ws.feed(frame).await.map_err(End::PageFailed)?;
            }
            Ok(None) => break Ok(()),
            Err(length) => break Err(End::BadLength(length)),
        }
    };
    ws.flush().await.map_err(End::PageFailed)?;
    outcome
}

/// Takes the first packet of `pending`, from scsynth: `None` until all its
/// bytes arrived, and `Err` with its length when that is outside 0 to
/// [`MAX_PACKET`].
fn next_packet(pending: &mut Vec<u8>) -> Result<Option<Vec<u8>>, i32> {
    let Some(&[a, b, c, d]) = pending.get(..4) else {
        return Ok(None);
    };
    let length = i32::from_be_bytes([a, b, c, d]);
    let size = usize::try_from(length)
        .ok()
        .filter(|size| *size <= MAX_PACKET)
        .ok_or(length)?;
    let Some(packet) = pending.get(4..4 + size) else {
        return Ok(None);
    };
    let packet = packet.to_vec();
    pending.drain(..4 + size);
    Ok(Some(packet))
}

/// Writes `packet` to scsynth after its length, 32 bits in network byte
/// order, in one write.
async fn write_packet(tcp: &mut TcpStream, packet: &[u8]) -> io::Result<()> {
    let length = i32::try_from(packet.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "a packet over i32::MAX bytes"))?;
    let mut frame = Vec::with_capacity(4 + packet.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(packet);
    tcp.write_all(&frame).await
}

/// Writes `/notify 0` to scsynth, ends the stream to it, and reads what
/// comes until scsynth closes its end.
async fn unregister(mut tcp: TcpStream) {
    let notify = Notify {
        on: false,
        client_id: None,
    };
    let packet = Packet::Message(Command::Notify(notify)).encode();
    if write_packet(&mut tcp, &packet).await.is_err() || tcp.shutdown().await.is_err() {
        return;
    }
    // A socket that closes with bytes unread sends a reset (RFC 1122,
    // 4.2.2.13), and the reset can discard the `/notify 0` before scsynth
    // reads it.
    let mut buffer = vec![0; READ_SIZE];
    while let Ok(1..) = tcp.read(&mut buffer).await {}
}

/// Sends a close frame with `code` and `reason`, if `code` is some, reads
/// the websocket until the page answers the close, and drops it.
async fn close(mut ws: WebSocket, code: Option<CloseCode>, reason: &str) {
    let _ = time::timeout(CLOSE_LIMIT, async {
        if let Some(code) = code {
            let frame = CloseFrame {
                code,
                reason: truncate(reason).into(),
            };
            let _ = ws.close(Some(frame)).await;
        }
        while let Some(Ok(_)) = ws.next().await {}
    })
    .await;
}

/// The first [`MAX_REASON`] bytes of `reason` at most, cut at a character.
fn truncate(reason: &str) -> &str {
    let mut end = reason.len().min(MAX_REASON);
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    &reason[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn framed(packet: &[u8]) -> Vec<u8> {
        let mut bytes = u32::try_from(packet.len()).unwrap().to_be_bytes().to_vec();
        bytes.extend_from_slice(packet);
        bytes
    }

    #[test]
    fn a_packet_comes_once_all_its_bytes_arrived() {
        let bytes = framed(b"/status\0,\0\0\0");
        let mut pending = Vec::new();
        for (index, byte) in bytes.iter().enumerate() {
            assert_eq!(next_packet(&mut pending), Ok(None), "after {index} bytes");
            pending.push(*byte);
        }
        assert_eq!(
            next_packet(&mut pending),
            Ok(Some(b"/status\0,\0\0\0".to_vec()))
        );
        assert_eq!(pending, Vec::<u8>::new());
    }

    #[test]
    fn packets_come_one_at_a_time_in_order() {
        let mut pending = framed(b"one\0");
        pending.extend(framed(b"two\0"));
        pending.extend(&[0, 0]);
        assert_eq!(next_packet(&mut pending), Ok(Some(b"one\0".to_vec())));
        assert_eq!(next_packet(&mut pending), Ok(Some(b"two\0".to_vec())));
        assert_eq!(next_packet(&mut pending), Ok(None));
        assert_eq!(pending, [0, 0]);
    }

    #[test]
    fn an_empty_packet_is_a_packet() {
        let mut pending = framed(b"");
        assert_eq!(next_packet(&mut pending), Ok(Some(Vec::new())));
    }

    #[test]
    fn a_length_outside_the_limit_is_an_error() {
        let over = i32::try_from(MAX_PACKET + 1).unwrap();
        for length in [-1, i32::MIN, over] {
            let mut pending = length.to_be_bytes().to_vec();
            assert_eq!(next_packet(&mut pending), Err(length));
        }
        let mut pending = i32::try_from(MAX_PACKET).unwrap().to_be_bytes().to_vec();
        assert_eq!(next_packet(&mut pending), Ok(None));
    }

    #[test]
    fn a_reason_fits_a_close_frame() {
        let reason = "é".repeat(100);
        let cut = truncate(&reason);
        assert_eq!(cut.len(), 122);
        assert!(reason.starts_with(cut));
        assert_eq!(truncate("short"), "short");
    }
}
