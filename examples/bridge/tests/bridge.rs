//! The rules of the bridge, with a TCP listener in place of scsynth and a
//! websocket client of tokio-tungstenite in place of a page.

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use scsynth_client::scsynth_osc::command::top_level::Notify;
use scsynth_client::scsynth_osc::{Command, Packet};
use scsynth_client_bridge::{Config, connect, serve};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::ORIGIN;
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{WebSocketStream, client_async};

/// The longest that a test waits for anything.
const LIMIT: Duration = Duration::from_secs(5);

/// The origins that the bridges of the tests allow.
const ORIGINS: [&str; 2] = ["http://localhost:5173", "http://127.0.0.1:5173"];

/// The packet of the page in the tests: `/status`.
const STATUS: &[u8] = b"/status\0,\0\0\0";

/// The packet of scsynth in the tests: `/synced 1`.
const SYNCED: &[u8] = b"/synced\0,i\0\0\0\0\0\x01";

type WebSocket = WebSocketStream<TcpStream>;

async fn within<T>(future: impl Future<Output = T>) -> T {
    timeout(LIMIT, future)
        .await
        .expect("the wait passed its limit")
}

/// A bridge, and the listener that plays scsynth for it.
struct Bridge {
    addr: SocketAddr,
    scsynth: TcpListener,
}

impl Bridge {
    async fn start() -> Self {
        let scsynth = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = start_bridge(scsynth.local_addr().unwrap()).await;
        Self { addr, scsynth }
    }

    /// A websocket of a page of the first origin, and the TCP connection
    /// that the bridge opened for it.
    async fn pair(&self) -> (WebSocket, TcpStream) {
        let ws = open(self.addr, Some(ORIGINS[0])).await.unwrap();
        let tcp = self.accept().await;
        (ws, tcp)
    }

    /// The next TCP connection that the bridge opened.
    async fn accept(&self) -> TcpStream {
        within(self.scsynth.accept()).await.unwrap().0
    }
}

/// A bridge to `scsynth` on a port of its own, which allows [`ORIGINS`].
async fn start_bridge(scsynth: SocketAddr) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = Config {
        listen: addr,
        scsynth,
        origins: ORIGINS.map(str::to_owned).to_vec(),
    };
    tokio::spawn(serve(listener, config));
    addr
}

/// A websocket to the bridge at `addr`, with `origin` as its `Origin`
/// header, if some.
async fn open(addr: SocketAddr, origin: Option<&str>) -> Result<WebSocket, WsError> {
    let mut request = format!("ws://{addr}").into_client_request()?;
    if let Some(origin) = origin {
        let value = HeaderValue::from_str(origin).unwrap();
        request.headers_mut().insert(ORIGIN, value);
    }
    let stream = within(TcpStream::connect(addr)).await?;
    let (ws, _) = within(client_async(request, stream)).await?;
    Ok(ws)
}

/// The bytes of `packet` on the TCP connection: its length, then itself.
fn framed(packet: &[u8]) -> Vec<u8> {
    let mut bytes = u32::try_from(packet.len()).unwrap().to_be_bytes().to_vec();
    bytes.extend_from_slice(packet);
    bytes
}

/// The next packet that the bridge wrote on `tcp`, after its length.
async fn read_packet(tcp: &mut TcpStream) -> Vec<u8> {
    let mut length = [0; 4];
    within(tcp.read_exact(&mut length)).await.unwrap();
    let mut packet = vec![0; usize::try_from(i32::from_be_bytes(length)).unwrap()];
    within(tcp.read_exact(&mut packet)).await.unwrap();
    packet
}

/// The next message of the bridge on `ws`.
async fn next(ws: &mut WebSocket) -> Message {
    within(ws.next())
        .await
        .expect("the websocket ended")
        .unwrap()
}

/// Asserts that the bridge wrote `/notify 0` on `tcp`, and then ended the
/// stream.
async fn assert_unregistered(tcp: &mut TcpStream) {
    let packet = read_packet(tcp).await;
    let notify = Notify {
        on: false,
        client_id: None,
    };
    let expected = Packet::Message(Command::Notify(notify));
    assert_eq!(Packet::<Command>::decode(&packet).unwrap(), expected);
    let mut rest = Vec::new();
    within(tcp.read_to_end(&mut rest)).await.unwrap();
    assert_eq!(rest, Vec::<u8>::new(), "bytes after /notify 0");
}

/// Asserts that the bridge closed `ws` with `code`, and gives the reason.
async fn assert_closed(ws: &mut WebSocket, code: CloseCode) -> String {
    let message = next(ws).await;
    let Message::Close(Some(frame)) = message else {
        panic!("not a close frame: {message:?}");
    };
    assert_eq!(frame.code, code);
    frame.reason.to_string()
}

#[tokio::test]
async fn a_packet_passes_each_way() {
    let bridge = Bridge::start().await;
    let (mut ws, mut tcp) = bridge.pair().await;

    within(ws.send(Message::binary(STATUS))).await.unwrap();
    let mut bytes = vec![0; 4 + STATUS.len()];
    within(tcp.read_exact(&mut bytes)).await.unwrap();
    assert_eq!(bytes, framed(STATUS));

    within(tcp.write_all(&framed(SYNCED))).await.unwrap();
    assert_eq!(next(&mut ws).await, Message::binary(SYNCED));
}

#[tokio::test]
async fn each_websocket_gets_its_own_tcp_connection_at_its_accept() {
    let bridge = Bridge::start().await;
    let (mut first, mut first_tcp) = bridge.pair().await;
    let (mut second, mut second_tcp) = bridge.pair().await;

    within(second.send(Message::binary(SYNCED))).await.unwrap();
    within(first.send(Message::binary(STATUS))).await.unwrap();
    assert_eq!(read_packet(&mut first_tcp).await, STATUS);
    assert_eq!(read_packet(&mut second_tcp).await, SYNCED);

    within(second_tcp.write_all(&framed(STATUS))).await.unwrap();
    within(first_tcp.write_all(&framed(SYNCED))).await.unwrap();
    assert_eq!(next(&mut first).await, Message::binary(SYNCED));
    assert_eq!(next(&mut second).await, Message::binary(STATUS));
}

#[tokio::test]
async fn the_connection_to_scsynth_has_no_delay() {
    let scsynth = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp = within(connect(scsynth.local_addr().unwrap()))
        .await
        .unwrap();
    assert!(tcp.nodelay().unwrap());
}

#[tokio::test]
async fn two_packets_in_one_write_give_two_frames_in_order() {
    let bridge = Bridge::start().await;
    let (mut ws, mut tcp) = bridge.pair().await;

    let mut bytes = framed(SYNCED);
    bytes.extend(framed(STATUS));
    within(tcp.write_all(&bytes)).await.unwrap();
    assert_eq!(next(&mut ws).await, Message::binary(SYNCED));
    assert_eq!(next(&mut ws).await, Message::binary(STATUS));
}

#[tokio::test]
async fn a_packet_in_pieces_gives_one_frame() {
    let bridge = Bridge::start().await;
    let (mut ws, mut tcp) = bridge.pair().await;
    tcp.set_nodelay(true).unwrap();

    let bytes = framed(SYNCED);
    for piece in [&bytes[..2], &bytes[2..7], &bytes[7..]] {
        within(tcp.write_all(piece)).await.unwrap();
        sleep(Duration::from_millis(20)).await;
    }
    within(tcp.write_all(&framed(STATUS))).await.unwrap();
    assert_eq!(next(&mut ws).await, Message::binary(SYNCED));
    assert_eq!(next(&mut ws).await, Message::binary(STATUS));
}

#[tokio::test]
async fn each_allowed_origin_opens_a_websocket() {
    let bridge = Bridge::start().await;
    for origin in ORIGINS {
        let mut ws = open(bridge.addr, Some(origin)).await.unwrap();
        let mut tcp = bridge.accept().await;
        within(ws.send(Message::binary(STATUS))).await.unwrap();
        assert_eq!(read_packet(&mut tcp).await, STATUS);
    }
}

#[tokio::test]
async fn another_origin_gets_403_and_no_tcp_connection() {
    let bridge = Bridge::start().await;
    for origin in ["http://evil.example", "http://localhost:5174", "null"] {
        let Err(WsError::Http(response)) = open(bridge.addr, Some(origin)).await else {
            panic!("the origin {origin} opened a websocket");
        };
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    // A refused handshake opened no connection, so the first one that the
    // listener accepts is the one of this websocket.
    let (mut ws, mut tcp) = bridge.pair().await;
    within(ws.send(Message::binary(STATUS))).await.unwrap();
    assert_eq!(read_packet(&mut tcp).await, STATUS);
}

#[tokio::test]
async fn a_handshake_without_an_origin_opens_a_websocket() {
    let bridge = Bridge::start().await;
    let mut ws = open(bridge.addr, None).await.unwrap();
    let mut tcp = bridge.accept().await;
    within(ws.send(Message::binary(STATUS))).await.unwrap();
    assert_eq!(read_packet(&mut tcp).await, STATUS);
}

#[tokio::test]
async fn the_close_of_the_websocket_sends_notify_off_then_ends_the_stream() {
    let bridge = Bridge::start().await;
    let (mut ws, mut tcp) = bridge.pair().await;

    within(ws.close(None)).await.unwrap();
    assert_unregistered(&mut tcp).await;
    assert_eq!(next(&mut ws).await, Message::Close(None));
    assert!(within(ws.next()).await.is_none());
}

#[tokio::test]
async fn a_failed_websocket_sends_notify_off_then_ends_the_stream() {
    let bridge = Bridge::start().await;
    let (ws, mut tcp) = bridge.pair().await;

    drop(ws);
    assert_unregistered(&mut tcp).await;
}

#[tokio::test]
async fn the_end_of_the_stream_from_scsynth_closes_the_websocket() {
    let bridge = Bridge::start().await;
    let (mut ws, tcp) = bridge.pair().await;

    drop(tcp);
    let reason = assert_closed(&mut ws, CloseCode::Away).await;
    assert_eq!(reason, "scsynth closed the connection");
    assert!(within(ws.next()).await.is_none());
}

#[tokio::test]
async fn an_unreachable_scsynth_closes_the_websocket() {
    let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let scsynth = unused.local_addr().unwrap();
    drop(unused);
    let addr = start_bridge(scsynth).await;

    let mut ws = open(addr, Some(ORIGINS[0])).await.unwrap();
    let reason = assert_closed(&mut ws, CloseCode::Error).await;
    assert!(
        reason.starts_with(&format!("cannot connect to scsynth at {scsynth}: ")),
        "{reason}"
    );
}

#[tokio::test]
async fn a_text_frame_closes_the_websocket() {
    let bridge = Bridge::start().await;
    let (mut ws, mut tcp) = bridge.pair().await;

    within(ws.send(Message::text("/status"))).await.unwrap();
    let reason = assert_closed(&mut ws, CloseCode::Unsupported).await;
    assert_eq!(reason, "a text frame is no OSC packet");
    assert_unregistered(&mut tcp).await;
}

#[tokio::test]
async fn a_length_out_of_bounds_closes_the_pair() {
    let bridge = Bridge::start().await;
    for length in [-1, i32::MIN, (64 << 20) + 1] {
        let (mut ws, mut tcp) = bridge.pair().await;

        let mut bytes = framed(SYNCED);
        bytes.extend(length.to_be_bytes());
        bytes.extend(STATUS);
        within(tcp.write_all(&bytes)).await.unwrap();
        assert_eq!(next(&mut ws).await, Message::binary(SYNCED));
        let reason = assert_closed(&mut ws, CloseCode::Error).await;
        assert_eq!(
            reason,
            format!("scsynth sent a packet length of {length} bytes, outside 0 to 67108864")
        );
        assert_unregistered(&mut tcp).await;
    }
}
