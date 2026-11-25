//! `TcpClient` against a TCP listener of tokio on the loopback address that
//! plays scsynth: a length of 32 bits in network byte order before each
//! packet each way, a packet in pieces, two packets in one write, a large
//! packet, a bad length, the end of the stream, the close, the drop, and
//! `Scsynth::new`.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use scsynth_client::scsynth_osc::command::synth_def::DRecv;
use scsynth_client::scsynth_osc::{Command, Packet, RawArg, RawMessage, Reply, SyncId};
use scsynth_client::{
    ClientError, Event, Incoming, Options, OscClient, Scsynth, SendError, TcpClient,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::time::{sleep, timeout};

/// The longest that a test waits for the peer or for an event.
const LIMIT: Duration = Duration::from_secs(5);

/// A client, and the stream of the peer that accepted it.
async fn connect() -> (TcpClient, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the peer binds");
    let addr = listener.local_addr().expect("the peer has an address");
    let client = timeout(LIMIT, TcpClient::connect(addr))
        .await
        .expect("a connect within the limit")
        .expect("the client connects");
    let (peer, _) = timeout(LIMIT, listener.accept())
        .await
        .expect("an accept within the limit")
        .expect("the peer accepts");
    peer.set_nodelay(true).expect("the peer sets TCP_NODELAY");
    (client, peer)
}

/// The events that the listener of `client` gets from now on. The receiver
/// ends when the client drops its listener.
fn listen(client: &TcpClient) -> UnboundedReceiver<Event> {
    let (sender, receiver) = mpsc::unbounded_channel();
    client.listen(Box::new(move |event| {
        let _ = sender.send(event);
    }));
    receiver
}

/// The next event, or `None` once the client dropped its listener.
async fn next(events: &mut UnboundedReceiver<Event>) -> Option<Event> {
    timeout(LIMIT, events.recv())
        .await
        .expect("an event within the limit")
}

/// The bytes of a packet after their length.
fn frame(packet: &[u8]) -> Vec<u8> {
    let length = u32::try_from(packet.len()).expect("a packet under 4 GiB");
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(packet);
    frame
}

async fn read_exact(peer: &mut TcpStream, buffer: &mut [u8]) {
    timeout(LIMIT, peer.read_exact(buffer))
        .await
        .expect("bytes within the limit")
        .expect("the peer reads");
}

/// The next packet that the peer reads, after its length.
async fn read_packet(peer: &mut TcpStream) -> Vec<u8> {
    let mut length = [0; 4];
    read_exact(peer, &mut length).await;
    let mut packet = vec![0; usize::try_from(u32::from_be_bytes(length)).expect("a length")];
    read_exact(peer, &mut packet).await;
    packet
}

async fn write(peer: &mut TcpStream, bytes: &[u8]) {
    timeout(LIMIT, peer.write_all(bytes))
        .await
        .expect("a write within the limit")
        .expect("the peer writes");
}

/// Reads the end of the stream on the peer.
async fn assert_end(peer: &mut TcpStream) {
    let mut byte = [0; 1];
    let read = timeout(LIMIT, peer.read(&mut byte))
        .await
        .expect("the end within the limit")
        .expect("the peer reads");
    assert_eq!(read, 0, "the end of the stream");
}

fn synced(id: i32) -> Packet<Reply> {
    Packet::Message(Reply::Synced(SyncId(id)))
}

fn message(id: i32) -> Event {
    Event::Message(Incoming::Reply(Reply::Synced(SyncId(id))))
}

fn assert_send_sync<T: Send + Sync>() {}

fn assert_client<T: OscClient>() {}

#[test]
fn the_client_is_send_and_sync_and_an_osc_client() {
    assert_send_sync::<TcpClient>();
    assert_client::<TcpClient>();
}

#[tokio::test]
async fn the_peer_reads_the_length_and_the_packet_of_each_send() {
    let (client, mut peer) = connect().await;
    let packets = [
        Packet::Message(Command::Status),
        Packet::Message(Command::Sync(SyncId(1))),
    ];
    for packet in &packets {
        client.send(packet).expect("open");
    }
    for packet in &packets {
        let bytes = packet.encode();
        let mut length = [0; 4];
        read_exact(&mut peer, &mut length).await;
        assert_eq!(
            length,
            u32::try_from(bytes.len()).expect("short").to_be_bytes()
        );
        let mut body = vec![0; bytes.len()];
        read_exact(&mut peer, &mut body).await;
        assert_eq!(body, bytes);
    }
}

#[tokio::test]
async fn a_reply_in_pieces_that_split_the_length_and_the_body_gives_one_event() {
    let (client, mut peer) = connect().await;
    let mut events = listen(&client);
    let bytes = frame(&synced(1).encode());
    for piece in [&bytes[..2], &bytes[2..9], &bytes[9..]] {
        write(&mut peer, piece).await;
        // The reader task reads each piece on its own.
        sleep(Duration::from_millis(20)).await;
    }
    write(&mut peer, &frame(&synced(2).encode())).await;
    assert_eq!(next(&mut events).await, Some(message(1)));
    assert_eq!(next(&mut events).await, Some(message(2)));
}

#[tokio::test]
async fn two_replies_in_one_write_give_two_events_in_order() {
    let (client, mut peer) = connect().await;
    let mut events = listen(&client);
    let bytes = [frame(&synced(1).encode()), frame(&synced(2).encode())].concat();
    write(&mut peer, &bytes).await;
    assert_eq!(next(&mut events).await, Some(message(1)));
    assert_eq!(next(&mut events).await, Some(message(2)));
}

#[tokio::test]
async fn a_packet_of_60016_bytes_passes_each_way() {
    let (client, mut peer) = connect().await;
    let mut events = listen(&client);
    let sent = Packet::Message(Command::DRecv(DRecv {
        data: vec![7; 60_000],
        completion: None,
    }));
    assert_eq!(sent.encode().len(), 60_016);
    client.send(&sent).expect("open");
    assert_eq!(read_packet(&mut peer).await, sent.encode());
    let raw = RawMessage {
        address: "/blob".to_owned(),
        args: vec![RawArg::Blob(vec![7; 60_000])],
    };
    assert_eq!(raw.encode().len(), 60_016);
    write(&mut peer, &frame(&raw.encode())).await;
    assert_eq!(
        next(&mut events).await,
        Some(Event::Message(Incoming::Other(raw)))
    );
}

#[tokio::test]
async fn the_end_of_the_stream_gives_closed_once_and_then_send_refuses() {
    let (client, peer) = connect().await;
    let mut events = listen(&client);
    drop(peer);
    assert_eq!(next(&mut events).await, Some(Event::Closed));
    assert_eq!(next(&mut events).await, None);
    assert_eq!(
        client.send(&Packet::Message(Command::Status)),
        Err(SendError::Closed)
    );
}

#[tokio::test]
async fn close_ends_the_stream_of_the_peer_and_gives_closed_once() {
    let (client, mut peer) = connect().await;
    let mut events = listen(&client);
    client.close();
    client.close();
    assert_end(&mut peer).await;
    assert_eq!(next(&mut events).await, Some(Event::Closed));
    assert_eq!(next(&mut events).await, None);
    assert_eq!(
        client.send(&Packet::Message(Command::Status)),
        Err(SendError::Closed)
    );
}

#[tokio::test]
async fn a_packet_sent_before_close_still_reaches_the_peer() {
    let (client, mut peer) = connect().await;
    let status = Packet::Message(Command::Status);
    client.send(&status).expect("open");
    client.close();
    assert_eq!(read_packet(&mut peer).await, status.encode());
    assert_end(&mut peer).await;
}

#[tokio::test]
async fn a_dropped_client_ends_the_stream_of_the_peer() {
    let (client, mut peer) = connect().await;
    drop(client);
    assert_end(&mut peer).await;
}

#[tokio::test]
async fn a_client_dropped_inside_its_listener_ends_the_stream_of_the_peer() {
    let (client, mut peer) = connect().await;
    let slot = Arc::new(Mutex::new(None::<TcpClient>));
    let held = Arc::clone(&slot);
    let (sender, mut events) = mpsc::unbounded_channel();
    client.listen(Box::new(move |event| {
        let client = held.lock().unwrap_or_else(PoisonError::into_inner).take();
        drop(client);
        let _ = sender.send(event);
    }));
    *slot.lock().unwrap_or_else(PoisonError::into_inner) = Some(client);
    write(&mut peer, &frame(&synced(1).encode())).await;
    assert_eq!(next(&mut events).await, Some(message(1)));
    assert_eq!(next(&mut events).await, Some(Event::Closed));
    assert_eq!(next(&mut events).await, None);
    assert_end(&mut peer).await;
}

#[tokio::test]
async fn a_bad_length_gives_an_error_then_closed() {
    let negative = i32::MIN.to_be_bytes();
    let over = ((64_i32 << 20) + 1).to_be_bytes();
    for length in [negative, over] {
        let (client, mut peer) = connect().await;
        let mut events = listen(&client);
        write(&mut peer, &length).await;
        assert!(
            matches!(
                next(&mut events).await,
                Some(Event::Error(ClientError::Transport(_)))
            ),
            "an error of the transport"
        );
        assert_eq!(next(&mut events).await, Some(Event::Closed));
        assert_eq!(next(&mut events).await, None);
        assert_end(&mut peer).await;
    }
}

#[tokio::test]
async fn scsynth_new_syncs_with_the_peer() {
    let (client, mut peer) = connect().await;
    let scsynth = Scsynth::new(client, Options::default());
    let served = tokio::spawn(async move {
        let bytes = read_packet(&mut peer).await;
        let packet = Packet::<Command>::decode(&bytes).expect("a packet of commands");
        for command in packet.into_messages() {
            if let Command::Sync(id) = command {
                let reply = Packet::Message(Reply::Synced(id));
                write(&mut peer, &frame(&reply.encode())).await;
            }
        }
        peer
    });
    let outcome = timeout(LIMIT, scsynth.sync())
        .await
        .expect("the sync ends within the limit");
    assert_eq!(outcome, Ok(()));
    served.await.expect("the peer answers");
}
