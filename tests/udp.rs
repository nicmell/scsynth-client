//! `UdpClient` against a UDP socket of tokio on the loopback address that
//! plays scsynth: one datagram for each packet each way, the limit of a
//! datagram, the errors that the client survives, the events before
//! `listen`, the close, and `Scsynth::new` with the timer of tokio.

use std::net::SocketAddr;
use std::time::Duration;

use scsynth_client::scsynth_osc::command::synth_def::DRecv;
use scsynth_client::scsynth_osc::reply::command_replies::Done;
use scsynth_client::scsynth_osc::{Bundle, Command, Packet, Reply, SyncId, Timetag};
use scsynth_client::{
    ClientError, DEFAULT_MAX_DATAGRAM, Error, Event, Incoming, Options, OscClient, Scsynth,
    SendError, UdpClient,
};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::time::{sleep, timeout};

/// The longest that a test waits for the peer or for an event.
const LIMIT: Duration = Duration::from_secs(5);

/// A peer on the loopback address, and a client of it.
async fn connect() -> (UdpClient, UdpSocket) {
    let peer = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("the peer binds");
    let addr = peer.local_addr().expect("the peer has an address");
    let client = UdpClient::connect(addr).await.expect("the client connects");
    (client, peer)
}

/// The events that the listener of `client` gets from now on. The receiver
/// ends when the client drops its listener.
fn listen(client: &UdpClient) -> UnboundedReceiver<Event> {
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

/// The next datagram of the peer, and its sender.
async fn receive(peer: &UdpSocket) -> (Vec<u8>, SocketAddr) {
    let mut buffer = vec![0; 65_535];
    let (length, from) = timeout(LIMIT, peer.recv_from(&mut buffer))
        .await
        .expect("a datagram within the limit")
        .expect("the peer receives");
    buffer.truncate(length);
    (buffer, from)
}

/// Sends `bytes` from the peer to `to`.
async fn answer(peer: &UdpSocket, bytes: &[u8], to: SocketAddr) {
    peer.send_to(bytes, to).await.expect("the peer sends");
}

/// The address of the client, from a `/status` that the peer receives.
async fn address(client: &UdpClient, peer: &UdpSocket) -> SocketAddr {
    client
        .send(&Packet::Message(Command::Status))
        .expect("open");
    receive(peer).await.1
}

fn synced(id: i32) -> Reply {
    Reply::Synced(SyncId(id))
}

fn message(reply: Reply) -> Event {
    Event::Message(Incoming::Reply(reply))
}

/// A `/d_recv` whose blob has `size` bytes.
fn d_recv(size: usize) -> Packet<Command> {
    Packet::Message(Command::DRecv(DRecv {
        data: vec![7; size],
        completion: None,
    }))
}

fn assert_send_sync<T: Send + Sync>() {}

fn assert_client<T: OscClient>() {}

#[test]
fn the_client_is_send_and_sync_and_an_osc_client() {
    assert_send_sync::<UdpClient>();
    assert_client::<UdpClient>();
}

#[tokio::test]
async fn the_peer_receives_the_exact_bytes_of_each_packet() {
    let (client, peer) = connect().await;
    let request = Packet::Bundle(Bundle {
        time: Timetag::IMMEDIATELY,
        packets: vec![
            Packet::Message(Command::Status),
            Packet::Message(Command::Sync(SyncId(1))),
        ],
    });
    client.send(&request).expect("open");
    client
        .send(&Packet::Message(Command::Version))
        .expect("open");
    assert_eq!(receive(&peer).await.0, request.encode());
    assert_eq!(
        receive(&peer).await.0,
        Packet::Message(Command::Version).encode()
    );
}

#[tokio::test]
async fn the_replies_of_the_peer_come_as_typed_events_in_order() {
    let (client, peer) = connect().await;
    let mut events = listen(&client);
    let to = address(&client, &peer).await;
    let replies = [synced(1), Reply::Done(Done::Quit), synced(2)];
    for reply in &replies {
        answer(&peer, &Packet::Message(reply.clone()).encode(), to).await;
    }
    for reply in replies {
        assert_eq!(next(&mut events).await, Some(message(reply)));
    }
}

#[tokio::test]
async fn a_bundle_of_two_replies_gives_two_events_in_order() {
    let (client, peer) = connect().await;
    let mut events = listen(&client);
    let to = address(&client, &peer).await;
    let bundle = Packet::Bundle(Bundle {
        time: Timetag::IMMEDIATELY,
        packets: vec![Packet::Message(synced(1)), Packet::Message(synced(2))],
    });
    answer(&peer, &bundle.encode(), to).await;
    assert_eq!(next(&mut events).await, Some(message(synced(1))));
    assert_eq!(next(&mut events).await, Some(message(synced(2))));
}

#[tokio::test]
async fn a_packet_over_the_limit_is_refused_before_it_leaves_and_one_at_the_limit_passes() {
    let (client, peer) = connect().await;
    let over = d_recv(DEFAULT_MAX_DATAGRAM - 12);
    let at = d_recv(DEFAULT_MAX_DATAGRAM - 16);
    assert_eq!(over.encode().len(), 8196);
    assert_eq!(at.encode().len(), 8192);
    assert_eq!(
        client.send(&over),
        Err(SendError::TooLarge {
            size: 8196,
            max: 8192
        })
    );
    client.send(&at).expect("open");
    assert_eq!(receive(&peer).await.0, at.encode());
}

#[tokio::test]
async fn the_largest_datagram_is_an_option() {
    let peer = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("the peer binds");
    let addr = peer.local_addr().expect("the peer has an address");
    let client = UdpClient::connect_with_max_datagram(addr, 100)
        .await
        .expect("the client connects");
    assert_eq!(
        client.send(&d_recv(88)),
        Err(SendError::TooLarge {
            size: 104,
            max: 100
        })
    );
    client.send(&d_recv(84)).expect("open");
    assert_eq!(receive(&peer).await.0, d_recv(84).encode());
}

#[tokio::test]
async fn bytes_that_are_no_osc_give_an_error_and_the_client_goes_on() {
    let (client, peer) = connect().await;
    let mut events = listen(&client);
    let to = address(&client, &peer).await;
    answer(&peer, &[1, 2, 3], to).await;
    answer(&peer, &Packet::Message(synced(1)).encode(), to).await;
    assert!(
        matches!(
            next(&mut events).await,
            Some(Event::Error(ClientError::Decode(_)))
        ),
        "a decode error"
    );
    assert_eq!(next(&mut events).await, Some(message(synced(1))));
}

#[tokio::test]
async fn events_before_listen_are_kept_and_come_at_listen_in_order() {
    let (client, peer) = connect().await;
    let to = address(&client, &peer).await;
    for id in 1..=3 {
        answer(&peer, &Packet::Message(synced(id)).encode(), to).await;
    }
    // The reader task receives the datagrams while no listener is set.
    sleep(Duration::from_millis(50)).await;
    let mut events = listen(&client);
    for id in 1..=3 {
        assert_eq!(next(&mut events).await, Some(message(synced(id))));
    }
}

#[tokio::test]
async fn close_gives_closed_once_and_then_send_refuses() {
    let (client, _peer) = connect().await;
    let mut events = listen(&client);
    client.close();
    client.close();
    assert_eq!(next(&mut events).await, Some(Event::Closed));
    assert_eq!(next(&mut events).await, None);
    assert_eq!(
        client.send(&Packet::Message(Command::Status)),
        Err(SendError::Closed)
    );
}

/// A port on the loopback address that no socket holds.
async fn closed_port() -> SocketAddr {
    let gone = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("a socket binds");
    gone.local_addr().expect("the socket has an address")
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn macos_reports_a_send_to_a_closed_port_as_an_error() {
    let addr = closed_port().await;
    let client = UdpClient::connect(addr).await.expect("the client connects");
    let mut events = listen(&client);
    client
        .send(&Packet::Message(Command::Status))
        .expect("open");
    assert!(
        matches!(
            next(&mut events).await,
            Some(Event::Error(ClientError::Transport(_)))
        ),
        "an error of the transport"
    );
}

#[tokio::test]
async fn the_client_goes_on_after_a_send_to_a_closed_port_once_a_peer_binds_it() {
    let addr = closed_port().await;
    let client = UdpClient::connect(addr).await.expect("the client connects");
    let mut events = listen(&client);
    client
        .send(&Packet::Message(Command::Status))
        .expect("open");
    let peer = UdpSocket::bind(addr)
        .await
        .expect("the peer binds the port");
    // On the Linux runner of CI, no error came within 5 s of the first send:
    // the test assumes neither an error nor its time, so it sends `/status`
    // until one reaches the peer.
    let mut buffer = vec![0; 65_535];
    let mut to = None;
    for _ in 0..50 {
        client
            .send(&Packet::Message(Command::Status))
            .expect("open");
        let received = timeout(Duration::from_millis(100), peer.recv_from(&mut buffer)).await;
        if let Ok(received) = received {
            to = Some(received.expect("the peer receives").1);
            break;
        }
    }
    let to = to.expect("a /status reaches the peer within 5 s");
    answer(&peer, &Packet::Message(synced(1)).encode(), to).await;
    loop {
        match next(&mut events).await {
            Some(Event::Error(ClientError::Transport(_))) => {}
            event => {
                assert_eq!(event, Some(message(synced(1))));
                break;
            }
        }
    }
}

#[tokio::test]
async fn scsynth_new_syncs_with_the_peer() {
    let (client, peer) = connect().await;
    let scsynth = Scsynth::new(client, Options::default());
    let served = tokio::spawn(async move {
        let (bytes, from) = receive(&peer).await;
        let packet = Packet::<Command>::decode(&bytes).expect("a packet of commands");
        for command in packet.into_messages() {
            if let Command::Sync(id) = command {
                answer(&peer, &Packet::Message(Reply::Synced(id)).encode(), from).await;
            }
        }
    });
    let outcome = timeout(LIMIT, scsynth.sync())
        .await
        .expect("the sync ends within the limit");
    assert_eq!(outcome, Ok(()));
    served.await.expect("the peer answers");
}

#[tokio::test]
async fn scsynth_new_times_out_when_the_peer_stays_silent() {
    let (client, _peer) = connect().await;
    let options = Options {
        timeout: Duration::from_millis(100),
        ..Options::default()
    };
    let scsynth = Scsynth::new(client, options);
    let outcome = timeout(LIMIT, scsynth.sync())
        .await
        .expect("the limit of the client ends first");
    assert_eq!(outcome, Err(Error::Timeout));
}
