//! `ChannelClient` and `ChannelPeer`: packets and events pass both ways in
//! the order of their arrival, before the call that adds them returns.
//! Events wait for `listen`, `Closed` comes once from either end, and a
//! listener or a handler that sends from inside its call runs neither inside
//! itself nor inside the other. `decode` types what it can of a packet, and
//! keeps the rest as `Incoming::Other`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};

use scsynth_client::{
    ChannelClient, ChannelPeer, ClientError, Event, Incoming, OscClient, SendError, decode,
};
use scsynth_osc::{
    Bundle, Command, DecodeErrorKind, Packet, RawArg, RawMessage, Reply, SyncId, Timetag,
};

fn sync(id: i32) -> Packet<Command> {
    Packet::Message(Command::Sync(SyncId(id)))
}

fn synced(id: i32) -> Incoming {
    Incoming::Reply(Reply::Synced(SyncId(id)))
}

fn tick(beat: i32) -> RawMessage {
    RawMessage {
        address: "/tick".to_owned(),
        args: vec![RawArg::Int(beat)],
    }
}

/// The events that the listener of `client` gets from now on.
fn listen(client: &ChannelClient) -> Receiver<Event> {
    let (sender, receiver) = mpsc::channel();
    client.listen(Box::new(move |event| {
        sender.send(event).expect("the test receives");
    }));
    receiver
}

/// The packets that the handler of `peer` gets from now on.
fn handle(peer: &ChannelPeer) -> Receiver<Packet<Command>> {
    let (sender, receiver) = mpsc::channel();
    peer.on_packet(move |_, packet| sender.send(packet).expect("the test receives"));
    receiver
}

fn all<T>(receiver: &Receiver<T>) -> Vec<T> {
    receiver.try_iter().collect()
}

fn assert_send_sync<T: Send + Sync>() {}

fn assert_client<T: OscClient>() {}

#[test]
fn both_ends_are_send_and_sync_and_the_client_is_an_osc_client() {
    assert_send_sync::<ChannelClient>();
    assert_send_sync::<ChannelPeer>();
    assert_client::<ChannelClient>();
}

#[test]
fn packets_reach_the_peer_in_order_through_the_queue_then_the_handler() {
    let (client, peer) = ChannelClient::pair();
    for id in 1..=3 {
        client.send(&sync(id)).expect("open");
    }
    assert_eq!(peer.take(), Some(sync(1)));
    assert_eq!(peer.take(), Some(sync(2)));
    client.send(&sync(4)).expect("open");
    let packets = handle(&peer);
    assert_eq!(all(&packets), [sync(3), sync(4)]);
    client.send(&sync(5)).expect("open");
    assert_eq!(all(&packets), [sync(5)]);
    assert_eq!(peer.take(), None);
}

#[test]
fn messages_and_errors_reach_the_client_in_order_before_the_call_returns() {
    let (client, peer) = ChannelClient::pair();
    let events = listen(&client);
    peer.send(synced(1));
    assert_eq!(all(&events), [Event::Message(synced(1))]);
    let error = ClientError::Transport("a write failed".to_owned());
    peer.send(Incoming::Other(tick(1)));
    peer.error(error.clone());
    peer.send(synced(2));
    assert_eq!(
        all(&events),
        [
            Event::Message(Incoming::Other(tick(1))),
            Event::Error(error),
            Event::Message(synced(2)),
        ]
    );
}

#[test]
fn events_before_listen_wait_for_it_and_a_second_listener_replaces_the_first() {
    let (client, peer) = ChannelClient::pair();
    peer.send(synced(1));
    peer.send(synced(2));
    let first = listen(&client);
    assert_eq!(
        all(&first),
        [Event::Message(synced(1)), Event::Message(synced(2))]
    );
    let second = listen(&client);
    peer.send(synced(3));
    assert_eq!(all(&first), Vec::<Event>::new());
    assert_eq!(all(&second), [Event::Message(synced(3))]);
}

#[test]
fn a_close_of_the_client_closes_both_ends_once() {
    let (client, peer) = ChannelClient::pair();
    let events = listen(&client);
    client.close();
    assert!(peer.is_closed());
    client.close();
    peer.close();
    assert_eq!(all(&events), [Event::Closed]);
    assert_eq!(client.send(&sync(1)), Err(SendError::Closed));
    peer.send(synced(1));
    assert_eq!(all(&events), Vec::<Event>::new());
}

#[test]
fn a_close_of_the_peer_comes_once_after_the_events_that_wait() {
    let (client, peer) = ChannelClient::pair();
    peer.send(synced(1));
    peer.close();
    peer.close();
    client.close();
    peer.send(synced(2));
    peer.error(ClientError::Transport("late".to_owned()));
    assert_eq!(client.send(&sync(1)), Err(SendError::Closed));
    let events = listen(&client);
    assert_eq!(all(&events), [Event::Message(synced(1)), Event::Closed]);
}

#[test]
fn packets_sent_before_the_close_still_reach_the_peer() {
    let (client, peer) = ChannelClient::pair();
    client.send(&sync(1)).expect("open");
    client.close();
    assert_eq!(peer.take(), Some(sync(1)));
    assert_eq!(peer.take(), None);
}

/// A guard that panics when a listener or a handler starts inside another.
#[derive(Clone, Default)]
struct Inside(Arc<AtomicBool>);

impl Inside {
    fn enter(&self) {
        assert!(
            !self.0.swap(true, Ordering::SeqCst),
            "a call runs inside another"
        );
    }

    fn leave(&self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

#[test]
fn a_handler_that_answers_and_a_listener_that_sends_run_one_at_a_time_in_order() {
    let (client, peer) = ChannelClient::pair();
    let client = Arc::new(client);
    let log = Arc::new(Mutex::new(Vec::new()));
    let inside = Inside::default();

    let (peer_log, peer_inside) = (Arc::clone(&log), inside.clone());
    peer.on_packet(move |answer, packet| {
        peer_inside.enter();
        let Packet::Message(Command::Sync(SyncId(id))) = packet else {
            panic!("a lone /sync");
        };
        peer_log
            .lock()
            .expect("log")
            .push(format!("peer gets {id}"));
        answer.send(synced(id));
        answer.send(Incoming::Other(tick(id)));
        peer_log
            .lock()
            .expect("log")
            .push(format!("peer answered {id}"));
        peer_inside.leave();
    });

    let (sender, client_log, client_inside) = (Arc::clone(&client), Arc::clone(&log), inside);
    client.listen(Box::new(move |event| {
        client_inside.enter();
        let line = match event {
            Event::Message(Incoming::Reply(Reply::Synced(SyncId(id)))) => {
                client_log
                    .lock()
                    .expect("log")
                    .push(format!("client gets {id}"));
                if id < 3 {
                    sender.send(&sync(id + 1)).expect("open");
                    format!("client sent {}", id + 1)
                } else {
                    sender.close();
                    "client closed".to_owned()
                }
            }
            Event::Message(Incoming::Other(raw)) => format!("client gets {}", raw.address),
            Event::Message(Incoming::Reply(reply)) => panic!("{reply:?}"),
            Event::Error(error) => panic!("{error}"),
            Event::Closed => "client gets the close".to_owned(),
        };
        client_log.lock().expect("log").push(line);
        client_inside.leave();
    }));

    client.send(&sync(1)).expect("open");
    let mut expected = Vec::new();
    for id in 1..=3 {
        expected.extend([
            format!("peer gets {id}"),
            format!("peer answered {id}"),
            format!("client gets {id}"),
        ]);
        if id < 3 {
            expected.push(format!("client sent {}", id + 1));
        } else {
            expected.push("client closed".to_owned());
        }
        expected.push("client gets /tick".to_owned());
    }
    expected.push("client gets the close".to_owned());
    assert_eq!(*log.lock().expect("log"), expected);
    assert!(peer.is_closed());
}

#[test]
fn a_call_from_another_thread_delivers() {
    let (client, peer) = ChannelClient::pair();
    let events = listen(&client);
    std::thread::spawn(move || peer.send(synced(1)))
        .join()
        .expect("the thread ends");
    assert_eq!(all(&events), [Event::Message(synced(1))]);
}

#[test]
fn decode_types_a_reply() {
    let bytes = Reply::Synced(SyncId(7)).encode();
    assert_eq!(decode(&bytes), Ok(vec![synced(7)]));
}

#[test]
fn decode_keeps_an_unknown_address_and_wrong_arguments_as_other() {
    let wrong = RawMessage {
        address: "/synced".to_owned(),
        args: vec![RawArg::Float(7.0)],
    };
    for raw in [tick(1), wrong] {
        assert_eq!(decode(&raw.encode()), Ok(vec![Incoming::Other(raw)]));
    }
}

#[test]
fn decode_refuses_bytes_that_are_no_osc_packet() {
    let Err(ClientError::Decode(error)) = decode(&[0, 1, 2]) else {
        panic!("no OSC packet");
    };
    assert!(matches!(error.kind(), DecodeErrorKind::Osc(_)), "{error}");
}

#[test]
fn decode_gives_the_messages_of_a_bundle_in_order() {
    let bundle = Packet::Bundle(Bundle {
        time: Timetag::IMMEDIATELY,
        packets: vec![
            Packet::Message(Reply::Synced(SyncId(1))),
            Packet::Message(Reply::Synced(SyncId(2))),
        ],
    });
    assert_eq!(decode(&bundle.encode()), Ok(vec![synced(1), synced(2)]));
}

#[test]
fn decode_types_the_replies_of_a_bundle_around_an_untyped_message() {
    let bundle = Packet::Bundle(Bundle {
        time: Timetag::IMMEDIATELY,
        packets: vec![
            Packet::Message(Reply::Synced(SyncId(1)).to_raw()),
            Packet::Message(tick(1)),
            Packet::Message(Reply::Synced(SyncId(2)).to_raw()),
        ],
    });
    assert_eq!(
        decode(&bundle.encode()),
        Ok(vec![synced(1), Incoming::Other(tick(1)), synced(2)])
    );
}
