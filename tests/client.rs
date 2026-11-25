//! `Scsynth` on a `ChannelClient` pair, with no runtime: the tests poll the
//! futures by hand, and fire the sleeps of the timer themselves.

mod common;

use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

use common::{
    ManualTimer, all, answer, barrier, connect, fail, n_go, now, poll, request, serve, status,
    synced, version,
};
use scsynth_client::scsynth_osc::command::node::NFree;
use scsynth_client::scsynth_osc::command::synth::SNew;
use scsynth_client::scsynth_osc::command::synth_def::DRecv;
use scsynth_client::scsynth_osc::command::top_level::Notify;
use scsynth_client::scsynth_osc::reply::command_replies::Done;
use scsynth_client::scsynth_osc::reply::node_notification::{NodeKind, NodeState};
use scsynth_client::scsynth_osc::{
    AddAction, Command, NewNodeId, NodeId, Packet, RawArg, RawMessage, Reply,
};
use scsynth_client::{
    ChannelClient, ClientError, Error, Event, Incoming, Listener, Options, OscClient, Registration,
    Scsynth, SendError,
};

const LIMIT: Duration = Duration::from_secs(3);

fn s_new(def: &str, node: i32) -> Command {
    Command::SNew(SNew {
        def: def.to_owned(),
        id: NewNodeId::new(node).expect("a node id"),
        action: AddAction::Head,
        target: NodeId::ROOT,
        controls: Vec::new(),
    })
}

fn notify(on: bool, client_id: Option<i32>) -> Command {
    Command::Notify(Notify { on, client_id })
}

const ALREADY_REGISTERED: &str = "notify: already registered\n";

/// A transport over a channel pair that refuses each packet over `max`
/// bytes.
struct Limited {
    client: ChannelClient,
    max: usize,
}

impl OscClient for Limited {
    fn send(&self, packet: &Packet<Command>) -> Result<(), SendError> {
        let size = packet.encode().len();
        if size > self.max {
            return Err(SendError::TooLarge {
                size,
                max: self.max,
            });
        }
        self.client.send(packet)
    }

    fn listen(&self, listener: Listener) {
        self.client.listen(listener);
    }

    fn close(&self) {
        self.client.close();
    }
}

#[test]
fn the_client_is_clone_send_and_sync_and_its_futures_are_send() {
    fn clone_send_sync<T: Clone + Send + Sync>() {}
    fn send<T: Send>(_: &T) {}
    clone_send_sync::<Scsynth>();
    let (scsynth, _peer, _timer) = connect(Options::default());
    send(&scsynth.request(Vec::new()));
    send(&scsynth.sync());
    send(&scsynth.status());
    send(&scsynth.version());
    send(&scsynth.quit());
    send(&scsynth.register());
    send(&scsynth.unregister());
    send(&scsynth.close());
    let mut events = scsynth.subscribe();
    send(&events.next());
    send(&events);
}

#[test]
fn sync_sends_an_empty_request_and_ends_at_its_synced() {
    let (scsynth, peer, timer) = connect(Options::default());
    let packets = serve(&peer, answer);
    assert_eq!(now(scsynth.sync()), Ok(()));
    assert_eq!(now(scsynth.sync()), Ok(()));
    assert_eq!(
        all(&packets),
        [request(Vec::new(), 1), request(Vec::new(), 2)]
    );
    assert_eq!(timer.limits(), []);
}

#[test]
fn a_request_gives_the_replies_kept_while_it_was_in_flight() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, |command| match command {
        Command::SNew(new) => vec![n_go(new.id.get())],
        other => answer(other),
    });
    let commands = vec![s_new("sine", 1000), s_new("sine", 1001)];
    assert_eq!(
        now(scsynth.request(commands.clone())),
        Ok(vec![n_go(1000), n_go(1001)])
    );
    assert_eq!(all(&packets), [request(commands, 1)]);
}

#[test]
fn a_fail_of_a_command_of_the_request_gives_failed() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let _packets = serve(&peer, |command| match command {
        Command::SNew(_) => vec![fail("/s_new", "SynthDef not found", None)],
        other => answer(other),
    });
    assert_eq!(
        now(scsynth.request(vec![s_new("missing", 1000)])),
        Err(Error::Failed {
            address: "/s_new".to_owned(),
            message: "SynthDef not found".to_owned(),
            value: None,
        })
    );
}

#[test]
fn the_second_request_leaves_only_after_the_first_settles() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let mut first = pin!(scsynth.request(vec![s_new("sine", 1000)]));
    let mut second = pin!(scsynth.request(vec![s_new("sine", 1001)]));
    assert!(poll(first.as_mut()).is_pending());
    assert!(poll(second.as_mut()).is_pending());
    assert_eq!(peer.take(), Some(request(vec![s_new("sine", 1000)], 1)));
    assert_eq!(peer.take(), None);
    peer.send(Incoming::Reply(n_go(1000)));
    peer.send(synced(1));
    assert_eq!(peer.take(), Some(request(vec![s_new("sine", 1001)], 2)));
    assert_eq!(poll(first.as_mut()), Poll::Ready(Ok(vec![n_go(1000)])));
    assert!(poll(second.as_mut()).is_pending());
    peer.send(synced(2));
    assert_eq!(poll(second.as_mut()), Poll::Ready(Ok(Vec::new())));
}

#[test]
fn a_timeout_sends_a_barrier_that_holds_the_next_request_until_it_answers() {
    let (scsynth, peer, timer) = connect(Options::default());
    let mut first = pin!(scsynth.sync());
    let mut second = pin!(scsynth.sync());
    assert!(poll(first.as_mut()).is_pending());
    assert!(poll(second.as_mut()).is_pending());
    assert_eq!(peer.take(), Some(request(Vec::new(), 1)));
    assert_eq!(timer.limits(), [LIMIT, LIMIT]);
    timer.fire(0);
    assert_eq!(poll(first.as_mut()), Poll::Ready(Err(Error::Timeout)));
    assert_eq!(peer.take(), Some(barrier(2)));
    assert!(poll(second.as_mut()).is_pending());
    assert_eq!(peer.take(), None);
    peer.send(synced(2));
    assert_eq!(peer.take(), Some(request(Vec::new(), 3)));
    peer.send(synced(3));
    assert_eq!(poll(second.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn a_result_that_comes_before_the_poll_wins_over_a_fired_sleep() {
    let (scsynth, peer, timer) = connect(Options::default());
    let mut call = pin!(scsynth.sync());
    assert!(poll(call.as_mut()).is_pending());
    timer.fire(0);
    peer.send(synced(1));
    assert_eq!(poll(call.as_mut()), Poll::Ready(Ok(())));
    assert_eq!(peer.take(), Some(request(Vec::new(), 1)));
    assert_eq!(peer.take(), None);
}

#[test]
fn the_limit_of_each_call_reaches_the_timer_at_its_first_poll() {
    let second = Duration::from_secs(1);
    let short = Duration::from_millis(250);
    let options = Options {
        timeout: second,
        ..Options::default()
    };
    let (scsynth, _peer, timer) = connect(options);
    let mut sync = pin!(scsynth.sync());
    assert_eq!(timer.limits(), []);
    assert!(poll(sync.as_mut()).is_pending());
    let quick = scsynth.timeout(short);
    let mut quick_sync = pin!(quick.sync());
    let mut quick_status = pin!(quick.status());
    assert!(poll(quick_sync.as_mut()).is_pending());
    assert!(poll(quick_status.as_mut()).is_pending());
    assert_eq!(timer.limits(), [second, short, short]);
    timer.fire(2);
    assert_eq!(
        poll(quick_status.as_mut()),
        Poll::Ready(Err(Error::Timeout))
    );
    assert!(poll(quick_sync.as_mut()).is_pending());
}

#[test]
fn a_dropped_call_that_was_queued_is_never_sent() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let mut first = pin!(scsynth.sync());
    assert!(poll(first.as_mut()).is_pending());
    let mut queued = Box::pin(scsynth.request(vec![s_new("sine", 1000)]));
    assert!(poll(queued.as_mut()).is_pending());
    drop(queued);
    assert_eq!(peer.take(), Some(request(Vec::new(), 1)));
    peer.send(synced(1));
    assert_eq!(poll(first.as_mut()), Poll::Ready(Ok(())));
    assert_eq!(peer.take(), None);
}

#[test]
fn a_dropped_call_in_flight_leaves_a_barrier() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let mut flight = Box::pin(scsynth.sync());
    assert!(poll(flight.as_mut()).is_pending());
    assert_eq!(peer.take(), Some(request(Vec::new(), 1)));
    drop(flight);
    assert_eq!(peer.take(), Some(barrier(2)));
    let mut next = pin!(scsynth.sync());
    assert!(poll(next.as_mut()).is_pending());
    assert_eq!(peer.take(), None);
    peer.send(synced(2));
    assert_eq!(peer.take(), Some(request(Vec::new(), 3)));
}

#[test]
fn send_goes_out_at_once_outside_the_queue() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let mut flight = pin!(scsynth.request(Vec::new()));
    assert!(poll(flight.as_mut()).is_pending());
    let free = Packet::Message(Command::NFree(NFree {
        nodes: vec![NodeId(1000)],
    }));
    assert_eq!(scsynth.send(&free), Ok(()));
    assert_eq!(peer.take(), Some(request(Vec::new(), 1)));
    assert_eq!(peer.take(), Some(free.clone()));
    peer.send(Incoming::Reply(Reply::NEnd(NodeState {
        node: NodeId(1000),
        parent: Some(NodeId::ROOT),
        prev: None,
        next: None,
        kind: NodeKind::Synth,
    })));
    peer.send(synced(1));
    let Poll::Ready(Ok(kept)) = poll(flight.as_mut()) else {
        panic!("the request settles");
    };
    assert!(matches!(kept.as_slice(), [Reply::NEnd(_)]), "{kept:?}");
    peer.close();
    assert_eq!(scsynth.send(&free), Err(Error::Send(SendError::Closed)));
}

#[test]
fn a_subscription_gets_every_event_from_its_call_then_the_close_then_none() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let _packets = serve(&peer, answer);
    let mut events = scsynth.subscribe();
    assert!(poll(pin!(events.next())).is_pending());
    assert_eq!(now(scsynth.sync()), Ok(()));
    let mut later = scsynth.subscribe();
    drop(scsynth.subscribe());
    let tick = RawMessage {
        address: "/tick".to_owned(),
        args: vec![RawArg::Int(1)],
    };
    peer.send(Incoming::Other(tick.clone()));
    let error = ClientError::Transport("a write failed".to_owned());
    peer.error(error.clone());
    peer.close();
    let mut after = scsynth.subscribe();
    let tail = [
        Event::Message(Incoming::Other(tick)),
        Event::Error(error),
        Event::Closed,
    ];
    assert_eq!(now(events.next()), Some(Event::Message(synced(1))));
    for event in &tail {
        assert_eq!(now(events.next()).as_ref(), Some(event));
        assert_eq!(now(later.next()).as_ref(), Some(event));
    }
    assert_eq!(now(events.next()), None);
    assert_eq!(now(later.next()), None);
    assert_eq!(now(after.next()), None);
}

#[test]
fn a_subscription_ends_once_every_handle_of_the_client_is_dropped() {
    let (scsynth, _peer, _timer) = connect(Options::default());
    let mut events = scsynth.subscribe();
    let clone = scsynth.clone();
    drop(scsynth);
    assert!(poll(pin!(events.next())).is_pending());
    drop(clone);
    assert_eq!(now(events.next()), None);
}

#[test]
fn status_version_and_quit_send_their_command_alone_and_take_its_reply() {
    let (scsynth, peer, timer) = connect(Options::default());
    let packets = serve(&peer, answer);
    assert_eq!(now(scsynth.status()), Ok(status()));
    assert_eq!(now(scsynth.version()), Ok(version()));
    assert_eq!(now(scsynth.quit()), Ok(()));
    let commands = [Command::Status, Command::Version, Command::Quit];
    assert_eq!(all(&packets), commands.map(Packet::Message));
    assert_eq!(timer.limits(), []);
}

#[test]
fn status_waits_outside_the_queue_and_the_flight_keeps_its_reply() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let mut flight = pin!(scsynth.request(Vec::new()));
    let mut load = pin!(scsynth.status());
    assert!(poll(flight.as_mut()).is_pending());
    assert!(poll(load.as_mut()).is_pending());
    assert_eq!(peer.take(), Some(request(Vec::new(), 1)));
    assert_eq!(peer.take(), Some(Packet::Message(Command::Status)));
    peer.send(Incoming::Reply(Reply::StatusReply(status())));
    assert_eq!(poll(load.as_mut()), Poll::Ready(Ok(status())));
    peer.send(synced(1));
    assert_eq!(
        poll(flight.as_mut()),
        Poll::Ready(Ok(vec![Reply::StatusReply(status())]))
    );
}

#[test]
fn register_gives_the_client_id_and_the_limit_of_the_reply() {
    let options = Options {
        client_id: Some(3),
        max_logins: Some(4),
        ..Options::default()
    };
    let (scsynth, peer, _timer) = connect(options);
    let packets = serve(&peer, |command| match command {
        Command::Notify(Notify { on: true, .. }) => vec![Reply::Done(Done::Notify {
            client_id: 3,
            max_logins: Some(8),
        })],
        other => answer(other),
    });
    assert_eq!(
        now(scsynth.register()),
        Ok(Registration {
            client_id: 3,
            max_logins: 8,
        })
    );
    assert_eq!(all(&packets), [request(vec![notify(true, Some(3))], 1)]);
}

#[test]
fn register_takes_the_limit_of_the_options_when_the_reply_has_none() {
    let reply_without_limit = |command: &Command| match command {
        Command::Notify(Notify { on: true, .. }) => vec![Reply::Done(Done::Notify {
            client_id: 1,
            max_logins: None,
        })],
        other => answer(other),
    };
    let options = Options {
        max_logins: Some(4),
        ..Options::default()
    };
    let (scsynth, peer, _timer) = connect(options);
    let _packets = serve(&peer, reply_without_limit);
    assert_eq!(
        now(scsynth.register()),
        Ok(Registration {
            client_id: 1,
            max_logins: 4,
        })
    );
    let (scsynth, peer, _timer) = connect(Options::default());
    let _packets = serve(&peer, reply_without_limit);
    assert_eq!(
        now(scsynth.register()),
        Err(Error::NoReply { address: "/notify" })
    );
}

#[test]
fn register_without_a_done_gives_no_reply() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let _packets = serve(&peer, |command| match command {
        Command::Notify(_) => Vec::new(),
        other => answer(other),
    });
    assert_eq!(
        now(scsynth.register()),
        Err(Error::NoReply { address: "/notify" })
    );
}

#[test]
fn register_on_a_registered_connection_takes_the_limit_of_the_options() {
    let options = Options {
        max_logins: Some(4),
        ..Options::default()
    };
    let (scsynth, peer, _timer) = connect(options);
    let packets = serve(&peer, |command| match command {
        Command::Notify(Notify { on: true, .. }) => {
            vec![fail("/notify", ALREADY_REGISTERED, Some(2))]
        }
        other => answer(other),
    });
    assert_eq!(
        now(scsynth.register()),
        Ok(Registration {
            client_id: 2,
            max_logins: 4,
        })
    );
    assert_eq!(now(scsynth.close()), Ok(()));
    assert_eq!(
        all(&packets),
        [
            request(vec![notify(true, None)], 1),
            request(vec![notify(false, None)], 2)
        ]
    );
}

#[test]
fn register_on_a_registered_connection_without_a_limit_gives_the_fail() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let _packets = serve(&peer, |command| match command {
        Command::Notify(Notify { on: true, .. }) => {
            vec![fail("/notify", ALREADY_REGISTERED, Some(0))]
        }
        other => answer(other),
    });
    assert_eq!(
        now(scsynth.register()),
        Err(Error::Failed {
            address: "/notify".to_owned(),
            message: ALREADY_REGISTERED.to_owned(),
            value: Some(0),
        })
    );
}

#[test]
fn register_with_a_client_id_over_31_sends_notify_0_and_gives_no_node_range() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, |command| match command {
        Command::Notify(Notify { on: true, .. }) => vec![Reply::Done(Done::Notify {
            client_id: 32,
            max_logins: Some(64),
        })],
        other => answer(other),
    });
    assert_eq!(
        now(scsynth.register()),
        Err(Error::NoNodeRange { client_id: 32 })
    );
    assert_eq!(now(scsynth.close()), Ok(()));
    assert_eq!(
        all(&packets),
        [
            request(vec![notify(true, None)], 1),
            request(vec![notify(false, None)], 2)
        ]
    );
}

#[test]
fn register_with_a_client_id_under_0_gives_no_node_range_whatever_notify_0_answers() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, |command| match command {
        Command::Notify(Notify { on: true, .. }) => vec![Reply::Done(Done::Notify {
            client_id: -1,
            max_logins: Some(64),
        })],
        Command::Notify(Notify { on: false, .. }) => {
            vec![fail("/notify", "not registered\n", None)]
        }
        other => answer(other),
    });
    assert_eq!(
        now(scsynth.register()),
        Err(Error::NoNodeRange { client_id: -1 })
    );
    assert_eq!(now(scsynth.close()), Ok(()));
    assert_eq!(
        all(&packets),
        [
            request(vec![notify(true, None)], 1),
            request(vec![notify(false, None)], 2)
        ]
    );
}

#[test]
fn the_options_default_to_the_flags_of_scsynth() {
    assert_eq!(
        Options::default(),
        Options {
            timeout: Duration::from_secs(3),
            client_id: None,
            max_logins: None,
            buffers: 1024,
            control_buses: 16384,
            audio_buses: 1024,
            inputs: 8,
            outputs: 8,
        }
    );
}

#[test]
fn unregister_sends_notify_0_and_the_client_forgets_its_registration() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, answer);
    assert_eq!(
        now(scsynth.register()),
        Ok(Registration {
            client_id: 0,
            max_logins: 4,
        })
    );
    assert_eq!(now(scsynth.unregister()), Ok(()));
    assert_eq!(now(scsynth.close()), Ok(()));
    assert_eq!(
        all(&packets),
        [
            request(vec![notify(true, None)], 1),
            request(vec![notify(false, None)], 2)
        ]
    );
    assert!(peer.is_closed());
}

#[test]
fn close_unregisters_a_registered_client_then_closes() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, answer);
    assert!(now(scsynth.register()).is_ok());
    assert_eq!(now(scsynth.close()), Ok(()));
    assert_eq!(
        all(&packets),
        [
            request(vec![notify(true, None)], 1),
            request(vec![notify(false, None)], 2)
        ]
    );
    assert!(peer.is_closed());
}

#[test]
fn close_of_a_client_that_is_not_registered_only_closes() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, answer);
    assert_eq!(now(scsynth.close()), Ok(()));
    assert_eq!(all(&packets), []);
    assert!(peer.is_closed());
}

#[test]
fn close_gives_the_error_of_unregister_after_the_close() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let _packets = serve(&peer, |command| match command {
        Command::Notify(Notify { on: false, .. }) => {
            vec![fail("/notify", "not registered", None)]
        }
        other => answer(other),
    });
    assert!(now(scsynth.register()).is_ok());
    assert_eq!(
        now(scsynth.close()),
        Err(Error::Failed {
            address: "/notify".to_owned(),
            message: "not registered".to_owned(),
            value: None,
        })
    );
    assert!(peer.is_closed());
}

#[test]
fn a_close_of_the_transport_settles_the_pending_calls_and_refuses_the_next() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let mut flight = pin!(scsynth.sync());
    let mut queued = pin!(scsynth.sync());
    let mut load = pin!(scsynth.status());
    assert!(poll(flight.as_mut()).is_pending());
    assert!(poll(queued.as_mut()).is_pending());
    assert!(poll(load.as_mut()).is_pending());
    peer.close();
    let closed = Error::Send(SendError::Closed);
    assert_eq!(poll(flight.as_mut()), Poll::Ready(Err(closed.clone())));
    assert_eq!(poll(queued.as_mut()), Poll::Ready(Err(closed.clone())));
    assert_eq!(poll(load.as_mut()), Poll::Ready(Err(closed.clone())));
    assert_eq!(now(scsynth.sync()), Err(closed.clone()));
    assert_eq!(now(scsynth.version()), Err(closed));
}

#[test]
fn a_refused_packet_gives_send_and_the_next_request_still_leaves() {
    let (client, peer) = ChannelClient::pair();
    let timer = ManualTimer::default();
    let scsynth = Scsynth::with_timer(
        Limited { client, max: 64 },
        timer.clone(),
        Options::default(),
    );
    let large = Command::DRecv(DRecv {
        data: vec![0; 100],
        completion: None,
    });
    let too_large = |sync| {
        Err(Error::Send(SendError::TooLarge {
            size: request(vec![large.clone()], sync).encode().len(),
            max: 64,
        }))
    };
    assert_eq!(now(scsynth.request(vec![large.clone()])), too_large(1));
    let mut flight = pin!(scsynth.sync());
    let mut refused = pin!(scsynth.request(vec![large.clone()]));
    let mut next = pin!(scsynth.sync());
    assert!(poll(flight.as_mut()).is_pending());
    assert!(poll(refused.as_mut()).is_pending());
    assert!(poll(next.as_mut()).is_pending());
    assert_eq!(peer.take(), Some(request(Vec::new(), 2)));
    peer.send(synced(2));
    assert_eq!(poll(flight.as_mut()), Poll::Ready(Ok(())));
    assert_eq!(poll(refused.as_mut()), Poll::Ready(too_large(3)));
    assert_eq!(peer.take(), Some(request(Vec::new(), 4)));
    peer.send(synced(4));
    assert_eq!(poll(next.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn a_refused_status_gives_send_and_starts_no_sleep() {
    let (client, peer) = ChannelClient::pair();
    let timer = ManualTimer::default();
    let scsynth = Scsynth::with_timer(
        Limited { client, max: 8 },
        timer.clone(),
        Options::default(),
    );
    let size = Packet::Message(Command::Status).encode().len();
    assert_eq!(
        now(scsynth.status()),
        Err(Error::Send(SendError::TooLarge { size, max: 8 }))
    );
    assert_eq!(timer.limits(), []);
    assert_eq!(peer.take(), None);
}
