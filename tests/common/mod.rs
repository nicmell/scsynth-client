//! What the tests of the client share: a timer that the test fires, a poll
//! by hand with no runtime, and a peer that plays scsynth.

use std::future::{self, Future};
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use scsynth_client::scsynth_osc::command::top_level::Notify;
use scsynth_client::scsynth_osc::reply::command_replies::Done;
use scsynth_client::scsynth_osc::reply::top_level::{StatusReply, VersionReply};
use scsynth_client::scsynth_osc::{Bundle, Command, Packet, Reply, SyncId, Timetag};
use scsynth_client::{ChannelClient, ChannelPeer, Incoming, Options, Scsynth, Sleep, Timer};

/// A timer whose sleeps end when the test fires them.
#[derive(Clone, Default)]
pub struct ManualTimer(Arc<Mutex<Vec<Alarm>>>);

struct Alarm {
    limit: Duration,
    fired: Arc<AtomicBool>,
}

impl Timer for ManualTimer {
    fn sleep(&self, limit: Duration) -> Sleep {
        let fired = Arc::new(AtomicBool::new(false));
        self.alarms().push(Alarm {
            limit,
            fired: Arc::clone(&fired),
        });
        Box::pin(future::poll_fn(move |_| {
            if fired.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }))
    }
}

impl ManualTimer {
    /// The limit of each sleep so far, in the order of their start.
    pub fn limits(&self) -> Vec<Duration> {
        self.alarms().iter().map(|alarm| alarm.limit).collect()
    }

    /// Ends the sleep `index`, in the order of their start.
    pub fn fire(&self, index: usize) {
        self.alarms()[index].fired.store(true, Ordering::SeqCst);
    }

    fn alarms(&self) -> std::sync::MutexGuard<'_, Vec<Alarm>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Polls `future` once, with a waker that does nothing.
pub fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

/// The output of `future`, which must be ready at its first poll.
pub fn now<F: Future>(future: F) -> F::Output {
    match poll(pin!(future)) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("the future is pending"),
    }
}

/// A client on a channel pair, with a timer that the test fires.
pub fn connect(options: Options) -> (Scsynth, ChannelPeer, ManualTimer) {
    let (client, peer) = ChannelClient::pair();
    let timer = ManualTimer::default();
    let scsynth = Scsynth::with_timer(client, timer.clone(), options);
    (scsynth, peer, timer)
}

/// Makes `peer` answer each command of each packet with `answer`, and gives
/// the packets that it gets.
pub fn serve<A>(peer: &ChannelPeer, mut answer: A) -> Receiver<Packet<Command>>
where
    A: FnMut(&Command) -> Vec<Reply> + Send + 'static,
{
    let (packets, received) = mpsc::channel();
    peer.on_packet(move |peer, packet| {
        packets.send(packet.clone()).expect("the test receives");
        for command in packet.into_messages() {
            for reply in answer(&command) {
                peer.send(Incoming::Reply(reply));
            }
        }
    });
    received
}

/// The answers of a scsynth that registers its clients with the id 0 and a
/// limit of 4 logins.
pub fn answer(command: &Command) -> Vec<Reply> {
    match command {
        Command::Sync(id) => vec![Reply::Synced(*id)],
        Command::Status => vec![Reply::StatusReply(status())],
        Command::Version => vec![Reply::VersionReply(version())],
        Command::Quit => vec![Reply::Done(Done::Quit)],
        Command::Notify(Notify { on: true, .. }) => vec![Reply::Done(Done::Notify {
            client_id: 0,
            max_logins: Some(4),
        })],
        Command::Notify(Notify { on: false, .. }) => vec![Reply::Done(Done::NotifyOff)],
        _ => Vec::new(),
    }
}

pub fn status() -> StatusReply {
    StatusReply {
        ugens: 0,
        synths: 0,
        groups: 1,
        synthdefs: 0,
        avg_cpu: 0.0,
        peak_cpu: 0.0,
        nominal_sample_rate: 44_100.0,
        actual_sample_rate: 44_100.0,
    }
}

pub fn version() -> VersionReply {
    VersionReply {
        program: "scsynth".to_owned(),
        major: 3,
        minor: 14,
        patch: ".1".to_owned(),
        branch: "Version-3.14.1".to_owned(),
        commit: "na".to_owned(),
    }
}

/// The packet of a request: its commands, then `/sync sync`, in one
/// immediate bundle.
pub fn request(commands: Vec<Command>, sync: i32) -> Packet<Command> {
    let packets = commands
        .into_iter()
        .chain([Command::Sync(SyncId(sync))])
        .map(Packet::Message)
        .collect();
    Packet::Bundle(Bundle {
        time: Timetag::IMMEDIATELY,
        packets,
    })
}

/// A lone `/sync`: the packet of a barrier.
pub fn barrier(sync: i32) -> Packet<Command> {
    Packet::Message(Command::Sync(SyncId(sync)))
}

pub fn synced(sync: i32) -> Incoming {
    Incoming::Reply(Reply::Synced(SyncId(sync)))
}

pub fn all<T>(receiver: &Receiver<T>) -> Vec<T> {
    receiver.try_iter().collect()
}
