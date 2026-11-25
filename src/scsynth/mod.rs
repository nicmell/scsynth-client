use std::collections::{HashMap, VecDeque};
use std::mem;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use scsynth_osc::command::top_level::Notify;
use scsynth_osc::reply::command_replies::Done;
use scsynth_osc::reply::top_level::{StatusReply, VersionReply};
use scsynth_osc::{Command, NodeId, Packet, Reply};

use crate::client::{Event, OscClient, SendError};
use crate::core::{Core, Effect, RequestId, Wait};
use crate::error::Error;
use crate::ids::{IdKind, Ids};
use crate::options::Options;
use crate::timer::Timer;
use crate::wait::{self, Raced, Receiver, Sender, Writer};

mod buffers;
mod buses;
mod defs;
mod nodes;

pub use nodes::Target;

/// A client of scsynth over one [`OscClient`]: a handle, whose clones share
/// one connection.
///
/// Its awaited methods wait for the answer of scsynth within a limit, which
/// counts from the first poll of the call. One request is in flight at a
/// time, as one bundle that ends in `/sync`. On a native target it is `Send`
/// and `Sync`, and the future of each method is `Send`.
#[derive(Clone)]
pub struct Scsynth {
    shared: Arc<Shared>,
    limit: Duration,
}

/// The login of a client of scsynth: what [`Scsynth::register`] gives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Registration {
    /// The id of this client among the clients of the server.
    pub client_id: i32,
    /// How many clients the server takes at once.
    pub max_logins: i32,
}

/// The events of the transport of a [`Scsynth`], from the call of
/// [`Scsynth::subscribe`] on.
pub struct Subscription {
    events: wait::Reader<Event>,
}

/// The result of a request or a wait of the core.
type Outcome = Result<Vec<Reply>, Error>;

/// What the clones of a [`Scsynth`] share.
struct Shared {
    client: Box<dyn OscClient>,
    timer: Box<dyn Timer>,
    options: Options,
    state: Mutex<State>,
}

struct State {
    core: Core,
    /// The senders of the calls that wait for their outcome.
    calls: HashMap<RequestId, Sender<Outcome>>,
    subscriptions: Vec<Writer<Event>>,
    /// Whether scsynth holds a login for this connection, as far as the
    /// client knows.
    registered: bool,
    /// The ids of the client, from its last registration.
    ids: Option<Ids>,
    /// Whether the transport closed.
    closed: bool,
}

/// What the client does out of the lock of its state, in order.
enum Action {
    /// A packet for the transport, with the request that it carries.
    Send(Option<RequestId>, Packet<Command>),
    Settle(Sender<Outcome>, Outcome),
    Publish(Vec<Writer<Event>>, Event),
    /// The last event of the subscriptions.
    End(Vec<Writer<Event>>),
}

/// A call that waits for its outcome. Dropped before it settles, it counts
/// as timed out.
struct Call<'a> {
    shared: &'a Shared,
    id: RequestId,
    receiver: Receiver<Outcome>,
    settled: bool,
}

/// The start of the message of the `/fail` of `/notify` from a sender that
/// scsynth already registered.
const ALREADY_REGISTERED: &str = "notify: already registered";

impl Scsynth {
    /// A client over `client`, whose awaited methods race the sleeps of
    /// `timer`. It sets the listener of `client`.
    #[must_use]
    #[cfg_attr(
        target_arch = "wasm32",
        allow(
            clippy::arc_with_non_send_sync,
            reason = "the transport and the timer are Send and Sync on a native target alone"
        )
    )]
    pub fn with_timer(client: impl OscClient, timer: impl Timer, options: Options) -> Self {
        let limit = options.timeout;
        let state = State {
            core: Core::new(),
            calls: HashMap::new(),
            subscriptions: Vec::new(),
            registered: false,
            ids: None,
            closed: false,
        };
        let shared = Arc::new(Shared {
            client: Box::new(client),
            timer: Box::new(timer),
            options,
            state: Mutex::new(state),
        });
        // A strong reference would be a cycle: the client owns the listener,
        // and the shared state owns the client.
        let weak = Arc::downgrade(&shared);
        shared.client.listen(Box::new(move |event| {
            if let Some(shared) = weak.upgrade() {
                shared.event(event);
            }
        }));
        Self { shared, limit }
    }

    /// The same client, whose awaited methods take `limit` as their limit.
    #[must_use]
    pub fn timeout(&self, limit: Duration) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            limit,
        }
    }

    /// Sends one packet at once, outside the queue of the requests, and
    /// waits for nothing.
    ///
    /// # Errors
    ///
    /// [`Error::Send`] when the transport refuses the packet.
    pub fn send(&self, packet: &Packet<Command>) -> Result<(), Error> {
        self.shared.client.send(packet).map_err(Error::Send)
    }

    /// Sends `commands` as one request, after the request in flight, and
    /// gives the replies that arrived while it was in flight.
    ///
    /// # Errors
    ///
    /// [`Error::Failed`] with the first `/fail` of its commands,
    /// [`Error::Timeout`] when its `/synced` does not come within the limit,
    /// and [`Error::Send`] when the transport refuses it or closes.
    pub async fn request(&self, commands: Vec<Command>) -> Result<Vec<Reply>, Error> {
        self.shared
            .start(|core| core.request(commands))
            .outcome(self.limit)
            .await
    }

    /// A request with no command: it ends once scsynth has done what the
    /// requests before it asked.
    ///
    /// # Errors
    ///
    /// As [`Self::request`].
    pub async fn sync(&self) -> Result<(), Error> {
        self.request(Vec::new()).await.map(drop)
    }

    /// The load of the server: the next `/status.reply` after a plain send
    /// of `/status`.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] when no `/status.reply` comes within the limit, and
    /// [`Error::Send`] when the transport refuses `/status` or closes.
    pub async fn status(&self) -> Result<StatusReply, Error> {
        let replies = self.ask(Wait::Status, Command::Status).await?;
        replies
            .into_iter()
            .find_map(|reply| match reply {
                Reply::StatusReply(status) => Some(status),
                _ => None,
            })
            .ok_or(Error::NoReply { address: "/status" })
    }

    /// The version of the server: the next `/version.reply` after a plain
    /// send of `/version`.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] when no `/version.reply` comes within the limit,
    /// and [`Error::Send`] when the transport refuses `/version` or closes.
    pub async fn version(&self) -> Result<VersionReply, Error> {
        let replies = self.ask(Wait::Version, Command::Version).await?;
        replies
            .into_iter()
            .find_map(|reply| match reply {
                Reply::VersionReply(version) => Some(version),
                _ => None,
            })
            .ok_or(Error::NoReply {
                address: "/version",
            })
    }

    /// Stops the server: a plain send of `/quit`, which ends at its
    /// `/done /quit`.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] when no `/done /quit` comes within the limit, and
    /// [`Error::Send`] when the transport refuses `/quit` or closes.
    pub async fn quit(&self) -> Result<(), Error> {
        self.ask(Wait::Quit, Command::Quit).await.map(drop)
    }

    /// Registers this client for the notifications of the server: a request
    /// of `/notify 1`, with [`Options::client_id`].
    ///
    /// The login limit comes from the reply, else from
    /// [`Options::max_logins`]. A connection that scsynth already registered
    /// gets a `/fail` with its client id: with [`Options::max_logins`], that
    /// is a registration too. Each registration gives the client fresh
    /// ranges of ids, from its client id, its login limit and the counts of
    /// its [`Options`].
    ///
    /// # Errors
    ///
    /// As [`Self::request`]: [`Error::Failed`] for an already registered
    /// connection when [`Options::max_logins`] is `None`. [`Error::NoReply`]
    /// when the reply of `/notify` is missing, or has no login limit and
    /// [`Options::max_logins`] is `None`. [`Error::NoNodeRange`] for a client
    /// id under 0 or over 31, after a request of `/notify 0`, whatever its
    /// answer: the client is then not registered.
    pub async fn register(&self) -> Result<Registration, Error> {
        let registration = self.login().await?;
        let client_id = registration.client_id;
        let options = &self.shared.options;
        let Some(ids) = Ids::new(client_id, registration.max_logins, options) else {
            let _ = self.unregister().await;
            let mut state = self.shared.lock();
            state.registered = false;
            state.ids = None;
            return Err(Error::NoNodeRange { client_id });
        };
        self.shared.lock().ids = Some(ids);
        Ok(registration)
    }

    /// Sends `/notify 1`, and reads the registration in its answer.
    async fn login(&self) -> Result<Registration, Error> {
        let options = &self.shared.options;
        let notify = Notify {
            on: true,
            client_id: options.client_id,
        };
        let no_reply = Error::NoReply { address: "/notify" };
        match self.request(vec![Command::Notify(notify)]).await {
            Ok(replies) => {
                let Some((client_id, max_logins)) = replies.iter().find_map(notified) else {
                    return Err(no_reply);
                };
                self.shared.lock().registered = true;
                let max_logins = max_logins.or(options.max_logins).ok_or(no_reply)?;
                Ok(Registration {
                    client_id,
                    max_logins,
                })
            }
            Err(error) => {
                let Some(client_id) = already_registered(&error) else {
                    return Err(error);
                };
                self.shared.lock().registered = true;
                let max_logins = options.max_logins.ok_or(error)?;
                Ok(Registration {
                    client_id,
                    max_logins,
                })
            }
        }
    }

    /// Ends the registration of this client: a request of `/notify 0`. The
    /// client forgets its registration and its ids once scsynth answers.
    ///
    /// # Errors
    ///
    /// As [`Self::request`]: [`Error::Failed`] when scsynth holds no
    /// registration for this connection.
    pub async fn unregister(&self) -> Result<(), Error> {
        let notify = Notify {
            on: false,
            client_id: None,
        };
        let outcome = self.request(vec![Command::Notify(notify)]).await;
        if matches!(outcome, Ok(_) | Err(Error::Failed { .. })) {
            let mut state = self.shared.lock();
            state.registered = false;
            state.ids = None;
        }
        outcome.map(drop)
    }

    /// Closes the connection, after [`Self::unregister`] when the client is
    /// registered. It closes the transport in every case.
    ///
    /// # Errors
    ///
    /// The error of [`Self::unregister`], after the close.
    pub async fn close(&self) -> Result<(), Error> {
        let registered = self.shared.lock().registered;
        let outcome = if registered {
            self.unregister().await
        } else {
            Ok(())
        };
        self.shared.client.close();
        outcome
    }

    /// The events of the transport from now on, in order, after the client
    /// used them. A subscription made after the close gives none.
    #[must_use]
    pub fn subscribe(&self) -> Subscription {
        let (writer, events) = wait::queue();
        let mut state = self.shared.lock();
        if state.closed {
            drop(state);
            writer.close();
        } else {
            state.subscriptions.push(writer);
        }
        Subscription { events }
    }

    /// A request of the asynchronous `command`, whose replies must hold
    /// `done`: else [`Error::NoReply`] for the address of `command`.
    async fn request_done(&self, command: Command, done: Done) -> Result<Vec<Reply>, Error> {
        let address = command.address();
        let replies = self.request(vec![command]).await?;
        if replies.contains(&Reply::Done(done)) {
            Ok(replies)
        } else {
            Err(Error::NoReply { address })
        }
    }

    /// Waits for `wait`, and sends `command` as a plain send.
    async fn ask(&self, wait: Wait, command: Command) -> Result<Vec<Reply>, Error> {
        let call = self.shared.start(|core| core.wait(wait));
        self.send(&Packet::Message(command))?;
        call.outcome(self.limit).await
    }

    /// The next node id of the client, in its range.
    ///
    /// # Errors
    ///
    /// [`Error::NotRegistered`] before [`Self::register`].
    pub(crate) fn next_node_id(&self) -> Result<NodeId, Error> {
        self.with_ids(|ids| Ok(ids.next_node()))
    }

    /// The first id of a run of `count` free ids of `kind` in the range of
    /// the client, which it now holds.
    ///
    /// # Errors
    ///
    /// [`Error::NotRegistered`] before [`Self::register`], and
    /// [`Error::Exhausted`] when no such run is free.
    pub(crate) fn alloc_ids(&self, kind: IdKind, count: NonZeroU32) -> Result<i32, Error> {
        self.with_ids(|ids| ids.alloc(kind, count))
    }

    /// Gives back the run of ids of `kind` that starts at `first`. Nothing
    /// changes when the client holds no run that starts there.
    ///
    /// # Errors
    ///
    /// [`Error::NotRegistered`] before [`Self::register`].
    pub(crate) fn free_ids(&self, kind: IdKind, first: i32) -> Result<(), Error> {
        self.with_ids(|ids| {
            ids.free(kind, first);
            Ok(())
        })
    }

    fn with_ids<R>(&self, use_ids: impl FnOnce(&mut Ids) -> Result<R, Error>) -> Result<R, Error> {
        let mut state = self.shared.lock();
        let ids = state.ids.as_mut().ok_or(Error::NotRegistered)?;
        use_ids(ids)
    }
}

impl Subscription {
    /// The next event: every message, also one that a request takes, every
    /// [`Event::Error`], and [`Event::Closed`] as the last one. `None` after
    /// the last one, and once every handle of the client is dropped.
    pub async fn next(&mut self) -> Option<Event> {
        self.events.next().await
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Starts a request or a wait of the core.
    fn start(&self, begin: impl FnOnce(&mut Core) -> (RequestId, Vec<Effect>)) -> Call<'_> {
        let (sender, receiver) = wait::oneshot();
        let id = self.update(|state| {
            let (id, effects) = begin(&mut state.core);
            state.calls.insert(id, sender);
            (id, state.actions(effects))
        });
        Call {
            shared: self,
            id,
            receiver,
            settled: false,
        }
    }

    fn event(&self, event: Event) {
        match event {
            Event::Message(incoming) => self.apply(|core| core.receive(incoming)),
            Event::Error(error) => self.update(|state| {
                let readers = state.readers();
                ((), vec![Action::Publish(readers, Event::Error(error))])
            }),
            Event::Closed => self.update(|state| {
                state.closed = true;
                let effects = state.core.closed();
                let mut actions = state.actions(effects);
                actions.push(Action::End(mem::take(&mut state.subscriptions)));
                ((), actions)
            }),
        }
    }

    fn apply(&self, change: impl FnOnce(&mut Core) -> Vec<Effect>) {
        self.update(|state| {
            let effects = change(&mut state.core);
            ((), state.actions(effects))
        });
    }

    /// Changes the state under its lock, then runs the actions out of it.
    fn update<R>(&self, change: impl FnOnce(&mut State) -> (R, Vec<Action>)) -> R {
        // A transport may deliver inside `send`, as `ChannelClient` does: no
        // call into it, a waker or a subscription runs under the lock.
        let (result, actions) = change(&mut self.lock());
        self.run(actions);
        result
    }

    fn run(&self, actions: Vec<Action>) {
        let mut actions = VecDeque::from(actions);
        while let Some(action) = actions.pop_front() {
            match action {
                Action::Send(request, packet) => {
                    // A barrier that the transport refuses holds the queue
                    // until the timeout of a queued request sends another.
                    let (Err(error), Some(id)) = (self.client.send(&packet), request) else {
                        continue;
                    };
                    let more = {
                        let mut state = self.lock();
                        let effects = state.core.send_failed(id, error);
                        state.actions(effects)
                    };
                    for action in more.into_iter().rev() {
                        actions.push_front(action);
                    }
                }
                Action::Settle(sender, outcome) => sender.send(outcome),
                Action::Publish(writers, event) => {
                    for writer in &writers {
                        writer.push(event.clone());
                    }
                }
                Action::End(writers) => {
                    for writer in &writers {
                        writer.push(Event::Closed);
                        writer.close();
                    }
                }
            }
        }
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(PoisonError::into_inner);
        for writer in state.subscriptions.drain(..) {
            writer.close();
        }
    }
}

impl State {
    fn actions(&mut self, effects: Vec<Effect>) -> Vec<Action> {
        let mut actions = Vec::with_capacity(effects.len());
        for effect in effects {
            match effect {
                Effect::Send { request, packet } => actions.push(Action::Send(request, packet)),
                Effect::Settle(id, outcome) => {
                    if let Some(sender) = self.calls.remove(&id) {
                        actions.push(Action::Settle(sender, outcome));
                    }
                }
                Effect::Publish(incoming) => {
                    let readers = self.readers();
                    actions.push(Action::Publish(readers, Event::Message(incoming)));
                }
            }
        }
        actions
    }

    /// The subscriptions that are not dropped.
    fn readers(&mut self) -> Vec<Writer<Event>> {
        self.subscriptions.retain(Writer::is_read);
        self.subscriptions.clone()
    }
}

impl Call<'_> {
    async fn outcome(mut self, limit: Duration) -> Outcome {
        let shared = self.shared;
        let raced = wait::race(&mut self.receiver, || shared.timer.sleep(limit)).await;
        let outcome = match raced {
            Raced::Received(outcome) => outcome,
            Raced::Elapsed => {
                let id = self.id;
                shared.apply(|core| core.timed_out(id));
                // The timeout, or a result that settled the call first: on
                // another thread, its sender may not have sent it yet.
                (&mut self.receiver).await
            }
        };
        self.settled = true;
        // The state that holds the sender outlives the call: `None` does not
        // come.
        outcome.unwrap_or(Err(Error::Send(SendError::Closed)))
    }
}

impl Drop for Call<'_> {
    fn drop(&mut self) {
        if !self.settled {
            let id = self.id;
            self.shared.apply(|core| core.timed_out(id));
        }
    }
}

/// The client id and the login limit of a `/done /notify`.
fn notified(reply: &Reply) -> Option<(i32, Option<i32>)> {
    match reply {
        Reply::Done(Done::Notify {
            client_id,
            max_logins,
        }) => Some((*client_id, *max_logins)),
        _ => None,
    }
}

/// The client id of the `/fail` of `/notify` from a sender that scsynth
/// already registered.
fn already_registered(error: &Error) -> Option<i32> {
    match error {
        Error::Failed {
            address,
            message,
            value: Some(client_id),
        } if address == "/notify" && message.starts_with(ALREADY_REGISTERED) => Some(*client_id),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::future::{self, Future};
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use super::*;
    use crate::channel::ChannelClient;
    use crate::client::Incoming;
    use crate::timer::Sleep;

    /// A timer whose sleeps never end.
    struct Never;

    impl Timer for Never {
        fn sleep(&self, _limit: Duration) -> Sleep {
            Box::pin(future::pending())
        }
    }

    fn now<F: Future>(future: F) -> F::Output {
        match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("the future is pending"),
        }
    }

    /// A client of a scsynth that registers it with the id 1 under a limit
    /// of 4 logins.
    fn client() -> Scsynth {
        let (client, peer) = ChannelClient::pair();
        peer.on_packet(|peer, packet| {
            for command in packet.into_messages() {
                let reply = match command {
                    Command::Sync(id) => Reply::Synced(id),
                    Command::Notify(Notify { on: true, .. }) => Reply::Done(Done::Notify {
                        client_id: 1,
                        max_logins: Some(4),
                    }),
                    Command::Notify(_) => Reply::Done(Done::NotifyOff),
                    _ => continue,
                };
                peer.send(Incoming::Reply(reply));
            }
        });
        Scsynth::with_timer(client, Never, Options::default())
    }

    #[test]
    fn the_ids_need_a_registration_and_each_one_starts_fresh_ranges() {
        let scsynth = client();
        let one = NonZeroU32::MIN;
        let four = NonZeroU32::new(4).expect("a count over 0");
        assert_eq!(scsynth.next_node_id(), Err(Error::NotRegistered));
        assert_eq!(
            scsynth.alloc_ids(IdKind::Buffer, one),
            Err(Error::NotRegistered)
        );
        assert_eq!(
            scsynth.free_ids(IdKind::Buffer, 256),
            Err(Error::NotRegistered)
        );
        for _ in 0..2 {
            assert!(now(scsynth.register()).is_ok());
            assert_eq!(scsynth.next_node_id(), Ok(NodeId((1 << 26) + 1000)));
            assert_eq!(scsynth.alloc_ids(IdKind::Buffer, one), Ok(256));
            assert_eq!(scsynth.alloc_ids(IdKind::ControlBus, four), Ok(4096));
            assert_eq!(scsynth.alloc_ids(IdKind::AudioBus, four), Ok(268));
            assert_eq!(scsynth.free_ids(IdKind::AudioBus, 268), Ok(()));
            assert_eq!(scsynth.alloc_ids(IdKind::AudioBus, one), Ok(268));
            assert_eq!(now(scsynth.unregister()), Ok(()));
            assert_eq!(scsynth.next_node_id(), Err(Error::NotRegistered));
        }
    }
}
