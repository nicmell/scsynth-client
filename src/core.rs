use std::collections::VecDeque;

use scsynth_osc::reply::command_replies::{Done, Fail};
use scsynth_osc::{Bundle, Command, NodeId, Packet, RawArg, Reply, SyncId, Timetag};

use crate::client::{Incoming, SendError};
use crate::error::Error;

/// The name of a request or a wait of the [`Core`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RequestId(u64);

/// A reply that the [`Core`] waits for outside its queue of requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Wait {
    /// A `/status.reply`.
    Status,
    /// A `/version.reply`.
    Version,
    /// A `/done /quit`.
    Quit,
    /// The `/n_end` of the node.
    End(NodeId),
}

/// What the caller of the [`Core`] does, in order.
#[derive(Debug, PartialEq)]
pub(crate) enum Effect {
    /// A packet for the transport. `request` names the request that it
    /// carries; a barrier has none.
    Send {
        request: Option<RequestId>,
        packet: Packet<Command>,
    },
    /// The result of a request or a wait.
    Settle(RequestId, Result<Vec<Reply>, Error>),
    /// A message for every subscription.
    Publish(Incoming),
}

/// The requests to scsynth and their replies, one request in flight at a
/// time: a state machine with no I/O and no clock.
#[derive(Default)]
pub(crate) struct Core {
    last_id: u64,
    /// The last sync id that the core sent: 0 before the first.
    last_sync: i32,
    queue: VecDeque<Queued>,
    flight: Option<Flight>,
    /// The sync ids of the pending barrier, any of which clears it: empty
    /// when no barrier is pending.
    barriers: Vec<SyncId>,
    waits: Vec<(RequestId, Wait)>,
    closed: bool,
}

/// A request that waits for its turn.
struct Queued {
    id: RequestId,
    commands: Vec<Command>,
}

/// The request in flight.
struct Flight {
    id: RequestId,
    sync: SyncId,
    /// The addresses of its commands and of the commands of their
    /// completions.
    addresses: Vec<&'static str>,
    replies: Vec<Reply>,
    /// Its first `/fail`.
    failure: Option<Fail>,
}

impl Core {
    /// A core with no request, open.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Queues a request of `commands`, which leaves at once when no request
    /// is in flight and no barrier is pending.
    pub(crate) fn request(&mut self, commands: Vec<Command>) -> (RequestId, Vec<Effect>) {
        let id = self.next_id();
        if self.closed {
            return (id, vec![settle_closed(id)]);
        }
        self.queue.push_back(Queued { id, commands });
        let mut effects = Vec::new();
        self.launch(&mut effects);
        (id, effects)
    }

    /// Registers a wait for the reply `what`. It sends nothing.
    pub(crate) fn wait(&mut self, what: Wait) -> (RequestId, Vec<Effect>) {
        let id = self.next_id();
        if self.closed {
            return (id, vec![settle_closed(id)]);
        }
        self.waits.push((id, what));
        (id, Vec::new())
    }

    /// Takes one message of scsynth, and publishes it.
    pub(crate) fn receive(&mut self, incoming: Incoming) -> Vec<Effect> {
        let mut effects = Vec::new();
        if let Incoming::Reply(reply) = &incoming {
            self.answer_waits(reply, &mut effects);
            match reply {
                Reply::Synced(sync) => self.synced(*sync, &mut effects),
                Reply::Fail(fail) => {
                    if let Some(flight) = self.flight.as_mut().filter(|flight| {
                        flight.failure.is_none()
                            && flight
                                .addresses
                                .iter()
                                .any(|address| *address == fail.command)
                    }) {
                        flight.failure = Some(fail.clone());
                    }
                }
                reply => {
                    if let Some(flight) = &mut self.flight {
                        flight.replies.push(reply.clone());
                    }
                }
            }
        }
        effects.push(Effect::Publish(incoming));
        effects
    }

    /// Settles the request or the wait `id` with [`Error::Timeout`]. A
    /// request in flight leaves a barrier.
    pub(crate) fn timed_out(&mut self, id: RequestId) -> Vec<Effect> {
        let mut effects = Vec::new();
        if let Some(flight) = self.flight.take_if(|flight| flight.id == id) {
            effects.push(Effect::Settle(id, Err(Error::Timeout)));
            self.barriers.push(flight.sync);
            self.barrier(&mut effects);
        } else if let Some(index) = self.queue.iter().position(|queued| queued.id == id) {
            self.queue.remove(index);
            effects.push(Effect::Settle(id, Err(Error::Timeout)));
            if !self.barriers.is_empty() {
                self.barrier(&mut effects);
            }
        } else if let Some(index) = self.waits.iter().position(|(wait, _)| *wait == id) {
            self.waits.remove(index);
            effects.push(Effect::Settle(id, Err(Error::Timeout)));
        }
        effects
    }

    /// Settles the request in flight `id` with the error of the transport.
    pub(crate) fn send_failed(&mut self, id: RequestId, error: SendError) -> Vec<Effect> {
        let mut effects = Vec::new();
        if let Some(flight) = self.flight.take_if(|flight| flight.id == id) {
            effects.push(Effect::Settle(flight.id, Err(Error::Send(error))));
            self.launch(&mut effects);
        }
        effects
    }

    /// Settles every request and every wait with [`SendError::Closed`], and
    /// refuses those that come after.
    pub(crate) fn closed(&mut self) -> Vec<Effect> {
        self.closed = true;
        self.barriers.clear();
        let flight = self.flight.take().map(|flight| flight.id);
        let queued = self.queue.drain(..).map(|queued| queued.id);
        let waits = self.waits.drain(..).map(|(id, _)| id);
        flight
            .into_iter()
            .chain(queued)
            .chain(waits)
            .map(settle_closed)
            .collect()
    }

    fn next_id(&mut self) -> RequestId {
        self.last_id += 1;
        RequestId(self.last_id)
    }

    fn next_sync(&mut self) -> SyncId {
        self.last_sync = if self.last_sync == i32::MAX {
            1
        } else {
            self.last_sync + 1
        };
        SyncId(self.last_sync)
    }

    /// Sends the next request, when no request is in flight and no barrier
    /// is pending.
    fn launch(&mut self, effects: &mut Vec<Effect>) {
        if self.flight.is_some() || !self.barriers.is_empty() {
            return;
        }
        let Some(Queued { id, commands }) = self.queue.pop_front() else {
            return;
        };
        let sync = self.next_sync();
        let mut addresses = Vec::new();
        for command in &commands {
            collect_addresses(command, &mut addresses);
        }
        let packets = commands
            .into_iter()
            .chain([Command::Sync(sync)])
            .map(Packet::Message)
            .collect();
        effects.push(Effect::Send {
            request: Some(id),
            packet: Packet::Bundle(Bundle {
                time: Timetag::IMMEDIATELY,
                packets,
            }),
        });
        self.flight = Some(Flight {
            id,
            sync,
            addresses,
            replies: Vec::new(),
            failure: None,
        });
    }

    /// Sends one more barrier: a lone `/sync`.
    fn barrier(&mut self, effects: &mut Vec<Effect>) {
        let sync = self.next_sync();
        self.barriers.push(sync);
        effects.push(Effect::Send {
            request: None,
            packet: Packet::Message(Command::Sync(sync)),
        });
    }

    fn synced(&mut self, sync: SyncId, effects: &mut Vec<Effect>) {
        if let Some(flight) = self.flight.take_if(|flight| flight.sync == sync) {
            effects.push(Effect::Settle(flight.id, flight.result()));
        } else if self.barriers.contains(&sync) {
            self.barriers.clear();
        } else {
            return;
        }
        self.launch(effects);
    }

    fn answer_waits(&mut self, reply: &Reply, effects: &mut Vec<Effect>) {
        self.waits.retain(|&(id, wait)| {
            let answered = wait.answered_by(reply);
            if answered {
                effects.push(Effect::Settle(id, Ok(vec![reply.clone()])));
            }
            !answered
        });
    }
}

impl Wait {
    fn answered_by(self, reply: &Reply) -> bool {
        match (self, reply) {
            (Self::Status, Reply::StatusReply(_))
            | (Self::Version, Reply::VersionReply(_))
            | (Self::Quit, Reply::Done(Done::Quit)) => true,
            (Self::End(node), Reply::NEnd(state)) => state.node == node,
            _ => false,
        }
    }
}

impl Flight {
    fn result(self) -> Result<Vec<Reply>, Error> {
        match self.failure {
            Some(Fail {
                command,
                message,
                value,
            }) => Err(Error::Failed {
                address: command,
                message,
                value,
            }),
            None => Ok(self.replies),
        }
    }
}

fn settle_closed(id: RequestId) -> Effect {
    Effect::Settle(id, Err(Error::Send(SendError::Closed)))
}

/// Pushes the address of `command`, then those of the commands of its
/// completion, at any depth.
fn collect_addresses(command: &Command, addresses: &mut Vec<&'static str>) {
    addresses.push(command.address());
    // scsynth-osc 0.2.1 has no accessor for the completion of a command: any
    // blob that decodes as a packet of commands counts as one, a blob of
    // `/cmd` or `/u_cmd` too. The data of `/d_recv` does not decode.
    let completions = command
        .to_raw()
        .args
        .into_iter()
        .filter_map(|arg| match arg {
            RawArg::Blob(bytes) => Packet::<Command>::decode(&bytes).ok(),
            _ => None,
        });
    for inner in completions.flat_map(Packet::into_messages) {
        collect_addresses(&inner, addresses);
    }
}

#[cfg(test)]
mod tests {
    use scsynth_osc::command::buffer::{BAllocRead, BQuery};
    use scsynth_osc::command::synth::SNew;
    use scsynth_osc::command::synth_def::DRecv;
    use scsynth_osc::command::top_level::Notify;
    use scsynth_osc::reply::node_notification::{NodeKind, NodeState};
    use scsynth_osc::reply::top_level::{StatusReply, VersionReply};
    use scsynth_osc::{AddAction, BufferId, Completion, NewNodeId, RawMessage};

    use super::*;

    const NOTHING: [Effect; 0] = [];

    fn s_new(def: &str, node: i32) -> Command {
        Command::SNew(SNew {
            def: def.to_owned(),
            id: NewNodeId::new(node).expect("a node id"),
            action: AddAction::Head,
            target: NodeId::ROOT,
            controls: Vec::new(),
        })
    }

    fn sent(id: RequestId, commands: Vec<Command>, sync: i32) -> Effect {
        let mut packets: Vec<_> = commands.into_iter().map(Packet::Message).collect();
        packets.push(Packet::Message(Command::Sync(SyncId(sync))));
        Effect::Send {
            request: Some(id),
            packet: Packet::Bundle(Bundle {
                time: Timetag::IMMEDIATELY,
                packets,
            }),
        }
    }

    fn barrier(sync: i32) -> Effect {
        Effect::Send {
            request: None,
            packet: Packet::Message(Command::Sync(SyncId(sync))),
        }
    }

    fn synced(sync: i32) -> Reply {
        Reply::Synced(SyncId(sync))
    }

    fn fail(command: &str, message: &str, value: Option<i32>) -> Reply {
        Reply::Fail(Fail {
            command: command.to_owned(),
            message: message.to_owned(),
            value,
        })
    }

    fn failed(address: &str, message: &str, value: Option<i32>) -> Result<Vec<Reply>, Error> {
        Err(Error::Failed {
            address: address.to_owned(),
            message: message.to_owned(),
            value,
        })
    }

    fn node(id: i32) -> NodeState {
        NodeState {
            node: NodeId(id),
            parent: Some(NodeId::ROOT),
            prev: None,
            next: None,
            kind: NodeKind::Synth,
        }
    }

    fn status() -> Reply {
        Reply::StatusReply(StatusReply {
            ugens: 0,
            synths: 0,
            groups: 1,
            synthdefs: 0,
            avg_cpu: 0.0,
            peak_cpu: 0.0,
            nominal_sample_rate: 48_000.0,
            actual_sample_rate: 48_000.0,
        })
    }

    fn version() -> Reply {
        Reply::VersionReply(VersionReply {
            program: "scsynth".to_owned(),
            major: 3,
            minor: 14,
            patch: ".1".to_owned(),
            branch: "Version-3.14.1".to_owned(),
            commit: "na".to_owned(),
        })
    }

    fn buffer_read(completion: Completion) -> BAllocRead {
        BAllocRead {
            buffer: BufferId::new(3).expect("a buffer"),
            path: "missing.wav".to_owned(),
            file_frame: 0,
            frames: 0,
            completion: Some(completion),
        }
    }

    fn query() -> Command {
        Command::BQuery(BQuery {
            buffers: vec![BufferId::new(3).expect("a buffer")],
        })
    }

    /// The effects of the replies, one after the other, each call checked to
    /// end in the publish of its reply, which is left out.
    fn replay(core: &mut Core, replies: impl IntoIterator<Item = Reply>) -> Vec<Effect> {
        let mut all = Vec::new();
        for reply in replies {
            let mut effects = core.receive(Incoming::Reply(reply.clone()));
            assert_eq!(effects.pop(), Some(Effect::Publish(Incoming::Reply(reply))));
            all.extend(effects);
        }
        all
    }

    #[test]
    fn a_request_leaves_as_one_immediate_bundle_that_ends_in_its_sync() {
        let mut core = Core::new();
        let commands = vec![s_new("sine", 1000), Command::Status];
        let (first, effects) = core.request(commands.clone());
        assert_eq!(effects, [sent(first, commands, 1)]);
        let (empty, effects) = core.request(Vec::new());
        assert_eq!(effects, NOTHING);
        assert_eq!(
            replay(&mut core, [synced(1)]),
            [
                Effect::Settle(first, Ok(Vec::new())),
                sent(empty, Vec::new(), 2)
            ]
        );
    }

    #[test]
    fn a_flight_keeps_its_replies_in_order_but_synced_and_fail() {
        let mut core = Core::new();
        let (id, _) = core.request(vec![s_new("sine", 1000), s_new("sine", 1001)]);
        let replies = [
            Reply::NGo(node(1000)),
            synced(9),
            fail("/n_free", "Node 9999 not found", None),
            Reply::NGo(node(1001)),
        ];
        assert_eq!(replay(&mut core, replies), NOTHING);
        assert_eq!(
            replay(&mut core, [synced(1)]),
            [Effect::Settle(
                id,
                Ok(vec![Reply::NGo(node(1000)), Reply::NGo(node(1001))])
            )]
        );
    }

    #[test]
    fn the_commands_of_a_completion_count_at_any_depth() {
        let mut core = Core::new();
        let read = buffer_read(Completion::Bundle {
            time: Timetag::IMMEDIATELY,
            commands: vec![query()],
        });
        let define = Command::DRecv(DRecv {
            data: b"no definition".to_vec(),
            completion: Some(Completion::Message(Box::new(Command::BAllocRead(read)))),
        });
        let (id, _) = core.request(vec![define]);
        let replies = [fail("/b_query", "index out of range", None), synced(1)];
        assert_eq!(
            replay(&mut core, replies),
            [Effect::Settle(
                id,
                failed("/b_query", "index out of range", None)
            )]
        );
    }

    #[test]
    fn the_synced_of_the_flight_settles_it_with_its_first_kept_fail() {
        let mut core = Core::new();
        let (first, _) = core.request(vec![s_new("a", 1000), s_new("b", 1000)]);
        let (second, _) = core.request(Vec::new());
        let replies = [
            fail("/s_new", "SynthDef not found", None),
            fail("/s_new", "duplicate node ID", None),
            synced(1),
        ];
        assert_eq!(
            replay(&mut core, replies),
            [
                Effect::Settle(first, failed("/s_new", "SynthDef not found", None)),
                sent(second, Vec::new(), 2)
            ]
        );
    }

    #[test]
    fn a_timed_out_flight_leaves_a_barrier_that_holds_the_queue() {
        let mut core = Core::new();
        let (first, _) = core.request(vec![s_new("sine", 1000)]);
        let (second, _) = core.request(Vec::new());
        assert_eq!(
            core.timed_out(first),
            [Effect::Settle(first, Err(Error::Timeout)), barrier(2)]
        );
        let (third, effects) = core.request(Vec::new());
        assert_eq!(effects, NOTHING);
        assert_eq!(
            replay(&mut core, [synced(2)]),
            [sent(second, Vec::new(), 3)]
        );
        assert_eq!(replay(&mut core, [synced(1)]), NOTHING);
        assert_eq!(
            replay(&mut core, [synced(3)]),
            [
                Effect::Settle(second, Ok(Vec::new())),
                sent(third, Vec::new(), 4)
            ]
        );
    }

    #[test]
    fn a_timed_out_queued_request_leaves_and_renews_a_pending_barrier() {
        let mut core = Core::new();
        let (first, _) = core.request(Vec::new());
        let (second, _) = core.request(Vec::new());
        let (third, _) = core.request(Vec::new());
        let (fourth, _) = core.request(Vec::new());
        assert_eq!(
            core.timed_out(second),
            [Effect::Settle(second, Err(Error::Timeout))]
        );
        assert_eq!(
            core.timed_out(first),
            [Effect::Settle(first, Err(Error::Timeout)), barrier(2)]
        );
        assert_eq!(
            core.timed_out(third),
            [Effect::Settle(third, Err(Error::Timeout)), barrier(3)]
        );
        assert_eq!(
            replay(&mut core, [synced(2)]),
            [sent(fourth, Vec::new(), 4)]
        );
        assert_eq!(replay(&mut core, [synced(3)]), NOTHING);
    }

    #[test]
    fn a_timed_out_wait_settles_and_a_settled_id_times_out_to_nothing() {
        let mut core = Core::new();
        let (late, effects) = core.wait(Wait::Status);
        assert_eq!(effects, NOTHING);
        let (answered, _) = core.wait(Wait::Status);
        assert_eq!(
            core.timed_out(late),
            [Effect::Settle(late, Err(Error::Timeout))]
        );
        assert_eq!(core.timed_out(late), NOTHING);
        assert_eq!(
            replay(&mut core, [status()]),
            [Effect::Settle(answered, Ok(vec![status()]))]
        );
        assert_eq!(core.timed_out(answered), NOTHING);
        let (request, _) = core.request(Vec::new());
        assert_eq!(
            replay(&mut core, [synced(1)]),
            [Effect::Settle(request, Ok(Vec::new()))]
        );
        assert_eq!(core.timed_out(request), NOTHING);
    }

    #[test]
    fn a_reply_settles_every_wait_for_it_outside_the_queue() {
        let mut core = Core::new();
        let (request, _) = core.request(vec![Command::Status]);
        let (first, effects) = core.wait(Wait::Status);
        assert_eq!(effects, NOTHING);
        let (second, _) = core.wait(Wait::Status);
        let (version_wait, _) = core.wait(Wait::Version);
        let (first_node, _) = core.wait(Wait::End(NodeId(1000)));
        let (second_node, _) = core.wait(Wait::End(NodeId(1001)));
        assert_eq!(
            replay(&mut core, [status()]),
            [
                Effect::Settle(first, Ok(vec![status()])),
                Effect::Settle(second, Ok(vec![status()]))
            ]
        );
        assert_eq!(
            replay(&mut core, [Reply::NEnd(node(1001)), version()]),
            [
                Effect::Settle(second_node, Ok(vec![Reply::NEnd(node(1001))])),
                Effect::Settle(version_wait, Ok(vec![version()]))
            ]
        );
        assert_eq!(replay(&mut core, [status()]), NOTHING);
        let kept = vec![status(), Reply::NEnd(node(1001)), version(), status()];
        assert_eq!(
            replay(&mut core, [synced(1)]),
            [Effect::Settle(request, Ok(kept))]
        );
        assert_eq!(
            replay(&mut core, [Reply::NEnd(node(1000))]),
            [Effect::Settle(
                first_node,
                Ok(vec![Reply::NEnd(node(1000))])
            )]
        );
    }

    #[test]
    fn a_done_quit_settles_every_wait_for_quit_and_no_other() {
        let mut core = Core::new();
        let (first, _) = core.wait(Wait::Quit);
        let (second, _) = core.wait(Wait::Quit);
        let (status_wait, _) = core.wait(Wait::Status);
        let quit = Reply::Done(Done::Quit);
        assert_eq!(replay(&mut core, [Reply::Done(Done::NotifyOff)]), NOTHING);
        assert_eq!(
            replay(&mut core, [quit.clone()]),
            [
                Effect::Settle(first, Ok(vec![quit.clone()])),
                Effect::Settle(second, Ok(vec![quit]))
            ]
        );
        assert_eq!(
            core.timed_out(status_wait),
            [Effect::Settle(status_wait, Err(Error::Timeout))]
        );
    }

    #[test]
    fn a_stray_synced_or_fail_settles_nothing() {
        let mut core = Core::new();
        let replies = [synced(1), fail("/s_new", "SynthDef not found", None)];
        assert_eq!(replay(&mut core, replies), NOTHING);
        let (id, _) = core.request(vec![s_new("sine", 1000)]);
        assert_eq!(replay(&mut core, [synced(7)]), NOTHING);
        assert_eq!(
            replay(&mut core, [synced(1)]),
            [Effect::Settle(id, Ok(Vec::new()))]
        );
    }

    #[test]
    fn a_failed_send_settles_the_flight_and_the_next_leaves_without_a_barrier() {
        let mut core = Core::new();
        let (first, _) = core.request(vec![s_new("sine", 1000)]);
        let (second, _) = core.request(vec![s_new("sine", 1001)]);
        assert_eq!(core.send_failed(second, SendError::Closed), NOTHING);
        let too_large = SendError::TooLarge {
            size: 9000,
            max: 8192,
        };
        assert_eq!(
            core.send_failed(first, too_large.clone()),
            [
                Effect::Settle(first, Err(Error::Send(too_large.clone()))),
                sent(second, vec![s_new("sine", 1001)], 2)
            ]
        );
        assert_eq!(core.send_failed(first, too_large), NOTHING);
    }

    #[test]
    fn a_close_settles_everything_and_refuses_what_comes_after() {
        let mut core = Core::new();
        let (flight, _) = core.request(vec![s_new("sine", 1000)]);
        let (queued, _) = core.request(Vec::new());
        let (wait, _) = core.wait(Wait::Version);
        let closed = || Err(Error::Send(SendError::Closed));
        assert_eq!(
            core.closed(),
            [
                Effect::Settle(flight, closed()),
                Effect::Settle(queued, closed()),
                Effect::Settle(wait, closed())
            ]
        );
        let (request, effects) = core.request(Vec::new());
        assert_eq!(effects, [Effect::Settle(request, closed())]);
        let (wait, effects) = core.wait(Wait::Status);
        assert_eq!(effects, [Effect::Settle(wait, closed())]);
        assert_eq!(replay(&mut core, [synced(1), status()]), NOTHING);
        assert_eq!(core.closed(), NOTHING);
    }

    #[test]
    fn every_message_is_published_after_the_settles_and_the_sends() {
        let mut core = Core::new();
        let (first, _) = core.request(Vec::new());
        let (second, _) = core.request(Vec::new());
        let (wait, _) = core.wait(Wait::End(NodeId(1000)));
        let first_synced = Incoming::Reply(synced(1));
        assert_eq!(
            core.receive(first_synced.clone()),
            [
                Effect::Settle(first, Ok(Vec::new())),
                sent(second, Vec::new(), 2),
                Effect::Publish(first_synced)
            ]
        );
        let end = Incoming::Reply(Reply::NEnd(node(1000)));
        assert_eq!(
            core.receive(end.clone()),
            [
                Effect::Settle(wait, Ok(vec![Reply::NEnd(node(1000))])),
                Effect::Publish(end)
            ]
        );
        let other = Incoming::Other(RawMessage {
            address: "/reply".to_owned(),
            args: vec![RawArg::Int(1)],
        });
        assert_eq!(core.receive(other.clone()), [Effect::Publish(other)]);
        assert_eq!(
            replay(&mut core, [synced(2)]),
            [Effect::Settle(second, Ok(vec![Reply::NEnd(node(1000))]))]
        );
    }

    // The orders that scsynth 3.14.1 gave.

    #[test]
    fn a_missing_definition_fails_before_the_synced() {
        let mut core = Core::new();
        let (id, _) = core.request(vec![s_new("missing", 1000)]);
        let replies = [fail("/s_new", "SynthDef not found", None), synced(1)];
        assert_eq!(
            replay(&mut core, replies),
            [Effect::Settle(
                id,
                failed("/s_new", "SynthDef not found", None)
            )]
        );
    }

    #[test]
    fn a_registered_client_gets_n_go_before_the_synced() {
        let mut core = Core {
            last_sync: 1,
            ..Core::new()
        };
        let (id, effects) = core.request(vec![s_new("sine", 1000)]);
        assert_eq!(effects, [sent(id, vec![s_new("sine", 1000)], 2)]);
        assert_eq!(
            replay(&mut core, [Reply::NGo(node(1000)), synced(2)]),
            [Effect::Settle(id, Ok(vec![Reply::NGo(node(1000))]))]
        );
    }

    #[test]
    fn a_client_that_is_not_registered_gets_the_synced_alone() {
        let mut core = Core::new();
        let (id, _) = core.request(vec![s_new("sine", 1000)]);
        assert_eq!(
            replay(&mut core, [synced(1)]),
            [Effect::Settle(id, Ok(Vec::new()))]
        );
    }

    #[test]
    fn a_second_request_leaves_after_the_synced_of_the_first() {
        let mut core = Core::new();
        let (first, effects) = core.request(vec![s_new("sine", 1000)]);
        assert_eq!(effects, [sent(first, vec![s_new("sine", 1000)], 1)]);
        let (second, effects) = core.request(vec![s_new("missing", 1001)]);
        assert_eq!(effects, NOTHING);
        assert_eq!(
            replay(&mut core, [synced(1)]),
            [
                Effect::Settle(first, Ok(Vec::new())),
                sent(second, vec![s_new("missing", 1001)], 2)
            ]
        );
        let replies = [fail("/s_new", "SynthDef not found", None), synced(2)];
        assert_eq!(
            replay(&mut core, replies),
            [Effect::Settle(
                second,
                failed("/s_new", "SynthDef not found", None)
            )]
        );
    }

    #[test]
    fn notify_on_a_registered_connection_fails_with_the_client_id() {
        let mut core = Core::new();
        let register = Command::Notify(Notify {
            on: true,
            client_id: None,
        });
        let (id, _) = core.request(vec![register]);
        let message = "notify: already registered\n";
        assert_eq!(
            replay(&mut core, [fail("/notify", message, Some(0)), synced(1)]),
            [Effect::Settle(id, failed("/notify", message, Some(0)))]
        );
    }

    #[test]
    fn d_recv_of_bytes_that_are_no_definition_is_done() {
        let mut core = Core::new();
        let define = Command::DRecv(DRecv {
            data: b"no definition".to_vec(),
            completion: None,
        });
        let (id, _) = core.request(vec![define]);
        assert_eq!(
            replay(&mut core, [Reply::Done(Done::DRecv), synced(1)]),
            [Effect::Settle(id, Ok(vec![Reply::Done(Done::DRecv)]))]
        );
    }

    #[test]
    fn a_failed_b_alloc_read_names_its_buffer_and_its_completion_counts() {
        let mut core = Core::new();
        let read = Command::BAllocRead(buffer_read(Completion::Message(Box::new(query()))));
        let message = "File 'missing.wav' could not be opened: System error\n";
        let (first, _) = core.request(vec![read.clone()]);
        assert_eq!(
            replay(
                &mut core,
                [fail("/b_allocRead", message, Some(3)), synced(1)]
            ),
            [Effect::Settle(
                first,
                failed("/b_allocRead", message, Some(3))
            )]
        );
        let (second, _) = core.request(vec![read]);
        let replies = [fail("/b_query", "index out of range", None), synced(2)];
        assert_eq!(
            replay(&mut core, replies),
            [Effect::Settle(
                second,
                failed("/b_query", "index out of range", None)
            )]
        );
    }

    #[test]
    fn a_fail_of_a_command_that_the_flight_does_not_hold_is_published_alone() {
        let mut core = Core::new();
        let (id, _) = core.request(vec![s_new("sine", 1000)]);
        let stray = Incoming::Reply(fail("/n_set", "Node 1000 not found", None));
        assert_eq!(core.receive(stray.clone()), [Effect::Publish(stray)]);
        assert_eq!(
            replay(&mut core, [synced(1)]),
            [Effect::Settle(id, Ok(Vec::new()))]
        );
    }

    #[test]
    fn late_replies_after_a_timeout_land_on_no_one_until_a_barrier_id_comes() {
        let mut core = Core::new();
        let (late, _) = core.request(vec![s_new("sine", 1000)]);
        let (queued, _) = core.request(vec![s_new("sine", 1001)]);
        assert_eq!(
            core.timed_out(late),
            [Effect::Settle(late, Err(Error::Timeout)), barrier(2)]
        );
        let replies = [
            Reply::NGo(node(1000)),
            fail("/s_new", "duplicate node ID", None),
        ];
        assert_eq!(replay(&mut core, replies), NOTHING);
        assert_eq!(
            replay(&mut core, [synced(1)]),
            [sent(queued, vec![s_new("sine", 1001)], 3)]
        );
        assert_eq!(replay(&mut core, [synced(2)]), NOTHING);
        assert_eq!(
            replay(&mut core, [Reply::NGo(node(1001)), synced(3)]),
            [Effect::Settle(queued, Ok(vec![Reply::NGo(node(1001))]))]
        );
    }

    #[test]
    fn sync_ids_wrap_from_i32_max_to_1() {
        let mut core = Core {
            last_sync: i32::MAX - 1,
            ..Core::new()
        };
        let (first, effects) = core.request(Vec::new());
        assert_eq!(effects, [sent(first, Vec::new(), i32::MAX)]);
        let (second, _) = core.request(Vec::new());
        assert_eq!(
            replay(&mut core, [synced(i32::MAX)]),
            [
                Effect::Settle(first, Ok(Vec::new())),
                sent(second, Vec::new(), 1)
            ]
        );
    }
}
