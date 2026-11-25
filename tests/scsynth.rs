//! The tests on a real scsynth: each one proves a fact of scsynth 3.14.1
//! that the client relies on, or the client against it. They are ignored by
//! default, and pass at once without `SCSYNTH_ADDR`. The README says how to
//! run them, under Tests.
//!
//! Two tests that run at once must not share a client id, a node, a buffer
//! or a bus: each test that registers asks for client ids of its own, and
//! the nodes made with no registration are in the range of the client id 30,
//! which no test registers.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use scsynth_client::scsynth_osc::command::buffer::BAlloc;
use scsynth_client::scsynth_osc::command::group::{GNew, NewGroup};
use scsynth_client::scsynth_osc::command::node::{ControlMapping, NFree};
use scsynth_client::scsynth_osc::command::synth::SNew;
use scsynth_client::scsynth_osc::command::synth_def::DRecv;
use scsynth_client::scsynth_osc::reply::buffer::BufferInfo;
use scsynth_client::scsynth_osc::reply::command_replies::{BufferCommand, Done};
use scsynth_client::scsynth_osc::reply::group::{
    GQueryTreeReply, TreeControl, TreeGroup, TreeNode, TreeSynth, TreeValue,
};
use scsynth_client::scsynth_osc::reply::node_notification::NodeKind;
use scsynth_client::scsynth_osc::{
    AddAction, BufferId, Bundle, BusMapping, Command, Control, ControlSetting, NewNodeId, NodeId,
    Packet, Reply, SyncId, Timetag,
};
use scsynth_client::scsynth_synthdef::{Arg, SynthDef, SynthDefBuilder};
use scsynth_client::{
    Error, Event, Incoming, Options, OscClient, Registration, Scsynth, SendError, Subscription,
    Target, TcpClient, UdpClient,
};
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::time::timeout;

/// The longest that a test waits for an event of a transport.
const LIMIT: Duration = Duration::from_secs(5);

/// The first client id of each test that registers: the id over UDP, and
/// the next one over TCP.
const MISSING_DEFINITION: i32 = 1;
const N_GO: i32 = 3;
const ALREADY_REGISTERED: i32 = 5;
const READ_BUFFER: i32 = 7;
const NOTIFICATIONS: i32 = 9;
const VALUES: i32 = 11;
const NO_FRAMES: i32 = 13;
const SAMPLES: i32 = 15;
const METHOD_TABLE: i32 = 17;

/// The first node id of each test that makes nodes with no registration:
/// ten for each, in the range of the client id 30.
const NOT_REGISTERED_NODES: i32 = (30 << 26) + 1000;
const BACK_TO_BACK_NODES: i32 = (30 << 26) + 1010;
const TOGETHER_NODES: i32 = (30 << 26) + 1020;
const QUERY_NODES: i32 = (30 << 26) + 1030;

/// A node id that no test makes.
const MISSING_NODE: i32 = (30 << 26) + 999;

/// The synth definitions of the tests. None has an output, so no test makes
/// a sound.
const CONTROL: &str = "scsynth-client-control";
const LINE: &str = "scsynth-client-line";
/// A synth definition that no test sends.
const MISSING: &str = "scsynth-client-missing";

/// The address of the scsynth of the tests, from `SCSYNTH_ADDR`.
fn address() -> Option<String> {
    env::var("SCSYNTH_ADDR").ok()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Transport {
    Udp,
    Tcp,
}

const TRANSPORTS: [Transport; 2] = [Transport::Udp, Transport::Tcp];

impl Transport {
    /// The offset of the ids that a test takes over this transport.
    fn offset(self) -> i32 {
        match self {
            Self::Udp => 0,
            Self::Tcp => 1,
        }
    }

    async fn connect(self, addr: &str) -> Box<dyn OscClient> {
        match self {
            Self::Udp => Box::new(UdpClient::connect(addr).await.expect("UDP connects")),
            Self::Tcp => Box::new(TcpClient::connect(addr).await.expect("TCP connects")),
        }
    }
}

/// A client of the scsynth at `addr`, not registered.
async fn client(addr: &str, transport: Transport, options: Options) -> Scsynth {
    match transport {
        Transport::Udp => {
            let udp = UdpClient::connect(addr).await.expect("UDP connects");
            Scsynth::new(udp, options)
        }
        Transport::Tcp => {
            let tcp = TcpClient::connect(addr).await.expect("TCP connects");
            Scsynth::new(tcp, options)
        }
    }
}

/// A client registered with the client id `first` over UDP, and the next
/// one over TCP.
async fn registered(addr: &str, transport: Transport, first: i32) -> (Scsynth, Registration) {
    let client_id = first + transport.offset();
    let options = Options {
        client_id: Some(client_id),
        ..Options::default()
    };
    let scsynth = client(addr, transport, options).await;
    let registration = scsynth.register().await.expect("the client registers");
    assert_eq!(
        registration.client_id, client_id,
        "scsynth gives another client id when the one asked for is taken: restart it after a failed run"
    );
    (scsynth, registration)
}

/// The first id of the share of the client of `count` ids.
fn first_of_share(registration: Registration, count: i32) -> i32 {
    registration.client_id * (count / registration.max_logins)
}

/// A transport with no client around it: each packet leaves as it is.
struct Raw {
    client: Box<dyn OscClient>,
    events: UnboundedReceiver<Event>,
}

impl Raw {
    async fn connect(addr: &str, transport: Transport) -> Self {
        Self::over(transport.connect(addr).await)
    }

    fn over(client: Box<dyn OscClient>) -> Self {
        let (sender, events) = mpsc::unbounded_channel();
        client.listen(Box::new(move |event| {
            let _ = sender.send(event);
        }));
        Self { client, events }
    }

    fn send(&self, packet: &Packet<Command>) {
        self.client.send(packet).expect("the transport takes it");
    }

    async fn next(&mut self) -> Event {
        timeout(LIMIT, self.events.recv())
            .await
            .expect("an event within the limit")
            .expect("the transport is open")
    }

    /// The replies up to the first that `last` takes, with it.
    async fn until(&mut self, last: impl Fn(&Reply) -> bool) -> Vec<Reply> {
        let mut replies = Vec::new();
        loop {
            match self.next().await {
                Event::Message(Incoming::Reply(reply)) => {
                    let end = last(&reply);
                    replies.push(reply);
                    if end {
                        return replies;
                    }
                }
                Event::Message(Incoming::Other(_)) => {}
                event => panic!("an unexpected {event:?}"),
            }
        }
    }

    fn close(&self) {
        self.client.close();
    }
}

/// The replies of `events` up to the first that `last` takes, with it.
async fn until(events: &mut Subscription, last: impl Fn(&Reply) -> bool) -> Vec<Reply> {
    let mut replies = Vec::new();
    loop {
        let event = timeout(LIMIT, events.next())
            .await
            .expect("an event within the limit");
        match event {
            Some(Event::Message(Incoming::Reply(reply))) => {
                let end = last(&reply);
                replies.push(reply);
                if end {
                    return replies;
                }
            }
            Some(Event::Message(Incoming::Other(_))) => {}
            event => panic!("an unexpected {event:?}"),
        }
    }
}

/// `commands`, then `/sync sync`, in one immediate bundle.
fn bundle(commands: Vec<Command>, sync: i32) -> Packet<Command> {
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

fn new_node(node: i32) -> NewNodeId {
    NewNodeId::new(node).expect("a node id")
}

/// `/g_new` of the group `node` at the head of the root group.
fn g_new(node: i32) -> Command {
    Command::GNew(GNew {
        groups: vec![NewGroup {
            id: new_node(node),
            action: AddAction::Head,
            target: NodeId::ROOT,
        }],
    })
}

/// `/s_new` of the synth `node` of `def` at the head of the root group.
fn s_new(def: &str, node: i32) -> Command {
    Command::SNew(SNew {
        def: def.to_owned(),
        id: new_node(node),
        action: AddAction::Head,
        target: NodeId::ROOT,
        controls: Vec::new(),
    })
}

fn n_free(node: i32) -> Command {
    Command::NFree(NFree {
        nodes: vec![NodeId(node)],
    })
}

fn d_recv(data: Vec<u8>) -> Command {
    Command::DRecv(DRecv {
        data,
        completion: None,
    })
}

fn buffer(index: i32) -> BufferId {
    BufferId::new(index).expect("a buffer id")
}

fn failed(address: &str, message: &str) -> Error {
    Error::Failed {
        address: address.to_owned(),
        message: message.to_owned(),
        value: None,
    }
}

fn is_synced(sync: i32) -> impl Fn(&Reply) -> bool {
    move |reply| *reply == Reply::Synced(SyncId(sync))
}

/// `SynthDef(name, { |freq = 440| })`: one control, which nothing reads.
fn control_def(name: &str) -> SynthDef {
    SynthDefBuilder::new(name, vec![Arg::new("freq", 440.0)])
        .and_then(SynthDefBuilder::finish)
        .expect("the definition builds")
}

/// `SynthDef(name, { Line.kr(dur: 0.3, doneAction: 2) })`: its synth frees
/// itself after 0.3 seconds.
fn line_def(name: &str) -> SynthDef {
    let mut builder = SynthDefBuilder::new(name, Vec::new()).expect("a builder");
    let line = [("dur", 0.3.into()), ("doneAction", 2.0.into())];
    builder.ugen("Line", "kr", &line).expect("a Line");
    builder.finish().expect("the definition builds")
}

/// A path in the temporary directory, for this process and `name`.
fn temporary(name: &str) -> PathBuf {
    env::temp_dir().join(format!("scsynth-client-{}-{name}", process::id()))
}

fn text(path: &Path) -> String {
    path.to_str().expect("a path in UTF-8").to_owned()
}

/// Writes a WAV file of `frames` silent frames of 16-bit samples.
fn write_wav(path: &Path, channels: u16, sample_rate: u32, frames: u32) {
    let block = channels * 2;
    let data = frames * u32::from(block);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"RIFF");
    bytes.extend_from_slice(&(36 + data).to_le_bytes());
    bytes.extend_from_slice(b"WAVEfmt ");
    bytes.extend_from_slice(&16_u32.to_le_bytes());
    bytes.extend_from_slice(&1_u16.to_le_bytes());
    bytes.extend_from_slice(&channels.to_le_bytes());
    bytes.extend_from_slice(&sample_rate.to_le_bytes());
    bytes.extend_from_slice(&(sample_rate * u32::from(block)).to_le_bytes());
    bytes.extend_from_slice(&block.to_le_bytes());
    bytes.extend_from_slice(&16_u16.to_le_bytes());
    bytes.extend_from_slice(b"data");
    bytes.extend_from_slice(&data.to_le_bytes());
    bytes.resize(
        bytes.len() + usize::try_from(data).expect("a small file"),
        0,
    );
    fs::write(path, bytes).expect("the test writes the file");
}

/// The OSC time tag of `time`.
fn timetag(time: SystemTime) -> Timetag {
    /// The seconds from 1900, where OSC counts from, to 1970.
    const FROM_1900_TO_1970: u64 = 2_208_988_800;
    let since = time.duration_since(UNIX_EPOCH).expect("a time after 1970");
    let seconds = u32::try_from(since.as_secs() + FROM_1900_TO_1970).expect("a time before 2036");
    let fraction = (u64::from(since.subsec_nanos()) << 32) / 1_000_000_000;
    Timetag {
        seconds,
        fraction: u32::try_from(fraction).expect("a fraction of a second"),
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn a_synth_of_a_missing_definition_fails_with_synthdef_not_found() {
    let Some(addr) = address() else { return };
    for transport in TRANSPORTS {
        let (scsynth, _) = registered(&addr, transport, MISSING_DEFINITION).await;
        let outcome = scsynth.synth(MISSING, [], Target::head(NodeId::ROOT)).await;
        assert_eq!(outcome, Err(failed("/s_new", "SynthDef not found")));
        scsynth.close().await.expect("the client unregisters");
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn a_registered_client_gets_the_n_go_of_a_request_before_its_synced() {
    let Some(addr) = address() else { return };
    for transport in TRANSPORTS {
        let (scsynth, registration) = registered(&addr, transport, N_GO).await;
        scsynth
            .add_synthdef(&control_def(CONTROL))
            .await
            .expect("scsynth takes the definition");
        let node = (registration.client_id << 26) + 1000;
        let replies = scsynth
            .request(vec![s_new(CONTROL, node)])
            .await
            .expect("the synth starts");
        assert!(
            replies
                .iter()
                .any(|reply| matches!(reply, Reply::NGo(state) if state.node == NodeId(node))),
            "{replies:?}"
        );
        scsynth.free(NodeId(node)).await.expect("the synth ends");
        scsynth.close().await.expect("the client unregisters");
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn a_client_that_is_not_registered_gets_no_n_go_and_still_its_fail() {
    let Some(addr) = address() else { return };
    for transport in TRANSPORTS {
        let scsynth = client(&addr, transport, Options::default()).await;
        let node = NOT_REGISTERED_NODES + transport.offset();
        let replies = scsynth
            .request(vec![g_new(node)])
            .await
            .expect("the group starts");
        assert!(
            !replies.iter().any(|reply| matches!(reply, Reply::NGo(_))),
            "{replies:?}"
        );
        let outcome = scsynth.request(vec![s_new(MISSING, node + 5)]).await;
        assert_eq!(outcome, Err(failed("/s_new", "SynthDef not found")));
        scsynth.free(NodeId(node)).await.expect("the group ends");
        scsynth.close().await.expect("the client closes");
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn the_fail_of_a_second_bundle_can_come_before_the_synced_of_the_first() {
    const TRIALS: usize = 20;
    let Some(addr) = address() else { return };
    for transport in TRANSPORTS {
        let mut raw = Raw::connect(&addr, transport).await;
        let node = BACK_TO_BACK_NODES + transport.offset();
        let mut early = 0;
        for trial in 0..TRIALS {
            let sync = i32::try_from(trial * 3).expect("a small count");
            let (first, second, cleanup) = (sync + 1, sync + 2, sync + 3);
            raw.send(&bundle(vec![g_new(node)], first));
            raw.send(&bundle(vec![s_new(MISSING, node + 5)], second));
            let replies = raw.until(is_synced(second)).await;
            let place = |wanted: &dyn Fn(&Reply) -> bool| {
                replies
                    .iter()
                    .position(wanted)
                    .unwrap_or_else(|| panic!("{replies:?}"))
            };
            let fail =
                place(&|reply| matches!(reply, Reply::Fail(fail) if fail.command == "/s_new"));
            if fail < place(&is_synced(first)) {
                early += 1;
            }
            raw.send(&bundle(vec![n_free(node)], cleanup));
            raw.until(is_synced(cleanup)).await;
        }
        println!(
            "over {transport:?}, the /fail of the second bundle came before the /synced of the first in {early} of {TRIALS} trials"
        );
        assert!(early > 0, "over {transport:?}");
        raw.close();
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn two_requests_started_together_each_get_their_own_answer() {
    let Some(addr) = address() else { return };
    for transport in TRANSPORTS {
        let scsynth = client(&addr, transport, Options::default()).await;
        let node = TOGETHER_NODES + transport.offset();
        let (good, bad) = tokio::join!(
            scsynth.request(vec![g_new(node)]),
            scsynth.request(vec![s_new(MISSING, node + 5)]),
        );
        assert!(good.is_ok(), "{good:?}");
        assert_eq!(bad, Err(failed("/s_new", "SynthDef not found")));
        scsynth.free(NodeId(node)).await.expect("the group ends");
        scsynth.close().await.expect("the client closes");
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn notify_on_a_registered_connection_fails_with_the_client_id() {
    let Some(addr) = address() else { return };
    for transport in TRANSPORTS {
        let (scsynth, registration) = registered(&addr, transport, ALREADY_REGISTERED).await;
        let again = scsynth.register().await;
        assert_eq!(
            again,
            Err(Error::Failed {
                address: "/notify".to_owned(),
                message: "notify: already registered\n".to_owned(),
                value: Some(registration.client_id),
            })
        );
        scsynth.close().await.expect("the client unregisters");
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn d_recv_of_bytes_that_are_no_definition_is_done_and_the_client_sends_none() {
    let Some(addr) = address() else { return };
    let garbage = b"no synth definition".to_vec();
    for transport in TRANSPORTS {
        let scsynth = client(&addr, transport, Options::default()).await;
        let replies = scsynth
            .request(vec![d_recv(garbage.clone())])
            .await
            .expect("scsynth answers");
        assert!(replies.contains(&Reply::Done(Done::DRecv)), "{replies:?}");
        let mut events = scsynth.subscribe();
        let refused = scsynth.add_synthdef_bytes(&garbage).await;
        assert!(matches!(refused, Err(Error::Read(_))), "{refused:?}");
        scsynth.sync().await.expect("scsynth answers");
        let replies = until(&mut events, |reply| matches!(reply, Reply::Synced(_))).await;
        assert!(!replies.contains(&Reply::Done(Done::DRecv)), "{replies:?}");
        scsynth.close().await.expect("the client closes");
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn b_alloc_of_a_buffer_over_the_count_is_done_and_writes_buffer_0() {
    let Some(addr) = address() else { return };
    let over = buffer(99_999);
    // Buffer 0 is in the range of the client id 0, which no test registers.
    let zero = buffer(0);
    for transport in TRANSPORTS {
        let scsynth = client(&addr, transport, Options::default()).await;
        let frames = 40 + transport.offset();
        let alloc = Command::BAlloc(BAlloc {
            buffer: over,
            frames,
            channels: 1,
            completion: None,
            sample_rate: 0.0,
        });
        let replies = scsynth.request(vec![alloc]).await.expect("scsynth answers");
        let done = Reply::Done(Done::Buffer {
            command: BufferCommand::BAlloc,
            buffer: over,
        });
        assert!(replies.contains(&done), "{replies:?}");
        let info = scsynth.query_buffer(zero).await.expect("buffer 0 is there");
        assert_eq!((info.frames, info.channels), (frames, 1));
        scsynth.free_buffer(zero).await.expect("buffer 0 is freed");
        scsynth.close().await.expect("the client closes");
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn read_buffer_fails_for_a_missing_file_and_reads_a_wav_file() {
    let Some(addr) = address() else { return };
    for transport in TRANSPORTS {
        let (scsynth, registration) = registered(&addr, transport, READ_BUFFER).await;
        let first = first_of_share(registration, Options::default().buffers);
        let missing = temporary(&format!("{transport:?}-missing.wav"));
        match scsynth.read_buffer(text(&missing)).await {
            Err(Error::Failed {
                address,
                message,
                value,
            }) => {
                assert_eq!((address.as_str(), value), ("/b_allocRead", Some(first)));
                assert!(
                    message.starts_with("File '") && message.ends_with('\n'),
                    "{message:?}"
                );
            }
            other => panic!("{other:?}"),
        }
        let path = temporary(&format!("{transport:?}.wav"));
        write_wav(&path, 2, 22_050, 10);
        let read = scsynth.read_buffer(text(&path)).await;
        fs::remove_file(&path).expect("the test removes the file");
        let info = read.expect("scsynth reads the file");
        let expected = BufferInfo {
            buffer: buffer(first),
            frames: 10,
            channels: 2,
            sample_rate: 22_050.0,
        };
        assert_eq!(info, expected);
        scsynth
            .free_buffer(info.buffer)
            .await
            .expect("the buffer is freed");
        scsynth.close().await.expect("the client unregisters");
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn free_of_a_node_that_does_not_exist_fails_with_node_not_found() {
    let Some(addr) = address() else { return };
    for transport in TRANSPORTS {
        let scsynth = client(&addr, transport, Options::default()).await;
        let message = format!("Node {MISSING_NODE} not found");
        assert_eq!(
            scsynth.free(NodeId(MISSING_NODE)).await,
            Err(failed("/n_free", &message))
        );
        scsynth.close().await.expect("the client closes");
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn free_synthdef_of_an_unknown_name_is_ok() {
    let Some(addr) = address() else { return };
    for transport in TRANSPORTS {
        let scsynth = client(&addr, transport, Options::default()).await;
        assert_eq!(scsynth.free_synthdef(MISSING).await, Ok(()));
        scsynth.close().await.expect("the client closes");
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn a_bundle_ahead_of_time_gets_its_synced_at_its_time() {
    const AHEAD: Duration = Duration::from_millis(500);
    let Some(addr) = address() else { return };
    for transport in TRANSPORTS {
        let mut raw = Raw::connect(&addr, transport).await;
        let sent = Instant::now();
        raw.send(&Packet::Bundle(Bundle {
            time: timetag(SystemTime::now() + AHEAD),
            packets: vec![Packet::Message(Command::Sync(SyncId(1)))],
        }));
        raw.until(is_synced(1)).await;
        let delay = sent.elapsed();
        println!("over {transport:?}, the /synced of a bundle 500 ms ahead came after {delay:?}");
        assert!(
            (Duration::from_millis(450)..Duration::from_millis(1000)).contains(&delay),
            "over {transport:?}: {delay:?}"
        );
        raw.close();
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn n_info_reaches_every_registered_client_and_a_tree_its_sender_alone() {
    let Some(addr) = address() else { return };
    let (udp, _) = registered(&addr, Transport::Udp, NOTIFICATIONS).await;
    let (tcp, _) = registered(&addr, Transport::Tcp, NOTIFICATIONS).await;
    for (asker, other) in [(&udp, &tcp), (&tcp, &udp)] {
        let mut events = other.subscribe();
        let group = asker
            .group(Target::head(NodeId::ROOT))
            .await
            .expect("the group starts");
        asker.tree(group, false).await.expect("the tree comes");
        asker.query(group).await.expect("the state comes");
        let mut replies = until(
            &mut events,
            |reply| matches!(reply, Reply::NInfo(state) if state.node == group),
        )
        .await;
        other.sync().await.expect("scsynth answers");
        replies.extend(until(&mut events, |reply| matches!(reply, Reply::Synced(_))).await);
        assert!(
            !replies
                .iter()
                .any(|reply| matches!(reply, Reply::GQueryTreeReply(_))),
            "{replies:?}"
        );
        asker.free(group).await.expect("the group ends");
    }
    udp.close().await.expect("the client unregisters");
    tcp.close().await.expect("the client unregisters");
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn a_client_that_is_not_registered_gets_no_n_info() {
    let Some(addr) = address() else { return };
    for transport in TRANSPORTS {
        let scsynth = client(&addr, transport, Options::default()).await;
        let node = QUERY_NODES + transport.offset();
        scsynth
            .request(vec![g_new(node)])
            .await
            .expect("the group starts");
        assert_eq!(
            scsynth.query(NodeId(node)).await,
            Err(Error::NoReply {
                address: "/n_query"
            })
        );
        scsynth.free(NodeId(node)).await.expect("the group ends");
        scsynth.close().await.expect("the client closes");
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn the_n_info_of_query_and_the_n_set_of_get_come_before_the_synced() {
    let Some(addr) = address() else { return };
    for transport in TRANSPORTS {
        let (scsynth, _) = registered(&addr, transport, VALUES).await;
        scsynth
            .add_synthdef(&control_def(CONTROL))
            .await
            .expect("scsynth takes the definition");
        let freq = ControlSetting::new("freq", 330.0);
        let synth = scsynth
            .synth(CONTROL, [freq], Target::head(NodeId::ROOT))
            .await
            .expect("the synth starts");
        assert_eq!(scsynth.get(synth, "freq").await, Ok(330.0));
        let state = scsynth.query(synth).await.expect("the state comes");
        assert_eq!(
            (state.node, state.parent, state.kind),
            (synth, Some(NodeId::ROOT), NodeKind::Synth)
        );
        scsynth.free(synth).await.expect("the synth ends");
        scsynth.close().await.expect("the client unregisters");
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn alloc_buffer_of_0_frames_gives_no_reply_and_its_buffer_id_back() {
    let Some(addr) = address() else { return };
    for transport in TRANSPORTS {
        let (scsynth, registration) = registered(&addr, transport, NO_FRAMES).await;
        let first = buffer(first_of_share(registration, Options::default().buffers));
        assert_eq!(
            scsynth.alloc_buffer(0, 1).await,
            Err(Error::NoReply {
                address: "/b_alloc"
            })
        );
        let allocated = scsynth.alloc_buffer(4, 1).await;
        assert_eq!(allocated, Ok(first));
        scsynth
            .free_buffer(first)
            .await
            .expect("the buffer is freed");
        scsynth.close().await.expect("the client unregisters");
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn load_synthdef_of_a_missing_path_fails_with_the_path() {
    let Some(addr) = address() else { return };
    for transport in TRANSPORTS {
        let scsynth = client(&addr, transport, Options::default()).await;
        let path = text(&temporary("missing.scsyndef"));
        assert_eq!(
            scsynth.load_synthdef(path.clone()).await,
            Err(failed("/d_load", &path))
        );
        scsynth.close().await.expect("the client closes");
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn get_bus_of_an_index_out_of_range_fails() {
    let Some(addr) = address() else { return };
    for transport in TRANSPORTS {
        let scsynth = client(&addr, transport, Options::default()).await;
        assert_eq!(
            scsynth.get_bus(Options::default().control_buses).await,
            Err(failed("/c_get", "index out of range"))
        );
        scsynth.close().await.expect("the client closes");
    }
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn get_samples_gives_the_samples_that_one_reply_holds() {
    let Some(addr) = address() else { return };
    for transport in TRANSPORTS {
        let (scsynth, _) = registered(&addr, transport, SAMPLES).await;
        let buffer = scsynth
            .alloc_buffer(16_384, 1)
            .await
            .expect("the buffer is allocated");
        assert_eq!(
            scsynth.get_samples(buffer, 100, 16).await,
            Ok(vec![0.0; 16])
        );
        let (largest, over) = match transport {
            Transport::Udp => (13_095, Error::NoReply { address: "/b_getn" }),
            Transport::Tcp => (13_098, failed("/b_getn", "buffer overflow")),
        };
        let samples = scsynth.get_samples(buffer, 0, largest).await;
        assert_eq!(
            samples.map(|samples| samples.len()),
            Ok(usize::from(largest))
        );
        assert_eq!(scsynth.get_samples(buffer, 0, largest + 1).await, Err(over));
        scsynth
            .free_buffer(buffer)
            .await
            .expect("the buffer is freed");
        scsynth.close().await.expect("the client unregisters");
    }
}

#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn macos_refuses_a_datagram_over_its_limit() {
    let Some(addr) = address() else { return };
    let udp = UdpClient::connect_with_max_datagram(&addr, 65_507)
        .await
        .expect("UDP connects");
    let mut raw = Raw::over(Box::new(udp));
    let over = Packet::Message(d_recv(vec![0; 9_300]));
    assert_eq!(over.encode().len(), 9_316);
    raw.send(&over);
    let event = raw.next().await;
    assert!(
        matches!(
            event,
            Event::Error(scsynth_client::ClientError::Transport(_))
        ),
        "{event:?}"
    );
    let under = Packet::Message(d_recv(vec![0; 9_000]));
    assert_eq!(under.encode().len(), 9_016);
    raw.send(&under);
    raw.until(|reply| *reply == Reply::Done(Done::DRecv)).await;
    raw.close();
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn a_packet_of_60_016_bytes_passes_over_tcp() {
    let Some(addr) = address() else { return };
    let mut raw = Raw::connect(&addr, Transport::Tcp).await;
    let packet = Packet::Message(d_recv(vec![0; 60_000]));
    assert_eq!(packet.encode().len(), 60_016);
    raw.send(&packet);
    raw.until(|reply| *reply == Reply::Done(Done::DRecv)).await;
    raw.close();
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn the_method_table_over_udp() {
    let Some(addr) = address() else { return };
    method_table(&addr, Transport::Udp).await;
}

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn the_method_table_over_tcp() {
    let Some(addr) = address() else { return };
    method_table(&addr, Transport::Tcp).await;
}

/// One session through every method of the client.
async fn method_table(addr: &str, transport: Transport) {
    let (scsynth, registration) = registered(addr, transport, METHOD_TABLE).await;
    let node_base = registration.client_id << 26;

    let version = scsynth.version().await.expect("the version comes");
    assert_eq!(version.program, "scsynth");
    let status = scsynth.status().await.expect("the status comes");
    assert!(status.nominal_sample_rate > 0.0, "{status:?}");
    scsynth.sync().await.expect("scsynth answers");

    let control = format!("{CONTROL}-{transport:?}");
    scsynth
        .add_synthdef(&control_def(&control))
        .await
        .expect("scsynth takes the definition");
    scsynth
        .add_synthdef(&line_def(LINE))
        .await
        .expect("scsynth takes the definition");

    let group = scsynth
        .group(Target::head(NodeId::ROOT))
        .await
        .expect("the group starts");
    assert_eq!(group, NodeId(node_base + 1000));
    let freq = ControlSetting::new("freq", 440.0);
    let synth = scsynth
        .synth(&control, [freq], Target::tail(group))
        .await
        .expect("the synth starts");
    assert_eq!(synth, NodeId(node_base + 1001));
    controls_and_buses(&scsynth, registration, group, synth, &control).await;
    queries_and_moves(&scsynth, group, synth, &control).await;

    let line = scsynth
        .synth(LINE, [], Target::head(NodeId::ROOT))
        .await
        .expect("the synth starts");
    scsynth
        .ended(line, Duration::from_secs(2))
        .await
        .expect("the synth ends by itself");

    buffers(&scsynth, registration, status.nominal_sample_rate).await;

    scsynth
        .free_synthdef(control)
        .await
        .expect("scsynth removes the definition");
    scsynth.unregister().await.expect("the client unregisters");
    assert_eq!(
        scsynth.group(Target::head(NodeId::ROOT)).await,
        Err(Error::NotRegistered)
    );
    scsynth.close().await.expect("the client closes");
    assert_eq!(scsynth.sync().await, Err(Error::Send(SendError::Closed)));
}

/// `get`, `set`, and a control bus with `set_bus`, `get_bus` and `map`, on
/// the synth `synth` of `def`, alone in `group`.
async fn controls_and_buses(
    scsynth: &Scsynth,
    registration: Registration,
    group: NodeId,
    synth: NodeId,
    def: &str,
) {
    assert_eq!(scsynth.get(synth, "freq").await, Ok(440.0));
    scsynth
        .set(synth, [ControlSetting::new("freq", 660.0)])
        .expect("the transport takes it");
    assert_eq!(scsynth.get(synth, "freq").await, Ok(660.0));

    let bus = scsynth.control_bus(1).expect("a control bus");
    let first = first_of_share(registration, Options::default().control_buses);
    assert_eq!(bus, first);
    scsynth.set_bus(bus, 0.25).expect("the transport takes it");
    assert_eq!(scsynth.get_bus(bus).await, Ok(0.25));
    let mapping = ControlMapping {
        control: Control::from("freq"),
        bus,
    };
    scsynth
        .map(synth, [mapping])
        .expect("the transport takes it");
    let mapped = GQueryTreeReply::WithControls(TreeGroup {
        id: group,
        children: vec![TreeNode::Synth(TreeSynth {
            id: synth,
            def: def.to_owned(),
            controls: vec![TreeControl {
                control: Control::from("freq"),
                value: TreeValue::Bus(BusMapping::Control(bus.unsigned_abs())),
            }],
        })],
    });
    assert_eq!(scsynth.tree(group, true).await, Ok(mapped));
    scsynth.free_control_bus(bus).expect("the bus goes back");
}

/// `query`, `tree`, `run`, `move_node`, `free_all`, `deep_free` and `free`,
/// from the synth `synth` of `def`, alone in `group`, which they free.
async fn queries_and_moves(scsynth: &Scsynth, group: NodeId, synth: NodeId, def: &str) {
    let state = scsynth.query(synth).await.expect("the state comes");
    assert_eq!((state.node, state.parent), (synth, Some(group)));
    let plain = GQueryTreeReply::WithoutControls(TreeGroup {
        id: group,
        children: vec![TreeNode::Synth(TreeSynth {
            id: synth,
            def: def.to_owned(),
            controls: (),
        })],
    });
    assert_eq!(scsynth.tree(group, false).await, Ok(plain));
    scsynth.run(synth, false).await.expect("the synth stops");
    scsynth.run(synth, true).await.expect("the synth runs");

    let inner = scsynth
        .group(Target::tail(group))
        .await
        .expect("the group starts");
    scsynth
        .move_node(synth, Target::head(inner))
        .await
        .expect("the synth moves");
    let state = scsynth.query(synth).await.expect("the state comes");
    assert_eq!(state.parent, Some(inner));
    scsynth
        .move_node(synth, Target::after(inner))
        .await
        .expect("the synth moves");
    let state = scsynth.query(synth).await.expect("the state comes");
    assert_eq!((state.parent, state.prev), (Some(group), Some(inner)));

    let other = scsynth
        .synth(def, [], Target::head(inner))
        .await
        .expect("the synth starts");
    scsynth.free_all(inner).await.expect("the group empties");
    let message = format!("Node {} not found", other.0);
    assert_eq!(
        scsynth.query(other).await,
        Err(failed("/n_query", &message))
    );
    scsynth.deep_free(group).await.expect("the synths end");
    let state = scsynth.query(group).await.expect("the state comes");
    let kind = NodeKind::Group {
        head: Some(inner),
        tail: Some(inner),
    };
    assert_eq!(state.kind, kind);
    scsynth.free(group).await.expect("the groups end");
}

/// `alloc_buffer`, `query_buffer`, `get_samples` and `free_buffer`, on a
/// server at the sample rate `rate`.
async fn buffers(scsynth: &Scsynth, registration: Registration, rate: f64) {
    let buffer = scsynth.alloc_buffer(32, 2).await.expect("the buffer");
    let first = first_of_share(registration, Options::default().buffers);
    assert_eq!(buffer.get(), first);
    let info = scsynth
        .query_buffer(buffer)
        .await
        .expect("the buffer is there");
    assert_eq!(
        (info.frames, info.channels, f64::from(info.sample_rate)),
        (32, 2, rate)
    );
    assert_eq!(scsynth.get_samples(buffer, 0, 64).await, Ok(vec![0.0; 64]));
    scsynth
        .free_buffer(buffer)
        .await
        .expect("the buffer is freed");
}
