//! The methods of `Scsynth` for synth definitions, buffers and buses on a
//! `ChannelClient` pair, with no runtime: the packet that each one sends,
//! and the value that it gives.

mod common;

use std::pin::pin;
use std::task::Poll;

use common::{
    all, answer, buffer, buffer_info, connect, done_buffer, fail, failed, now, poll, registered,
    registered_with, request, serve, sine, synced,
};
use scsynth_client::scsynth_osc::command::buffer::{BAlloc, BAllocRead, BFree, BGetn, BQuery};
use scsynth_client::scsynth_osc::command::control_bus::{CGet, CSet};
use scsynth_client::scsynth_osc::command::synth_def::{DFree, DLoad, DRecv};
use scsynth_client::scsynth_osc::command::top_level::Notify;
use scsynth_client::scsynth_osc::reply::buffer::{BInfo, BSetn};
use scsynth_client::scsynth_osc::reply::command_replies::{BufferCommand, Done};
use scsynth_client::scsynth_osc::reply::control_bus::CSet as CSetReply;
use scsynth_client::scsynth_osc::{
    Command, Completion, IndexRange, IndexValue, IndexValues, Number, Packet, Reply,
};
use scsynth_client::scsynth_synthdef::{SynthDef, read_def_file};
use scsynth_client::{Error, IdKind, Options};

/// The message of the `/fail` of a `/b_allocRead` of a missing file, in the
/// form that scsynth 3.14.1 gives it.
const NOT_OPENED: &str = "File 'missing.wav' could not be opened: System error\n";

/// The answers of a scsynth to the buffer commands, where each sound file
/// has 44,100 frames of 2 channels.
fn buffers(command: &Command) -> Vec<Reply> {
    match command {
        Command::BAlloc(alloc) => vec![done_buffer(BufferCommand::BAlloc, alloc.buffer)],
        Command::BAllocRead(read) => vec![
            Reply::BInfo(BInfo {
                buffers: vec![buffer_info(read.buffer, 44_100, 2)],
            }),
            done_buffer(BufferCommand::BAllocRead, read.buffer),
        ],
        Command::BFree(free) => vec![done_buffer(BufferCommand::BFree, free.buffer)],
        other => answer(other),
    }
}

fn b_alloc(index: i32, frames: i32, channels: i32) -> Command {
    Command::BAlloc(BAlloc {
        buffer: buffer(index),
        frames,
        channels,
        completion: None,
        sample_rate: 0.0,
    })
}

fn b_alloc_read(index: i32, path: &str) -> Command {
    let query = Command::BQuery(BQuery {
        buffers: vec![buffer(index)],
    });
    Command::BAllocRead(BAllocRead {
        buffer: buffer(index),
        path: path.to_owned(),
        file_frame: 0,
        frames: 0,
        completion: Some(Completion::Message(Box::new(query))),
    })
}

fn d_recv(data: Vec<u8>) -> Command {
    Command::DRecv(DRecv {
        data,
        completion: None,
    })
}

/// The buffer id of each `/b_alloc` and `/b_allocRead` of `packets`.
fn allocated(packets: Vec<Packet<Command>>) -> Vec<i32> {
    packets
        .into_iter()
        .flat_map(Packet::into_messages)
        .filter_map(|command| match command {
            Command::BAlloc(alloc) => Some(alloc.buffer.get()),
            Command::BAllocRead(read) => Some(read.buffer.get()),
            _ => None,
        })
        .collect()
}

#[test]
fn the_futures_of_the_resource_methods_are_send() {
    fn send<T: Send>(_: &T) {}
    let (scsynth, _peer, _timer) = connect(Options::default());
    let def = sine();
    send(&scsynth.add_synthdef(&def));
    send(&scsynth.add_synthdef_bytes(b"SCgf"));
    send(&scsynth.load_synthdef("sine.scsyndef"));
    send(&scsynth.free_synthdef("sine"));
    send(&scsynth.alloc_buffer(44_100, 2));
    send(&scsynth.read_buffer("a.wav"));
    send(&scsynth.free_buffer(buffer(0)));
    send(&scsynth.query_buffer(buffer(0)));
    send(&scsynth.get_samples(buffer(0), 0, 4));
    send(&scsynth.get_bus(0));
}

#[test]
fn add_synthdef_sends_the_bytes_of_the_definition() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, |command| match command {
        Command::DRecv(_) => vec![Reply::Done(Done::DRecv)],
        other => answer(other),
    });
    let def = sine();
    assert_eq!(now(scsynth.add_synthdef(&def)), Ok(()));
    let bytes = def.as_bytes().expect("the bytes of the definition");
    assert_eq!(
        read_def_file(&bytes).map(|file| file.synth_defs),
        Ok(vec![def])
    );
    assert_eq!(all(&packets), [request(vec![d_recv(bytes)], 1)]);
}

#[test]
fn add_synthdef_over_a_limit_of_the_format_gives_limit_and_sends_nothing() {
    let (scsynth, peer, timer) = connect(Options::default());
    let long = SynthDef {
        name: "s".repeat(256),
        ..sine()
    };
    let error = long.as_bytes().expect_err("a name over 255 bytes");
    assert_eq!(now(scsynth.add_synthdef(&long)), Err(Error::Limit(error)));
    assert_eq!(peer.take(), None);
    assert_eq!(timer.limits(), []);
}

#[test]
fn add_synthdef_bytes_refuses_bytes_that_are_no_definition_and_sends_nothing() {
    let (scsynth, peer, timer) = connect(Options::default());
    let garbage = b"no definition";
    let error = read_def_file(garbage).expect_err("no definition file");
    assert_eq!(
        now(scsynth.add_synthdef_bytes(garbage)),
        Err(Error::Read(error))
    );
    assert_eq!(peer.take(), None);
    assert_eq!(timer.limits(), []);
}

#[test]
fn add_synthdef_bytes_sends_the_bytes_of_a_definition_unchanged() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, |command| match command {
        Command::DRecv(_) => vec![Reply::Done(Done::DRecv)],
        other => answer(other),
    });
    let bytes = sine().as_bytes().expect("the bytes of the definition");
    assert_eq!(now(scsynth.add_synthdef_bytes(&bytes)), Ok(()));
    assert_eq!(all(&packets), [request(vec![d_recv(bytes)], 1)]);
}

#[test]
fn load_synthdef_sends_d_load_with_its_path() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, |command| match command {
        Command::DLoad(_) => vec![Reply::Done(Done::DLoad)],
        other => answer(other),
    });
    assert_eq!(now(scsynth.load_synthdef("defs/sine.scsyndef")), Ok(()));
    let load = Command::DLoad(DLoad {
        path: "defs/sine.scsyndef".to_owned(),
        completion: None,
    });
    assert_eq!(all(&packets), [request(vec![load], 1)]);
}

#[test]
fn the_definition_methods_without_their_done_give_no_reply() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, answer);
    let def = sine();
    let bytes = def.as_bytes().expect("the bytes of the definition");
    let d_recv_missing = Err(Error::NoReply { address: "/d_recv" });
    assert_eq!(now(scsynth.add_synthdef(&def)), d_recv_missing);
    assert_eq!(now(scsynth.add_synthdef_bytes(&bytes)), d_recv_missing);
    assert_eq!(
        now(scsynth.load_synthdef("defs/sine.scsyndef")),
        Err(Error::NoReply { address: "/d_load" })
    );
    assert_eq!(all(&packets).len(), 3);
}

#[test]
fn free_synthdef_settles_at_the_synced_alone() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, answer);
    assert_eq!(now(scsynth.free_synthdef("missing")), Ok(()));
    let free = Command::DFree(DFree {
        names: vec!["missing".to_owned()],
    });
    assert_eq!(all(&packets), [request(vec![free], 1)]);
}

#[test]
fn alloc_buffer_sends_b_alloc_with_the_next_buffer_id_of_the_client() {
    let (scsynth, _peer, packets, _timer) = registered(|command| match command {
        Command::Notify(Notify { on: true, .. }) => vec![Reply::Done(Done::Notify {
            client_id: 1,
            max_logins: Some(4),
        })],
        other => buffers(other),
    });
    assert_eq!(now(scsynth.alloc_buffer(44_100, 2)), Ok(buffer(256)));
    assert_eq!(now(scsynth.alloc_buffer(512, 1)), Ok(buffer(257)));
    assert_eq!(
        all(&packets),
        [
            request(vec![b_alloc(256, 44_100, 2)], 2),
            request(vec![b_alloc(257, 512, 1)], 3),
        ]
    );
}

#[test]
fn a_failed_alloc_gives_failed_and_its_buffer_id_back() {
    let (scsynth, _peer, packets, _timer) = registered(|command| match command {
        Command::BAlloc(alloc) if alloc.frames == 0 => {
            vec![fail("/b_alloc", "refused", Some(alloc.buffer.get()))]
        }
        other => buffers(other),
    });
    assert_eq!(
        now(scsynth.alloc_buffer(0, 1)),
        Err(Error::Failed {
            address: "/b_alloc".to_owned(),
            message: "refused".to_owned(),
            value: Some(0),
        })
    );
    assert_eq!(now(scsynth.alloc_buffer(512, 1)), Ok(buffer(0)));
    assert_eq!(allocated(all(&packets)), [0, 0]);
}

#[test]
fn alloc_buffer_without_the_done_of_its_buffer_gives_no_reply_and_its_buffer_id_back() {
    let (scsynth, _peer, packets, _timer) = registered(|command| match command {
        Command::BAlloc(alloc) if alloc.frames == 0 => Vec::new(),
        Command::BAlloc(alloc) if alloc.frames == 1 => {
            vec![done_buffer(BufferCommand::BAlloc, buffer(9))]
        }
        other => buffers(other),
    });
    let missing = Err(Error::NoReply {
        address: "/b_alloc",
    });
    assert_eq!(now(scsynth.alloc_buffer(0, 1)), missing);
    assert_eq!(now(scsynth.alloc_buffer(1, 1)), missing);
    assert_eq!(now(scsynth.alloc_buffer(512, 1)), Ok(buffer(0)));
    assert_eq!(allocated(all(&packets)), [0, 0, 0]);
}

#[test]
fn alloc_buffer_and_get_samples_send_i32_max_for_a_value_over_it() {
    let (scsynth, _peer, packets, _timer) = registered(buffers);
    assert_eq!(now(scsynth.alloc_buffer(u32::MAX, 1)), Ok(buffer(0)));
    assert_eq!(now(scsynth.alloc_buffer(1, u32::MAX)), Ok(buffer(1)));
    assert_eq!(
        now(scsynth.get_samples(buffer(0), u32::MAX, u16::MAX)),
        Err(Error::NoReply { address: "/b_getn" })
    );
    let get = Command::BGetn(BGetn {
        buffer: buffer(0),
        ranges: vec![IndexRange {
            start: i32::MAX,
            count: 65_535,
        }],
    });
    assert_eq!(
        all(&packets),
        [
            request(vec![b_alloc(0, i32::MAX, 1)], 2),
            request(vec![b_alloc(1, 1, i32::MAX)], 3),
            request(vec![get], 4)
        ]
    );
}

#[test]
fn read_buffer_sends_one_b_alloc_read_with_a_b_query_completion_and_reads_its_entry() {
    let (scsynth, _peer, packets, _timer) = registered(|command| match command {
        Command::BAllocRead(read) => vec![
            Reply::BInfo(BInfo {
                buffers: vec![buffer_info(buffer(9), 10, 1)],
            }),
            Reply::BInfo(BInfo {
                buffers: vec![buffer_info(read.buffer, 44_100, 2)],
            }),
            done_buffer(BufferCommand::BAllocRead, read.buffer),
        ],
        other => buffers(other),
    });
    assert_eq!(
        now(scsynth.read_buffer("sounds/a11wlk01.wav")),
        Ok(buffer_info(buffer(0), 44_100, 2))
    );
    assert_eq!(
        all(&packets),
        [request(vec![b_alloc_read(0, "sounds/a11wlk01.wav")], 2)]
    );
}

#[test]
fn a_failed_read_buffer_gives_failed_and_its_buffer_id_back() {
    let (scsynth, _peer, packets, _timer) = registered(|command| match command {
        Command::BAllocRead(read) if read.path == "missing.wav" => {
            vec![fail("/b_allocRead", NOT_OPENED, Some(read.buffer.get()))]
        }
        other => buffers(other),
    });
    assert_eq!(
        now(scsynth.read_buffer("missing.wav")),
        Err(Error::Failed {
            address: "/b_allocRead".to_owned(),
            message: NOT_OPENED.to_owned(),
            value: Some(0),
        })
    );
    assert_eq!(
        now(scsynth.read_buffer("a.wav")),
        Ok(buffer_info(buffer(0), 44_100, 2))
    );
    assert_eq!(allocated(all(&packets)), [0, 0]);
}

#[test]
fn read_buffer_without_the_b_info_of_its_buffer_gives_no_reply_and_its_buffer_id_back() {
    let (scsynth, _peer, packets, _timer) = registered(|command| match command {
        Command::BAllocRead(read) => vec![
            Reply::BInfo(BInfo {
                buffers: vec![buffer_info(buffer(9), 10, 1)],
            }),
            done_buffer(BufferCommand::BAllocRead, read.buffer),
        ],
        other => buffers(other),
    });
    assert_eq!(
        now(scsynth.read_buffer("a.wav")),
        Err(Error::NoReply {
            address: "/b_allocRead"
        })
    );
    assert_eq!(now(scsynth.alloc_buffer(512, 1)), Ok(buffer(0)));
    assert_eq!(allocated(all(&packets)), [0, 0]);
}

#[test]
fn a_dropped_alloc_gives_its_buffer_id_back() {
    let (scsynth, peer, _packets, _timer) = registered(buffers);
    peer.on_packet(|_, _| {});
    {
        let mut alloc = pin!(scsynth.alloc_buffer(512, 1));
        assert!(poll(alloc.as_mut()).is_pending());
    }
    let packets = serve(&peer, buffers);
    // The barrier that the dropped call left.
    peer.send(synced(3));
    assert_eq!(now(scsynth.alloc_buffer(512, 1)), Ok(buffer(0)));
    assert_eq!(all(&packets), [request(vec![b_alloc(0, 512, 1)], 4)]);
}

#[test]
fn free_buffer_sends_b_free_and_gives_the_buffer_id_back() {
    let (scsynth, _peer, packets, _timer) = registered(buffers);
    assert_eq!(now(scsynth.alloc_buffer(512, 1)), Ok(buffer(0)));
    assert_eq!(now(scsynth.alloc_buffer(512, 1)), Ok(buffer(1)));
    assert_eq!(now(scsynth.free_buffer(buffer(0))), Ok(()));
    assert_eq!(now(scsynth.free_buffer(buffer(500))), Ok(()));
    assert_eq!(now(scsynth.alloc_buffer(512, 1)), Ok(buffer(0)));
    assert_eq!(now(scsynth.alloc_buffer(512, 1)), Ok(buffer(2)));
    let free = |index| {
        Command::BFree(BFree {
            buffer: buffer(index),
            completion: None,
        })
    };
    let frees: Vec<_> = all(&packets)
        .into_iter()
        .flat_map(Packet::into_messages)
        .filter(|command| matches!(command, Command::BFree(_)))
        .collect();
    assert_eq!(frees, [free(0), free(500)]);
}

#[test]
fn a_failed_free_buffer_keeps_the_buffer_id() {
    let (scsynth, _peer, _packets, _timer) = registered(|command| match command {
        Command::BFree(free) => vec![fail("/b_free", "refused", Some(free.buffer.get()))],
        other => buffers(other),
    });
    assert_eq!(now(scsynth.alloc_buffer(512, 1)), Ok(buffer(0)));
    assert!(matches!(
        now(scsynth.free_buffer(buffer(0))),
        Err(Error::Failed { .. })
    ));
    assert_eq!(now(scsynth.alloc_buffer(512, 1)), Ok(buffer(1)));
}

#[test]
fn free_buffer_without_the_done_of_its_buffer_gives_no_reply_and_keeps_the_buffer_id() {
    let (scsynth, _peer, _packets, _timer) = registered(|command| match command {
        Command::BFree(_) => vec![done_buffer(BufferCommand::BFree, buffer(9))],
        other => buffers(other),
    });
    assert_eq!(now(scsynth.alloc_buffer(512, 1)), Ok(buffer(0)));
    assert_eq!(
        now(scsynth.free_buffer(buffer(0))),
        Err(Error::NoReply { address: "/b_free" })
    );
    assert_eq!(now(scsynth.alloc_buffer(512, 1)), Ok(buffer(1)));
}

#[test]
fn free_query_and_get_samples_need_no_registration() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, buffers);
    assert_eq!(now(scsynth.free_buffer(buffer(3))), Ok(()));
    assert_eq!(
        now(scsynth.query_buffer(buffer(3))),
        Err(Error::NoReply {
            address: "/b_query"
        })
    );
    assert_eq!(
        now(scsynth.get_samples(buffer(3), 0, 1)),
        Err(Error::NoReply { address: "/b_getn" })
    );
    assert_eq!(all(&packets).len(), 3);
}

#[test]
fn query_buffer_gives_the_entry_of_its_buffer() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, |command| match command {
        Command::BQuery(query) if query.buffers == [buffer(3)] => vec![Reply::BInfo(BInfo {
            buffers: vec![buffer_info(buffer(9), 10, 1), buffer_info(buffer(3), 0, 0)],
        })],
        Command::BQuery(_) => vec![Reply::BInfo(BInfo {
            buffers: vec![buffer_info(buffer(9), 10, 1)],
        })],
        other => answer(other),
    });
    assert_eq!(
        now(scsynth.query_buffer(buffer(3))),
        Ok(buffer_info(buffer(3), 0, 0))
    );
    assert_eq!(
        now(scsynth.query_buffer(buffer(4))),
        Err(Error::NoReply {
            address: "/b_query"
        })
    );
    let query = |index| {
        Command::BQuery(BQuery {
            buffers: vec![buffer(index)],
        })
    };
    assert_eq!(
        all(&packets),
        [request(vec![query(3)], 1), request(vec![query(4)], 2)]
    );
}

#[test]
fn get_samples_sends_one_run_of_b_getn_and_gives_the_values_of_its_start() {
    let setn = |index, start, values: &[f32]| {
        Reply::BSetn(BSetn {
            buffer: buffer(index),
            ranges: vec![IndexValues {
                start,
                values: values.to_vec(),
            }],
        })
    };
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, move |command| match command {
        Command::BGetn(get) if get.ranges[0].start == 4 => vec![
            setn(9, 4, &[9.0]),
            setn(3, 0, &[0.0]),
            setn(3, 4, &[0.25, -0.5]),
        ],
        Command::BGetn(_) => vec![setn(3, 0, &[0.0])],
        other => answer(other),
    });
    assert_eq!(
        now(scsynth.get_samples(buffer(3), 4, 2)),
        Ok(vec![0.25, -0.5])
    );
    assert_eq!(
        now(scsynth.get_samples(buffer(3), 8, 2)),
        Err(Error::NoReply { address: "/b_getn" })
    );
    let get = |start| {
        Command::BGetn(BGetn {
            buffer: buffer(3),
            ranges: vec![IndexRange { start, count: 2 }],
        })
    };
    assert_eq!(
        all(&packets),
        [request(vec![get(4)], 1), request(vec![get(8)], 2)]
    );
}

#[test]
fn a_buffer_id_at_the_count_of_the_server_gives_out_of_range_and_sends_nothing() {
    let options = Options {
        buffers: 16,
        ..Options::default()
    };
    let (scsynth, peer, timer) = connect(options);
    let over = Error::OutOfRange {
        kind: IdKind::Buffer,
        id: 16,
    };
    assert_eq!(now(scsynth.free_buffer(buffer(16))), Err(over.clone()));
    assert_eq!(
        now(scsynth.query_buffer(buffer(16))).map(drop),
        Err(over.clone())
    );
    assert_eq!(
        now(scsynth.get_samples(buffer(16), 0, 1)).map(drop),
        Err(over)
    );
    assert_eq!(peer.take(), None);
    assert_eq!(timer.limits(), []);
    let mut last = pin!(scsynth.query_buffer(buffer(15)));
    assert!(poll(last.as_mut()).is_pending());
    let query = Command::BQuery(BQuery {
        buffers: vec![buffer(15)],
    });
    assert_eq!(peer.take(), Some(request(vec![query], 1)));
}

#[test]
fn alloc_and_read_before_register_give_not_registered_and_send_nothing() {
    let (scsynth, peer, timer) = connect(Options::default());
    assert_eq!(now(scsynth.alloc_buffer(512, 1)), Err(Error::NotRegistered));
    assert_eq!(now(scsynth.read_buffer("a.wav")), Err(Error::NotRegistered));
    assert_eq!(peer.take(), None);
    assert_eq!(timer.limits(), []);
}

#[test]
fn a_full_buffer_range_gives_exhausted() {
    let options = Options {
        buffers: 8,
        ..Options::default()
    };
    let (scsynth, _peer, packets, _timer) = registered_with(options, buffers);
    assert_eq!(now(scsynth.alloc_buffer(512, 1)), Ok(buffer(0)));
    assert_eq!(now(scsynth.alloc_buffer(512, 1)), Ok(buffer(1)));
    assert_eq!(
        now(scsynth.alloc_buffer(512, 1)),
        Err(Error::Exhausted(IdKind::Buffer))
    );
    assert_eq!(allocated(all(&packets)), [0, 1]);
}

#[test]
fn a_client_id_at_or_over_the_login_limit_has_no_buffers_and_no_buses() {
    let (scsynth, _peer, packets, _timer) = registered(|command| match command {
        Command::Notify(Notify { on: true, .. }) => vec![Reply::Done(Done::Notify {
            client_id: 4,
            max_logins: Some(4),
        })],
        other => buffers(other),
    });
    assert_eq!(
        now(scsynth.alloc_buffer(512, 1)),
        Err(Error::Exhausted(IdKind::Buffer))
    );
    assert_eq!(
        scsynth.control_bus(1),
        Err(Error::Exhausted(IdKind::ControlBus))
    );
    assert_eq!(
        scsynth.audio_bus(1),
        Err(Error::Exhausted(IdKind::AudioBus))
    );
    assert_eq!(all(&packets), []);
}

#[test]
fn control_bus_and_audio_bus_give_runs_of_the_ranges_of_the_client_and_send_nothing() {
    let (scsynth, _peer, packets, _timer) = registered(|command| match command {
        Command::Notify(Notify { on: true, .. }) => vec![Reply::Done(Done::Notify {
            client_id: 1,
            max_logins: Some(4),
        })],
        other => answer(other),
    });
    assert_eq!(scsynth.control_bus(1), Ok(4096));
    assert_eq!(scsynth.control_bus(4), Ok(4097));
    assert_eq!(scsynth.control_bus(1), Ok(4101));
    assert_eq!(scsynth.audio_bus(2), Ok(268));
    assert_eq!(scsynth.audio_bus(1), Ok(270));
    assert_eq!(all(&packets), []);
}

#[test]
fn a_freed_bus_run_is_taken_again() {
    let (scsynth, _peer, packets, _timer) = registered(answer);
    assert_eq!(scsynth.control_bus(4), Ok(0));
    assert_eq!(scsynth.control_bus(4), Ok(4));
    assert_eq!(scsynth.free_control_bus(0), Ok(()));
    assert_eq!(scsynth.free_control_bus(1), Ok(()));
    assert_eq!(scsynth.control_bus(2), Ok(0));
    assert_eq!(scsynth.control_bus(4), Ok(8));
    assert_eq!(scsynth.audio_bus(2), Ok(16));
    assert_eq!(scsynth.free_audio_bus(16), Ok(()));
    assert_eq!(scsynth.audio_bus(2), Ok(16));
    assert_eq!(all(&packets), []);
}

#[test]
fn a_bus_count_of_0_gives_exhausted() {
    let (scsynth, _peer, _packets, _timer) = registered(answer);
    assert_eq!(
        scsynth.control_bus(0),
        Err(Error::Exhausted(IdKind::ControlBus))
    );
    assert_eq!(
        scsynth.audio_bus(0),
        Err(Error::Exhausted(IdKind::AudioBus))
    );
}

#[test]
fn a_full_bus_range_gives_exhausted() {
    let (scsynth, _peer, _packets, _timer) = registered(answer);
    assert_eq!(
        scsynth.control_bus(4097),
        Err(Error::Exhausted(IdKind::ControlBus))
    );
    assert_eq!(scsynth.control_bus(4096), Ok(0));
    assert_eq!(
        scsynth.control_bus(1),
        Err(Error::Exhausted(IdKind::ControlBus))
    );
    assert_eq!(scsynth.audio_bus(252), Ok(16));
    assert_eq!(
        scsynth.audio_bus(1),
        Err(Error::Exhausted(IdKind::AudioBus))
    );
}

#[test]
fn the_bus_runs_before_register_give_not_registered() {
    let (scsynth, peer, _timer) = connect(Options::default());
    assert_eq!(scsynth.control_bus(1), Err(Error::NotRegistered));
    assert_eq!(scsynth.control_bus(0), Err(Error::NotRegistered));
    assert_eq!(scsynth.audio_bus(1), Err(Error::NotRegistered));
    assert_eq!(scsynth.free_control_bus(0), Err(Error::NotRegistered));
    assert_eq!(scsynth.free_audio_bus(16), Err(Error::NotRegistered));
    assert_eq!(peer.take(), None);
}

#[test]
fn set_bus_is_a_plain_message_that_waits_for_nothing() {
    let (scsynth, peer, timer) = connect(Options::default());
    let mut flight = pin!(scsynth.sync());
    assert!(poll(flight.as_mut()).is_pending());
    assert_eq!(scsynth.set_bus(5, 0.5), Ok(()));
    assert_eq!(peer.take(), Some(request(Vec::new(), 1)));
    let set = CSet {
        buses: vec![IndexValue {
            index: 5,
            value: Number::Float(0.5),
        }],
    };
    assert_eq!(peer.take(), Some(Packet::Message(Command::CSet(set))));
    assert_eq!(peer.take(), None);
    assert_eq!(timer.limits(), [Options::default().timeout]);
    peer.send(synced(1));
    assert_eq!(poll(flight.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn get_bus_gives_the_value_of_its_index_in_the_c_set() {
    let reading = |index, value| {
        Reply::CSet(CSetReply {
            buses: vec![IndexValue { index, value }],
        })
    };
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, move |command| match command {
        Command::CGet(get) if get.buses == [5] => vec![reading(6, 0.75), reading(5, 0.5)],
        Command::CGet(get) if get.buses == [99_999] => {
            vec![fail("/c_get", "index out of range", None)]
        }
        Command::CGet(_) => vec![reading(6, 0.75)],
        other => answer(other),
    });
    assert_eq!(now(scsynth.get_bus(5)), Ok(0.5));
    assert_eq!(
        now(scsynth.get_bus(7)),
        Err(Error::NoReply { address: "/c_get" })
    );
    assert_eq!(
        now(scsynth.get_bus(99_999)),
        Err(failed("/c_get", "index out of range"))
    );
    let get = |index| Command::CGet(CGet { buses: vec![index] });
    assert_eq!(
        all(&packets),
        [
            request(vec![get(5)], 1),
            request(vec![get(7)], 2),
            request(vec![get(99_999)], 3)
        ]
    );
}
