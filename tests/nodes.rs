//! The node methods of `Scsynth` on a `ChannelClient` pair, with no runtime:
//! the packet that each one sends, and the value that it gives.

mod common;

use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

use common::{
    all, answer, connect, fail, failed, n_go, now, poll, registered, request, serve, synced,
    synth_state,
};
use scsynth_client::scsynth_osc::command::group::{
    GDeepFree, GFreeAll, GHead, GNew, GQueryTree, GTail, GroupMove, NewGroup, TreeRequest,
};
use scsynth_client::scsynth_osc::command::node::{
    ControlMapping, NAfter, NBefore, NFree, NMap, NQuery, NRun, NSet, NodeMove, NodeRun,
};
use scsynth_client::scsynth_osc::command::synth::{SGet, SNew};
use scsynth_client::scsynth_osc::command::top_level::Notify;
use scsynth_client::scsynth_osc::reply::command_replies::Done;
use scsynth_client::scsynth_osc::reply::group::{
    GQueryTreeReply, TreeControl, TreeGroup, TreeNode, TreeSynth, TreeValue,
};
use scsynth_client::scsynth_osc::reply::synth::{NSet as NSetReply, ValueReading};
use scsynth_client::scsynth_osc::{
    AddAction, Command, Control, ControlSetting, NewNodeId, NodeId, Packet, Reply,
};
use scsynth_client::{Error, Incoming, Options, Target};

/// The limit of `ended` in the tests, which is not the limit of the client.
const LIMIT: Duration = Duration::from_secs(60);

fn new_id(node: i32) -> NewNodeId {
    NewNodeId::new(node).expect("a node id")
}

fn n_end(node: i32) -> Incoming {
    Incoming::Reply(Reply::NEnd(synth_state(node)))
}

fn freq(value: f32) -> ControlSetting {
    ControlSetting::new("freq", value)
}

#[test]
fn the_futures_of_the_node_methods_are_send() {
    fn send<T: Send>(_: &T) {}
    let (scsynth, _peer, _timer) = connect(Options::default());
    let root = Target::head(NodeId::ROOT);
    let node = NodeId(1000);
    send(&scsynth.group(root));
    send(&scsynth.synth("sine", [], root));
    send(&scsynth.free(node));
    send(&scsynth.run(node, true));
    send(&scsynth.free_all(node));
    send(&scsynth.deep_free(node));
    send(&scsynth.move_node(node, root));
    send(&scsynth.query(node));
    send(&scsynth.tree(node, false));
    send(&scsynth.get(node, "freq"));
    send(&scsynth.ended(node, LIMIT));
}

#[test]
fn group_and_synth_send_the_next_node_ids_of_the_client_and_need_no_n_go() {
    let (scsynth, _peer, packets, _timer) = registered(|command| match command {
        Command::Notify(Notify { on: true, .. }) => vec![Reply::Done(Done::Notify {
            client_id: 1,
            max_logins: Some(4),
        })],
        other => answer(other),
    });
    let first = (1 << 26) + 1000;
    assert_eq!(
        now(scsynth.group(Target::head(NodeId::ROOT))),
        Ok(NodeId(first))
    );
    assert_eq!(
        now(scsynth.synth("sine", [freq(440.0)], Target::tail(NodeId(first)))),
        Ok(NodeId(first + 1))
    );
    assert_eq!(
        now(scsynth.group(Target::before(NodeId(first + 1)))),
        Ok(NodeId(first + 2))
    );
    assert_eq!(
        now(scsynth.synth("sine", [], Target::after(NodeId(first + 1)))),
        Ok(NodeId(first + 3))
    );
    let group = |id, action, target| {
        Command::GNew(GNew {
            groups: vec![NewGroup {
                id: new_id(id),
                action,
                target: NodeId(target),
            }],
        })
    };
    let synth = |id, action, target, controls| {
        Command::SNew(SNew {
            def: "sine".to_owned(),
            id: new_id(id),
            action,
            target: NodeId(target),
            controls,
        })
    };
    assert_eq!(
        all(&packets),
        [
            request(vec![group(first, AddAction::Head, 0)], 2),
            request(
                vec![synth(first + 1, AddAction::Tail, first, vec![freq(440.0)])],
                3
            ),
            request(vec![group(first + 2, AddAction::Before, first + 1)], 4),
            request(
                vec![synth(first + 3, AddAction::After, first + 1, Vec::new())],
                5
            ),
        ]
    );
}

#[test]
fn a_failed_synth_gives_failed_and_its_node_id_stays_taken() {
    let (scsynth, _peer, packets, _timer) = registered(|command| match command {
        Command::SNew(new) if new.def == "missing" => {
            vec![fail("/s_new", "SynthDef not found", None)]
        }
        Command::SNew(new) => vec![n_go(new.id.get())],
        other => answer(other),
    });
    let root = Target::head(NodeId::ROOT);
    assert_eq!(
        now(scsynth.synth("missing", [], root)),
        Err(failed("/s_new", "SynthDef not found"))
    );
    assert_eq!(now(scsynth.synth("sine", [], root)), Ok(NodeId(1001)));
    let ids: Vec<_> = all(&packets)
        .into_iter()
        .flat_map(Packet::into_messages)
        .filter_map(|command| match command {
            Command::SNew(new) => Some(new.id.get()),
            _ => None,
        })
        .collect();
    assert_eq!(ids, [1000, 1001]);
}

#[test]
fn group_and_synth_before_register_give_not_registered_and_send_nothing() {
    let (scsynth, peer, timer) = connect(Options::default());
    let root = Target::head(NodeId::ROOT);
    assert_eq!(now(scsynth.group(root)), Err(Error::NotRegistered));
    assert_eq!(
        now(scsynth.synth("sine", [], root)),
        Err(Error::NotRegistered)
    );
    assert_eq!(peer.take(), None);
    assert_eq!(timer.limits(), []);
}

#[test]
fn free_run_free_all_and_deep_free_send_their_command() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, answer);
    let node = NodeId(1000);
    let group = NodeId(1001);
    assert_eq!(now(scsynth.free(node)), Ok(()));
    assert_eq!(now(scsynth.run(node, false)), Ok(()));
    assert_eq!(now(scsynth.run(node, true)), Ok(()));
    assert_eq!(now(scsynth.free_all(group)), Ok(()));
    assert_eq!(now(scsynth.deep_free(group)), Ok(()));
    let run = |run| {
        Command::NRun(NRun {
            nodes: vec![NodeRun { node, run }],
        })
    };
    assert_eq!(
        all(&packets),
        [
            request(vec![Command::NFree(NFree { nodes: vec![node] })], 1),
            request(vec![run(false)], 2),
            request(vec![run(true)], 3),
            request(
                vec![Command::GFreeAll(GFreeAll {
                    groups: vec![group]
                })],
                4
            ),
            request(
                vec![Command::GDeepFree(GDeepFree {
                    groups: vec![group]
                })],
                5
            ),
        ]
    );
}

#[test]
fn a_free_of_a_missing_node_gives_failed() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let _packets = serve(&peer, |command| match command {
        Command::NFree(_) => vec![fail("/n_free", "Node 9999 not found", None)],
        other => answer(other),
    });
    assert_eq!(
        now(scsynth.free(NodeId(9999))),
        Err(failed("/n_free", "Node 9999 not found"))
    );
}

#[test]
fn move_node_sends_the_command_of_each_target() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, answer);
    let node = NodeId(1001);
    let group = NodeId(1000);
    let other = NodeId(1002);
    let into = GroupMove { group, node };
    let next_to = NodeMove {
        node,
        target: other,
    };
    let moves = [
        (
            Target::head(group),
            Command::GHead(GHead { moves: vec![into] }),
        ),
        (
            Target::tail(group),
            Command::GTail(GTail { moves: vec![into] }),
        ),
        (
            Target::before(other),
            Command::NBefore(NBefore {
                moves: vec![next_to],
            }),
        ),
        (
            Target::after(other),
            Command::NAfter(NAfter {
                moves: vec![next_to],
            }),
        ),
    ];
    for (sync, (target, command)) in (1..).zip(moves) {
        assert_eq!(now(scsynth.move_node(node, target)), Ok(()));
        assert_eq!(all(&packets), [request(vec![command], sync)]);
    }
}

#[test]
fn query_gives_the_n_info_of_its_node_and_skips_another() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let info = |node| Reply::NInfo(synth_state(node));
    let packets = serve(&peer, move |command| match command {
        Command::NQuery(query) if query.nodes == [NodeId(1000)] => vec![info(2000), info(1000)],
        Command::NQuery(_) => vec![info(2000)],
        other => answer(other),
    });
    assert_eq!(now(scsynth.query(NodeId(1000))), Ok(synth_state(1000)));
    assert_eq!(
        now(scsynth.query(NodeId(1001))),
        Err(Error::NoReply {
            address: "/n_query"
        })
    );
    let query = |node| {
        Command::NQuery(NQuery {
            nodes: vec![NodeId(node)],
        })
    };
    assert_eq!(
        all(&packets),
        [request(vec![query(1000)], 1), request(vec![query(1001)], 2)]
    );
}

#[test]
fn a_query_of_a_missing_node_gives_failed() {
    let (scsynth, peer, _timer) = connect(Options::default());
    let _packets = serve(&peer, |command| match command {
        Command::NQuery(_) => vec![fail("/n_query", "Node 9999 not found", None)],
        other => answer(other),
    });
    assert_eq!(
        now(scsynth.query(NodeId(9999))),
        Err(failed("/n_query", "Node 9999 not found"))
    );
}

#[test]
fn tree_gives_the_reply_of_its_group() {
    let tree = GQueryTreeReply::WithControls(TreeGroup {
        id: NodeId::ROOT,
        children: vec![TreeNode::Synth(TreeSynth {
            id: NodeId(1000),
            def: "sine".to_owned(),
            controls: vec![TreeControl {
                control: Control::from("freq"),
                value: TreeValue::Float(440.0),
            }],
        })],
    });
    let reply = tree.clone();
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, move |command| match command {
        Command::GQueryTree(query) if query.groups[0].group == NodeId::ROOT => {
            vec![Reply::GQueryTreeReply(reply.clone())]
        }
        other => answer(other),
    });
    assert_eq!(now(scsynth.tree(NodeId::ROOT, true)), Ok(tree));
    assert_eq!(
        now(scsynth.tree(NodeId(1000), false)),
        Err(Error::NoReply {
            address: "/g_queryTree"
        })
    );
    let query = |group, controls| {
        Command::GQueryTree(GQueryTree {
            groups: vec![TreeRequest { group, controls }],
        })
    };
    assert_eq!(
        all(&packets),
        [
            request(vec![query(NodeId::ROOT, true)], 1),
            request(vec![query(NodeId(1000), false)], 2)
        ]
    );
}

#[test]
fn get_gives_the_value_in_the_n_set_of_its_node() {
    let reading = |node, value| {
        Reply::NSet(NSetReply {
            node: NodeId(node),
            controls: vec![ValueReading {
                control: Control::from("freq"),
                value,
            }],
        })
    };
    let (scsynth, peer, _timer) = connect(Options::default());
    let packets = serve(&peer, move |command| match command {
        Command::SGet(get) if get.synth == NodeId(1000) => {
            vec![reading(2000, 220.0), reading(1000, 440.0)]
        }
        Command::SGet(_) => vec![reading(2000, 220.0)],
        other => answer(other),
    });
    assert_eq!(now(scsynth.get(NodeId(1000), "freq")), Ok(440.0));
    assert_eq!(
        now(scsynth.get(NodeId(1001), "freq")),
        Err(Error::NoReply { address: "/s_get" })
    );
    let get = |node| {
        Command::SGet(SGet {
            synth: NodeId(node),
            controls: vec![Control::from("freq")],
        })
    };
    assert_eq!(
        all(&packets),
        [request(vec![get(1000)], 1), request(vec![get(1001)], 2)]
    );
}

#[test]
fn set_and_map_are_plain_messages_that_wait_for_nothing() {
    let (scsynth, peer, timer) = connect(Options::default());
    let mut flight = pin!(scsynth.sync());
    assert!(poll(flight.as_mut()).is_pending());
    let node = NodeId(1000);
    let mapping = ControlMapping {
        control: Control::from("amp"),
        bus: 3,
    };
    assert_eq!(scsynth.set(node, [freq(660.0)]), Ok(()));
    assert_eq!(scsynth.map(node, [mapping.clone()]), Ok(()));
    assert_eq!(peer.take(), Some(request(Vec::new(), 1)));
    assert_eq!(
        peer.take(),
        Some(Packet::Message(Command::NSet(NSet {
            node,
            controls: vec![freq(660.0)],
        })))
    );
    assert_eq!(
        peer.take(),
        Some(Packet::Message(Command::NMap(NMap {
            node,
            mappings: vec![mapping],
        })))
    );
    assert_eq!(peer.take(), None);
    assert_eq!(timer.limits(), [Options::default().timeout]);
    peer.send(synced(1));
    assert_eq!(poll(flight.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn ended_keeps_an_n_end_that_comes_between_its_call_and_its_first_poll() {
    let (scsynth, peer, packets, timer) = registered(answer);
    let ended = scsynth.ended(NodeId(1000), LIMIT);
    peer.send(n_end(1000));
    assert_eq!(now(ended), Ok(()));
    assert_eq!(timer.limits(), []);
    assert_eq!(all(&packets), []);
}

#[test]
fn ended_skips_the_n_end_of_another_node_and_its_limit_reaches_the_timer() {
    let (scsynth, peer, _packets, timer) = registered(answer);
    let mut ended = pin!(scsynth.ended(NodeId(1000), LIMIT));
    assert_eq!(timer.limits(), []);
    assert!(poll(ended.as_mut()).is_pending());
    assert_eq!(timer.limits(), [LIMIT]);
    peer.send(n_end(1001));
    assert!(poll(ended.as_mut()).is_pending());
    peer.send(n_end(1000));
    assert_eq!(poll(ended.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn ended_after_the_n_end_of_its_node_times_out() {
    let (scsynth, peer, _packets, timer) = registered(answer);
    peer.send(n_end(1000));
    let mut ended = pin!(scsynth.ended(NodeId(1000), LIMIT));
    assert!(poll(ended.as_mut()).is_pending());
    timer.fire(0);
    assert_eq!(poll(ended.as_mut()), Poll::Ready(Err(Error::Timeout)));
}

#[test]
fn ended_before_register_gives_not_registered_and_waits_for_nothing() {
    let (scsynth, peer, timer) = connect(Options::default());
    let ended = scsynth.ended(NodeId(1000), LIMIT);
    peer.send(n_end(1000));
    assert_eq!(now(ended), Err(Error::NotRegistered));
    assert_eq!(timer.limits(), []);
}
