use std::future::Future;
use std::time::Duration;

use scsynth_osc::command::group::{
    GDeepFree, GFreeAll, GHead, GNew, GQueryTree, GTail, GroupMove, NewGroup, TreeRequest,
};
use scsynth_osc::command::node::{
    ControlMapping, NAfter, NBefore, NFree, NMap, NQuery, NRun, NSet, NodeMove, NodeRun,
};
use scsynth_osc::command::synth::{SGet, SNew};
use scsynth_osc::reply::group::GQueryTreeReply;
use scsynth_osc::reply::node_notification::NodeState;
use scsynth_osc::{AddAction, Command, Control, ControlSetting, NewNodeId, NodeId, Packet, Reply};

use super::Scsynth;
use crate::core::Wait;
use crate::error::Error;

/// Where a new node goes, or where a node moves: the head or the tail of a
/// group, or just before or just after a node. The root group of scsynth is
/// [`NodeId::ROOT`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Target {
    place: Place,
    node: NodeId,
}

/// The place of a [`Target`], next to its node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Place {
    Head,
    Tail,
    Before,
    After,
}

impl Target {
    /// At the head of `group`, where a node runs first.
    #[must_use]
    pub const fn head(group: NodeId) -> Self {
        Self {
            place: Place::Head,
            node: group,
        }
    }

    /// At the tail of `group`, where a node runs last.
    #[must_use]
    pub const fn tail(group: NodeId) -> Self {
        Self {
            place: Place::Tail,
            node: group,
        }
    }

    /// Just before `node`, in the group of `node`.
    #[must_use]
    pub const fn before(node: NodeId) -> Self {
        Self {
            place: Place::Before,
            node,
        }
    }

    /// Just after `node`, in the group of `node`.
    #[must_use]
    pub const fn after(node: NodeId) -> Self {
        Self {
            place: Place::After,
            node,
        }
    }
}

impl Place {
    /// The add action of `/g_new` and `/s_new` for this place.
    const fn action(self) -> AddAction {
        match self {
            Self::Head => AddAction::Head,
            Self::Tail => AddAction::Tail,
            Self::Before => AddAction::Before,
            Self::After => AddAction::After,
        }
    }
}

impl Scsynth {
    /// Makes a group at `target`, with the next node id of the client: a
    /// request of `/g_new`. It gives the id of the group.
    ///
    /// # Errors
    ///
    /// [`Error::NotRegistered`] before [`Self::register`], with nothing
    /// sent, and else as [`Self::request`]. The node id stays taken when the
    /// request fails.
    pub async fn group(&self, target: Target) -> Result<NodeId, Error> {
        let id = self.next_node_id()?;
        let group = NewGroup {
            id: new_node(id),
            action: target.place.action(),
            target: target.node,
        };
        let command = Command::GNew(GNew {
            groups: vec![group],
        });
        self.request(vec![command]).await?;
        Ok(id)
    }

    /// Makes a synth of the synth definition `name` at `target`, with the
    /// next node id of the client and the values of `controls`: a request of
    /// `/s_new`. It gives the id of the synth.
    ///
    /// # Errors
    ///
    /// [`Error::NotRegistered`] before [`Self::register`], with nothing
    /// sent, and else as [`Self::request`]. The node id stays taken when the
    /// request fails.
    pub async fn synth(
        &self,
        name: impl Into<String>,
        controls: impl IntoIterator<Item = ControlSetting>,
        target: Target,
    ) -> Result<NodeId, Error> {
        let id = self.next_node_id()?;
        let synth = SNew {
            def: name.into(),
            id: new_node(id),
            action: target.place.action(),
            target: target.node,
            controls: controls.into_iter().collect(),
        };
        self.request(vec![Command::SNew(synth)]).await?;
        Ok(id)
    }

    /// Frees `node`: a request of `/n_free`.
    ///
    /// # Errors
    ///
    /// As [`Self::request`]: [`Error::Failed`] for a node that scsynth does
    /// not have.
    pub async fn free(&self, node: NodeId) -> Result<(), Error> {
        let command = Command::NFree(NFree { nodes: vec![node] });
        self.request(vec![command]).await.map(drop)
    }

    /// Turns `node` on or off: a request of `/n_run`.
    ///
    /// # Errors
    ///
    /// As [`Self::request`].
    pub async fn run(&self, node: NodeId, on: bool) -> Result<(), Error> {
        let command = Command::NRun(NRun {
            nodes: vec![NodeRun { node, run: on }],
        });
        self.request(vec![command]).await.map(drop)
    }

    /// Frees every node in `group`: a request of `/g_freeAll`.
    ///
    /// # Errors
    ///
    /// As [`Self::request`].
    pub async fn free_all(&self, group: NodeId) -> Result<(), Error> {
        let command = Command::GFreeAll(GFreeAll {
            groups: vec![group],
        });
        self.request(vec![command]).await.map(drop)
    }

    /// Frees every synth in `group` and in the groups inside it, and keeps
    /// the groups: a request of `/g_deepFree`.
    ///
    /// # Errors
    ///
    /// As [`Self::request`].
    pub async fn deep_free(&self, group: NodeId) -> Result<(), Error> {
        let command = Command::GDeepFree(GDeepFree {
            groups: vec![group],
        });
        self.request(vec![command]).await.map(drop)
    }

    /// Moves `node` to `target`: a request of `/g_head`, `/g_tail`,
    /// `/n_before` or `/n_after`, by the place of `target`.
    ///
    /// # Errors
    ///
    /// As [`Self::request`].
    pub async fn move_node(&self, node: NodeId, target: Target) -> Result<(), Error> {
        let Target {
            place,
            node: target,
        } = target;
        let command = match place {
            Place::Head => Command::GHead(GHead {
                moves: vec![GroupMove {
                    group: target,
                    node,
                }],
            }),
            Place::Tail => Command::GTail(GTail {
                moves: vec![GroupMove {
                    group: target,
                    node,
                }],
            }),
            Place::Before => Command::NBefore(NBefore {
                moves: vec![NodeMove { node, target }],
            }),
            Place::After => Command::NAfter(NAfter {
                moves: vec![NodeMove { node, target }],
            }),
        };
        self.request(vec![command]).await.map(drop)
    }

    /// The state of `node`: the `/n_info` of `node` among the replies of a
    /// request of `/n_query`.
    ///
    /// # Errors
    ///
    /// As [`Self::request`]: [`Error::Failed`] for a node that scsynth does
    /// not have. [`Error::NoReply`] without an `/n_info` of `node`.
    pub async fn query(&self, node: NodeId) -> Result<NodeState, Error> {
        let command = Command::NQuery(NQuery { nodes: vec![node] });
        let replies = self.request(vec![command]).await?;
        replies
            .into_iter()
            .find_map(|reply| match reply {
                Reply::NInfo(state) if state.node == node => Some(state),
                _ => None,
            })
            .ok_or(Error::NoReply {
                address: "/n_query",
            })
    }

    /// The tree of `group`, with the values of the controls of its synths
    /// when `controls` is true: the `/g_queryTree.reply` of `group` among
    /// the replies of a request of `/g_queryTree`.
    ///
    /// # Errors
    ///
    /// As [`Self::request`]. [`Error::NoReply`] without a
    /// `/g_queryTree.reply` of `group`.
    pub async fn tree(&self, group: NodeId, controls: bool) -> Result<GQueryTreeReply, Error> {
        let command = Command::GQueryTree(GQueryTree {
            groups: vec![TreeRequest { group, controls }],
        });
        let replies = self.request(vec![command]).await?;
        replies
            .into_iter()
            .find_map(|reply| match reply {
                Reply::GQueryTreeReply(tree) if root(&tree) == group => Some(tree),
                _ => None,
            })
            .ok_or(Error::NoReply {
                address: "/g_queryTree",
            })
    }

    /// The value of `control` of the synth `node`: the value in the `/n_set`
    /// of `node` among the replies of a request of `/s_get`.
    ///
    /// # Errors
    ///
    /// As [`Self::request`]. [`Error::NoReply`] without an `/n_set` of
    /// `node` that holds `control`.
    pub async fn get(&self, node: NodeId, control: impl Into<Control>) -> Result<f32, Error> {
        let control = control.into();
        let command = Command::SGet(SGet {
            synth: node,
            controls: vec![control.clone()],
        });
        let replies = self.request(vec![command]).await?;
        replies
            .into_iter()
            .find_map(|reply| match reply {
                Reply::NSet(set) if set.node == node => set
                    .controls
                    .into_iter()
                    .find(|reading| reading.control == control)
                    .map(|reading| reading.value),
                _ => None,
            })
            .ok_or(Error::NoReply { address: "/s_get" })
    }

    /// Sets `controls` of `node`: a plain send of one `/n_set`, which waits
    /// for nothing.
    ///
    /// # Errors
    ///
    /// As [`Self::send`].
    pub fn set(
        &self,
        node: NodeId,
        controls: impl IntoIterator<Item = ControlSetting>,
    ) -> Result<(), Error> {
        let set = NSet {
            node,
            controls: controls.into_iter().collect(),
        };
        self.send(&Packet::Message(Command::NSet(set)))
    }

    /// Maps controls of `node` to control buses: a plain send of one
    /// `/n_map`, which waits for nothing.
    ///
    /// # Errors
    ///
    /// As [`Self::send`].
    pub fn map(
        &self,
        node: NodeId,
        mappings: impl IntoIterator<Item = ControlMapping>,
    ) -> Result<(), Error> {
        let map = NMap {
            node,
            mappings: mappings.into_iter().collect(),
        };
        self.send(&Packet::Message(Command::NMap(map)))
    }

    /// A future that ends at the `/n_end` of `node`, within `limit`.
    ///
    /// Its wait starts at this call: an `/n_end` that comes before the first
    /// poll counts, and one that came before this call does not. The sleep
    /// of `limit` starts at the first poll.
    ///
    /// # Errors
    ///
    /// The future gives [`Error::NotRegistered`] when the client is not
    /// registered at this call, [`Error::Timeout`] when no `/n_end` of
    /// `node` comes within `limit`, and [`Error::Send`] when the transport
    /// closes.
    pub fn ended(
        &self,
        node: NodeId,
        limit: Duration,
    ) -> impl Future<Output = Result<(), Error>> + '_ {
        let registered = self.shared.lock().registered;
        let call = registered.then(|| self.shared.start(|core| core.wait(Wait::End(node))));
        async move {
            let call = call.ok_or(Error::NotRegistered)?;
            call.outcome(limit).await.map(drop)
        }
    }
}

/// The id of a new node, from a node id of the range of the client.
fn new_node(id: NodeId) -> NewNodeId {
    // The ranges of the client ids 0 to 31 hold no negative node id.
    NewNodeId::new(id.0).expect("a node id of a range is 0 or more")
}

/// The group at the root of a tree.
fn root(tree: &GQueryTreeReply) -> NodeId {
    match tree {
        GQueryTreeReply::WithoutControls(group) => group.id,
        GQueryTreeReply::WithControls(group) => group.id,
    }
}
