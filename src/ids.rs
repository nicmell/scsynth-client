use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::ops::Range;

use scsynth_osc::NodeId;

use crate::error::Error;
use crate::options::Options;

/// A kind of id that a registered client takes in runs from a range of its
/// own.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum IdKind {
    /// The index of a buffer.
    Buffer,
    /// The index of a control bus.
    ControlBus,
    /// The index of an audio bus.
    AudioBus,
}

/// The number of client ids that own a range of node ids, from 0.
const NODE_CLIENTS: i32 = 32;
/// The shift of a client id to the first node id of its range.
const NODE_SHIFT: u32 = 26;
/// The first node id that a client gives, from the start of its range.
const FIRST_NODE: i32 = 1000;
/// The last node id of a range, from its start.
const LAST_NODE: i32 = 0x03FF_FFFF;

/// The ids of one registered client: its node ids, and its runs of buffers,
/// control buses and audio buses.
pub(crate) struct Ids {
    /// The first node id of the range of the client.
    node_base: i32,
    /// The next node id, from `node_base`.
    next_node: i32,
    buffers: Runs,
    control_buses: Runs,
    audio_buses: Runs,
}

impl Ids {
    /// The ids of the client `client_id` under the login limit `max_logins`,
    /// on a server with the counts of `options`. `None` when the client id
    /// has no range of node ids.
    pub(crate) fn new(client_id: i32, max_logins: i32, options: &Options) -> Option<Self> {
        if !(0..NODE_CLIENTS).contains(&client_id) {
            return None;
        }
        let first_private = i64::from(options.outputs) + i64::from(options.inputs);
        let range = |first, count| share(first, count, max_logins, client_id);
        Some(Self {
            node_base: client_id << NODE_SHIFT,
            next_node: FIRST_NODE,
            buffers: Runs::new(range(0, options.buffers)),
            control_buses: Runs::new(range(0, options.control_buses)),
            audio_buses: Runs::new(range(first_private, options.audio_buses)),
        })
    }

    /// The next node id: one over the last, and back to the first after the
    /// end of the range.
    pub(crate) fn next_node(&mut self) -> NodeId {
        let id = self.node_base + self.next_node;
        self.next_node = if self.next_node == LAST_NODE {
            FIRST_NODE
        } else {
            self.next_node + 1
        };
        NodeId(id)
    }

    /// The first id of a run of `count` free ids of `kind`, which the client
    /// now holds.
    pub(crate) fn alloc(&mut self, kind: IdKind, count: NonZeroU32) -> Result<i32, Error> {
        self.runs(kind).alloc(count).ok_or(Error::Exhausted(kind))
    }

    /// Gives back the run of ids of `kind` that starts at `first`. Nothing
    /// changes when no run that the client holds starts there.
    pub(crate) fn free(&mut self, kind: IdKind, first: i32) {
        self.runs(kind).free(first);
    }

    fn runs(&mut self, kind: IdKind) -> &mut Runs {
        match kind {
            IdKind::Buffer => &mut self.buffers,
            IdKind::ControlBus => &mut self.control_buses,
            IdKind::AudioBus => &mut self.audio_buses,
        }
    }
}

/// The share of the client `client_id` of the ids from `first` to `count`,
/// split equally among `max_logins` clients. Empty when the share is 0, and
/// when the range leaves `0..=i32::MAX`.
fn share(first: i64, count: i32, max_logins: i32, client_id: i32) -> Range<i32> {
    if max_logins <= 0 {
        return 0..0;
    }
    let share = (i64::from(count) - first) / i64::from(max_logins);
    if share <= 0 {
        return 0..0;
    }
    let start = first + share * i64::from(client_id);
    match (i32::try_from(start), i32::try_from(start + share)) {
        (Ok(start), Ok(end)) if start >= 0 => start..end,
        _ => 0..0,
    }
}

/// Runs of ids in one range: an alloc takes the first free run that fits,
/// lowest id first.
struct Runs {
    /// The free runs, in order, none next to another.
    free: Vec<Range<i32>>,
    /// The end of each run that the client holds, by its first id.
    held: BTreeMap<i32, i32>,
}

impl Runs {
    fn new(range: Range<i32>) -> Self {
        let free = if range.is_empty() {
            Vec::new()
        } else {
            vec![range]
        };
        Self {
            free,
            held: BTreeMap::new(),
        }
    }

    fn alloc(&mut self, count: NonZeroU32) -> Option<i32> {
        let count = i32::try_from(count.get()).ok()?;
        let index = self
            .free
            .iter()
            .position(|run| run.end - run.start >= count)?;
        let run = &mut self.free[index];
        let first = run.start;
        run.start += count;
        if run.start == run.end {
            self.free.remove(index);
        }
        self.held.insert(first, first + count);
        Some(first)
    }

    fn free(&mut self, first: i32) {
        let Some(end) = self.held.remove(&first) else {
            return;
        };
        let index = self.free.partition_point(|run| run.start < first);
        let mut run = first..end;
        if self.free.get(index).is_some_and(|next| next.start == end) {
            run.end = self.free.remove(index).end;
        }
        match index.checked_sub(1).map(|before| &mut self.free[before]) {
            Some(before) if before.end == first => before.end = run.end,
            _ => self.free.insert(index, run),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KINDS: [IdKind; 3] = [IdKind::Buffer, IdKind::ControlBus, IdKind::AudioBus];

    fn ids(client_id: i32, max_logins: i32) -> Ids {
        Ids::new(client_id, max_logins, &Options::default()).expect("a node range")
    }

    fn count(count: u32) -> NonZeroU32 {
        NonZeroU32::new(count).expect("a count over 0")
    }

    fn free_runs(ids: &mut Ids, kind: IdKind) -> Vec<Range<i32>> {
        ids.runs(kind).free.clone()
    }

    #[test]
    fn the_node_ids_of_the_clients_0_1_and_31() {
        // NodeIDAllocator.alloc of Engine.sc: `x | (user << 26)`, then
        // `(x + 1).wrap(initTemp, 0x03FFFFFF)`, with initTemp the
        // `initialNodeID` of ServerOptions in Server.sc, 1000.
        for (client_id, first, last) in [
            (0, 1000, 0x03FF_FFFF),
            (1, 0x0400_03E8, 0x07FF_FFFF),
            (31, 0x7C00_03E8, i32::MAX),
        ] {
            let mut ids = ids(client_id, 32);
            assert_eq!(ids.next_node(), NodeId(first));
            assert_eq!(ids.next_node(), NodeId(first + 1));
            ids.next_node = LAST_NODE;
            assert_eq!(ids.next_node(), NodeId(last));
            assert_eq!(ids.next_node(), NodeId(first));
        }
    }

    #[test]
    fn a_client_id_under_0_or_over_31_has_no_node_range() {
        // NodeIDAllocator.new of Engine.sc: "NodeIDAllocator user id > 31".
        for client_id in [-1, 32] {
            assert!(Ids::new(client_id, 64, &Options::default()).is_none());
        }
    }

    #[test]
    fn the_buffers_of_the_clients_0_1_and_31() {
        // Server.newBufferAllocators of Server.sc: `numBuffers div:
        // maxNumClients` for each client, from `numBuffersPerClient *
        // clientID`, with `reservedNumBuffers` 0.
        for (max_logins, ranges) in [
            (32, [0..32, 32..64, 992..1024]),
            (4, [0..256, 256..512, 7936..8192]),
        ] {
            for (client_id, range) in [0, 1, 31].into_iter().zip(ranges) {
                let mut ids = ids(client_id, max_logins);
                assert_eq!(free_runs(&mut ids, IdKind::Buffer), [range]);
            }
        }
    }

    #[test]
    fn the_control_buses_of_the_clients_0_1_and_31() {
        // Server.newBusAllocators of Server.sc: `numControlBusChannels div:
        // maxNumClients` for each client, from `numControlPerClient *
        // clientID`, with `reservedNumControlBusChannels` 0.
        for (max_logins, ranges) in [
            (32, [0..512, 512..1024, 15872..16384]),
            (4, [0..4096, 4096..8192, 126_976..131_072]),
        ] {
            for (client_id, range) in [0, 1, 31].into_iter().zip(ranges) {
                let mut ids = ids(client_id, max_logins);
                assert_eq!(free_runs(&mut ids, IdKind::ControlBus), [range]);
            }
        }
    }

    #[test]
    fn the_audio_buses_of_the_clients_0_1_and_31() {
        // Server.newBusAllocators of Server.sc: `numAudioBusChannels -
        // firstPrivateBus div: maxNumClients` for each client, from
        // `numAudioPerClient * clientID + firstPrivateBus`, with
        // `firstPrivateBus` the outputs plus the inputs, and
        // `reservedNumAudioBusChannels` 0.
        for (max_logins, ranges) in [
            (32, [16..47, 47..78, 977..1008]),
            (4, [16..268, 268..520, 7828..8080]),
        ] {
            for (client_id, range) in [0, 1, 31].into_iter().zip(ranges) {
                let mut ids = ids(client_id, max_logins);
                assert_eq!(free_runs(&mut ids, IdKind::AudioBus), [range]);
            }
        }
    }

    #[test]
    fn an_alloc_takes_the_first_free_run_that_fits() {
        let mut runs = Runs::new(0..10);
        assert_eq!(runs.alloc(count(3)), Some(0));
        assert_eq!(runs.alloc(count(2)), Some(3));
        runs.free(0);
        assert_eq!(runs.alloc(count(4)), Some(5));
        assert_eq!(runs.alloc(count(2)), Some(0));
        assert_eq!(runs.free, [2..3, 9..10]);
    }

    #[test]
    fn a_run_that_does_not_fit_gives_nothing() {
        let mut runs = Runs::new(0..10);
        assert_eq!(runs.alloc(count(11)), None);
        assert_eq!(runs.alloc(count(u32::MAX)), None);
        assert_eq!(runs.alloc(count(10)), Some(0));
        assert_eq!(runs.alloc(count(1)), None);
    }

    #[test]
    fn a_free_joins_the_run_with_its_free_neighbours() {
        let whole = 0..10;
        let mut runs = Runs::new(whole.clone());
        let held = [2, 2, 2].map(|size| runs.alloc(count(size)));
        assert_eq!(held, [Some(0), Some(2), Some(4)]);
        runs.free(0);
        assert_eq!(runs.free, [0..2, 6..10]);
        runs.free(4);
        assert_eq!(runs.free, [0..2, 4..10]);
        runs.free(2);
        assert_eq!(runs.free, [whole]);
    }

    #[test]
    fn a_free_of_a_run_that_is_not_held_changes_nothing() {
        let mut runs = Runs::new(0..10);
        assert_eq!(runs.alloc(count(4)), Some(0));
        assert_eq!(runs.alloc(count(4)), Some(4));
        runs.free(0);
        runs.free(0);
        runs.free(5);
        runs.free(20);
        assert_eq!(runs.free, [0..4, 8..10]);
        assert_eq!(runs.alloc(count(4)), Some(0));
    }

    #[test]
    fn an_empty_range_gives_exhausted() {
        let tight = Options {
            buffers: 3,
            control_buses: 3,
            audio_buses: 19,
            ..Options::default()
        };
        let past_i32 = Options {
            buffers: i32::MAX,
            control_buses: i32::MAX,
            audio_buses: i32::MAX,
            ..Options::default()
        };
        let below_0 = Options {
            buffers: -4,
            control_buses: -4,
            inputs: -100,
            ..Options::default()
        };
        for (client_id, max_logins, options) in [
            (0, 0, Options::default()),
            (0, -1, Options::default()),
            (0, 4, tight),
            (31, 1, past_i32),
            (0, 1, below_0),
        ] {
            let mut ids = Ids::new(client_id, max_logins, &options).expect("a node range");
            for kind in KINDS {
                assert_eq!(ids.alloc(kind, count(1)), Err(Error::Exhausted(kind)));
            }
        }
    }
}
