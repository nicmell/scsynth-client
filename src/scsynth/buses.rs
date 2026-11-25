use std::num::NonZeroU32;

use scsynth_osc::command::control_bus::{CGet, CSet};
use scsynth_osc::{Command, IndexValue, Number, Packet, Reply};

use super::Scsynth;
use crate::error::Error;
use crate::ids::IdKind;

impl Scsynth {
    /// The first index of a run of `count` free control buses in the range
    /// of the client, which the client now holds. It sends nothing: scsynth
    /// allocates no bus.
    ///
    /// # Errors
    ///
    /// [`Error::NotRegistered`] before [`Self::register`], and
    /// [`Error::Exhausted`] when no such run is free, also for a `count` of
    /// 0.
    pub fn control_bus(&self, count: u32) -> Result<i32, Error> {
        self.buses(IdKind::ControlBus, count)
    }

    /// The first index of a run of `count` free audio buses in the range of
    /// the client, which the client now holds. It sends nothing: scsynth
    /// allocates no bus.
    ///
    /// # Errors
    ///
    /// [`Error::NotRegistered`] before [`Self::register`], and
    /// [`Error::Exhausted`] when no such run is free, also for a `count` of
    /// 0.
    pub fn audio_bus(&self, count: u32) -> Result<i32, Error> {
        self.buses(IdKind::AudioBus, count)
    }

    /// Gives back the run of control buses that starts at `first`. Nothing
    /// changes when the client holds no run that starts there.
    ///
    /// # Errors
    ///
    /// [`Error::NotRegistered`] before [`Self::register`].
    pub fn free_control_bus(&self, first: i32) -> Result<(), Error> {
        self.free_ids(IdKind::ControlBus, first)
    }

    /// Gives back the run of audio buses that starts at `first`. Nothing
    /// changes when the client holds no run that starts there.
    ///
    /// # Errors
    ///
    /// [`Error::NotRegistered`] before [`Self::register`].
    pub fn free_audio_bus(&self, first: i32) -> Result<(), Error> {
        self.free_ids(IdKind::AudioBus, first)
    }

    /// Sets the control bus `index` to `value`: a plain send of one
    /// `/c_set`, which waits for nothing.
    ///
    /// # Errors
    ///
    /// As [`Self::send`].
    pub fn set_bus(&self, index: i32, value: f32) -> Result<(), Error> {
        let set = CSet {
            buses: vec![IndexValue {
                index,
                value: Number::Float(value),
            }],
        };
        self.send(&Packet::Message(Command::CSet(set)))
    }

    /// The value of the control bus `index`: its value in the `/c_set` among
    /// the replies of a request of `/c_get`.
    ///
    /// # Errors
    ///
    /// As [`Self::request`]: [`Error::Failed`] for an index that scsynth does
    /// not have. [`Error::NoReply`] without a `/c_set` that holds `index`.
    pub async fn get_bus(&self, index: i32) -> Result<f32, Error> {
        let command = Command::CGet(CGet { buses: vec![index] });
        let replies = self.request(vec![command]).await?;
        replies
            .into_iter()
            .find_map(|reply| match reply {
                Reply::CSet(set) => set
                    .buses
                    .into_iter()
                    .find(|bus| bus.index == index)
                    .map(|bus| bus.value),
                _ => None,
            })
            .ok_or(Error::NoReply { address: "/c_get" })
    }

    fn buses(&self, kind: IdKind, count: u32) -> Result<i32, Error> {
        match NonZeroU32::new(count) {
            Some(count) => self.alloc_ids(kind, count),
            None => self.with_ids(|_| Err(Error::Exhausted(kind))),
        }
    }
}
