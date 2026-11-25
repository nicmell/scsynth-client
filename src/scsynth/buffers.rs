use std::num::NonZeroU32;

use scsynth_osc::command::buffer::{BAlloc, BAllocRead, BFree, BGetn, BQuery};
use scsynth_osc::reply::buffer::BufferInfo;
use scsynth_osc::reply::command_replies::{BufferCommand, Done};
use scsynth_osc::{BufferId, Command, Completion, IndexRange, Reply};

use super::Scsynth;
use crate::error::Error;
use crate::ids::IdKind;

/// A buffer id that a call took from the range of the client. It goes back
/// to the client when it drops, unless the call keeps it.
struct NewBuffer<'a> {
    scsynth: &'a Scsynth,
    buffer: BufferId,
    kept: bool,
}

impl NewBuffer<'_> {
    fn keep(mut self) -> BufferId {
        self.kept = true;
        self.buffer
    }
}

impl Drop for NewBuffer<'_> {
    fn drop(&mut self) {
        if !self.kept {
            self.scsynth.give_back_buffer(self.buffer);
        }
    }
}

impl Scsynth {
    /// Allocates a buffer of `frames` frames of `channels` channels, at the
    /// sample rate of the server, with the next free buffer id of the
    /// client: a request of `/b_alloc`, which ends at the `/done /b_alloc`
    /// of the buffer. It gives the buffer id. A count over `i32::MAX` sends
    /// `i32::MAX`.
    ///
    /// # Errors
    ///
    /// [`Error::NotRegistered`] before [`Self::register`], and
    /// [`Error::Exhausted`] when the range of the client has no free buffer
    /// id, both with nothing sent. Else as [`Self::request`].
    /// [`Error::NoReply`] without the `/done /b_alloc` of the buffer. The
    /// buffer id goes back to the client when the call fails or is dropped.
    pub async fn alloc_buffer(&self, frames: u32, channels: u32) -> Result<BufferId, Error> {
        let new = self.new_buffer()?;
        let buffer = new.buffer;
        let command = Command::BAlloc(BAlloc {
            buffer,
            frames: saturated(frames),
            channels: saturated(channels),
            completion: None,
            sample_rate: 0.0,
        });
        let done = Done::Buffer {
            command: BufferCommand::BAlloc,
            buffer,
        };
        self.request_done(command, done).await?;
        Ok(new.keep())
    }

    /// Allocates a buffer with the next free buffer id of the client, and
    /// reads into it the whole sound file at `path`, a path on the host of
    /// scsynth: a request of one `/b_allocRead`, whose completion is a
    /// `/b_query` of the buffer. It gives the entry of the buffer in the
    /// `/b_info` of that query.
    ///
    /// # Errors
    ///
    /// As [`Self::alloc_buffer`]: [`Error::Failed`] for a file that scsynth
    /// cannot read. [`Error::NoReply`] without a `/b_info` of the buffer.
    pub async fn read_buffer(&self, path: impl Into<String>) -> Result<BufferInfo, Error> {
        let new = self.new_buffer()?;
        let buffer = new.buffer;
        let query = Command::BQuery(BQuery {
            buffers: vec![buffer],
        });
        let command = Command::BAllocRead(BAllocRead {
            buffer,
            path: path.into(),
            file_frame: 0,
            frames: 0,
            completion: Some(Completion::Message(Box::new(query))),
        });
        let replies = self.request(vec![command]).await?;
        let info = entry(replies, buffer).ok_or(Error::NoReply {
            address: "/b_allocRead",
        })?;
        new.keep();
        Ok(info)
    }

    /// Frees `buffer`: a request of `/b_free`, which ends at the
    /// `/done /b_free` of `buffer`. Then the buffer id goes back to the
    /// client, if the client holds it.
    ///
    /// # Errors
    ///
    /// [`Error::OutOfRange`] for a buffer id at or over
    /// [`Options::buffers`](crate::Options::buffers), with nothing sent, and
    /// else as [`Self::request`]. [`Error::NoReply`] without the
    /// `/done /b_free` of `buffer`. On an error, the client keeps the
    /// buffer id.
    pub async fn free_buffer(&self, buffer: BufferId) -> Result<(), Error> {
        self.check_buffer(buffer)?;
        let command = Command::BFree(BFree {
            buffer,
            completion: None,
        });
        let done = Done::Buffer {
            command: BufferCommand::BFree,
            buffer,
        };
        self.request_done(command, done).await?;
        self.give_back_buffer(buffer);
        Ok(())
    }

    /// The frames, the channels and the sample rate of `buffer`: its entry
    /// in the `/b_info` among the replies of a request of `/b_query`.
    ///
    /// # Errors
    ///
    /// [`Error::OutOfRange`] for a buffer id at or over
    /// [`Options::buffers`](crate::Options::buffers), with nothing sent, and
    /// else as [`Self::request`]. [`Error::NoReply`] without a `/b_info` of
    /// `buffer`.
    pub async fn query_buffer(&self, buffer: BufferId) -> Result<BufferInfo, Error> {
        self.check_buffer(buffer)?;
        let command = Command::BQuery(BQuery {
            buffers: vec![buffer],
        });
        let replies = self.request(vec![command]).await?;
        entry(replies, buffer).ok_or(Error::NoReply {
            address: "/b_query",
        })
    }

    /// `count` samples of `buffer`, from the sample `start`: the values of
    /// the run of `start` in the `/b_setn` of `buffer` among the replies of
    /// a request of `/b_getn`. The samples of a buffer count across its
    /// channels, frame after frame. A `start` over `i32::MAX` sends
    /// `i32::MAX`, which scsynth refuses.
    ///
    /// One reply of scsynth holds at most 65,516 bytes (`big_scpacket` of
    /// `common/scsynthsend.h` in 3.14.1), so about 13,000 samples. This
    /// limit comes from the source, and is not measured yet.
    ///
    /// # Errors
    ///
    /// [`Error::OutOfRange`] for a buffer id at or over
    /// [`Options::buffers`](crate::Options::buffers), with nothing sent, and
    /// else as [`Self::request`]. [`Error::NoReply`] without a `/b_setn` of
    /// `buffer` that holds the run of `start`.
    pub async fn get_samples(
        &self,
        buffer: BufferId,
        start: u32,
        count: u16,
    ) -> Result<Vec<f32>, Error> {
        self.check_buffer(buffer)?;
        let start = saturated(start);
        let command = Command::BGetn(BGetn {
            buffer,
            ranges: vec![IndexRange {
                start,
                count: i32::from(count),
            }],
        });
        let replies = self.request(vec![command]).await?;
        replies
            .into_iter()
            .find_map(|reply| match reply {
                Reply::BSetn(set) if set.buffer == buffer => set
                    .ranges
                    .into_iter()
                    .find(|run| run.start == start)
                    .map(|run| run.values),
                _ => None,
            })
            .ok_or(Error::NoReply { address: "/b_getn" })
    }

    /// The next free buffer id of the client, which it holds until the
    /// [`NewBuffer`] drops or is kept.
    fn new_buffer(&self) -> Result<NewBuffer<'_>, Error> {
        let index = self.alloc_ids(IdKind::Buffer, NonZeroU32::MIN)?;
        let buffer = BufferId::new(index).expect("a buffer id of a range is 0 or more");
        Ok(NewBuffer {
            scsynth: self,
            buffer,
            kept: false,
        })
    }

    /// Gives `buffer` back to the client. Nothing changes when the client
    /// does not hold it, or is not registered.
    fn give_back_buffer(&self, buffer: BufferId) {
        let _ = self.free_ids(IdKind::Buffer, buffer.get());
    }

    /// Refuses a buffer id at or over
    /// [`Options::buffers`](crate::Options::buffers).
    fn check_buffer(&self, buffer: BufferId) -> Result<(), Error> {
        let id = buffer.get();
        if id >= self.shared.options.buffers {
            return Err(Error::OutOfRange {
                kind: IdKind::Buffer,
                id,
            });
        }
        Ok(())
    }
}

/// `value` as an `i32`, or `i32::MAX` for a value over it.
fn saturated(value: u32) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

/// The entry of `buffer` in a `/b_info` among `replies`.
fn entry(replies: Vec<Reply>, buffer: BufferId) -> Option<BufferInfo> {
    replies.into_iter().find_map(|reply| match reply {
        Reply::BInfo(info) => info.buffers.into_iter().find(|info| info.buffer == buffer),
        _ => None,
    })
}
