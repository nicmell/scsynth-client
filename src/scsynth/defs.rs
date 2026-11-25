use scsynth_osc::Command;
use scsynth_osc::command::synth_def::{DFree, DLoad, DRecv};
use scsynth_osc::reply::command_replies::Done;
use scsynth_synthdef::{SynthDef, read_def_file};

use super::Scsynth;
use crate::error::Error;

impl Scsynth {
    /// Sends the synth definition `def`: a request of `/d_recv` with the
    /// bytes of `def`, which ends at its `/done /d_recv`.
    ///
    /// # Errors
    ///
    /// [`Error::Limit`] when `def` breaks a limit of scsynth, with nothing
    /// sent, and else as [`Self::request`]. [`Error::NoReply`] without a
    /// `/done /d_recv`.
    pub async fn add_synthdef(&self, def: &SynthDef) -> Result<(), Error> {
        let data = def.as_bytes().map_err(Error::Limit)?;
        self.d_recv(data).await
    }

    /// Sends the bytes of a synth definition file: a request of `/d_recv`
    /// with `bytes`, once they read as a definition file, which ends at its
    /// `/done /d_recv`.
    ///
    /// # Errors
    ///
    /// [`Error::Read`] when `bytes` are no definition file, with nothing
    /// sent, and else as [`Self::request`]. [`Error::NoReply`] without a
    /// `/done /d_recv`.
    pub async fn add_synthdef_bytes(&self, bytes: &[u8]) -> Result<(), Error> {
        read_def_file(bytes).map_err(Error::Read)?;
        self.d_recv(bytes.to_vec()).await
    }

    /// Loads the synth definitions of the files at `path`, a path on the
    /// host of scsynth: a request of `/d_load`, which ends at its
    /// `/done /d_load`.
    ///
    /// # Errors
    ///
    /// As [`Self::request`]. [`Error::NoReply`] without a `/done /d_load`.
    pub async fn load_synthdef(&self, path: impl Into<String>) -> Result<(), Error> {
        let command = Command::DLoad(DLoad {
            path: path.into(),
            completion: None,
        });
        self.request_done(command, Done::DLoad).await.map(drop)
    }

    /// Removes the synth definition `name`: a request of `/d_free`. It gives
    /// `Ok` also for a name that scsynth does not have.
    ///
    /// # Errors
    ///
    /// As [`Self::request`].
    pub async fn free_synthdef(&self, name: impl Into<String>) -> Result<(), Error> {
        let command = Command::DFree(DFree {
            names: vec![name.into()],
        });
        self.request(vec![command]).await.map(drop)
    }

    async fn d_recv(&self, data: Vec<u8>) -> Result<(), Error> {
        let command = Command::DRecv(DRecv {
            data,
            completion: None,
        });
        self.request_done(command, Done::DRecv).await.map(drop)
    }
}
