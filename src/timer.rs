use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::client::{MaybeSend, MaybeSync};

/// A future that ends once its time has passed: what [`Timer::sleep`]
/// gives. `Send` on a native target.
#[cfg(not(target_arch = "wasm32"))]
pub type Sleep = Pin<Box<dyn Future<Output = ()> + Send>>;

/// A future that ends once its time has passed: what [`Timer::sleep`]
/// gives. `Send` on a native target.
#[cfg(target_arch = "wasm32")]
pub type Sleep = Pin<Box<dyn Future<Output = ()>>>;

/// The clock of a [`Scsynth`](crate::Scsynth): it makes the sleep that each
/// awaited method races against its limit.
pub trait Timer: MaybeSend + MaybeSync + 'static {
    /// A future that ends once `limit` has passed since this call.
    fn sleep(&self, limit: Duration) -> Sleep;
}

/// The [`Timer`] of tokio: its sleeps are those of [`tokio::time::sleep`].
///
/// A [`Scsynth`](crate::Scsynth) makes each sleep at the first poll of an
/// awaited method. So a runtime of tokio with its time driver, which
/// `enable_time` or `enable_all` turns on, must poll the methods.
#[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TokioTimer;

#[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
impl Timer for TokioTimer {
    fn sleep(&self, limit: Duration) -> Sleep {
        Box::pin(tokio::time::sleep(limit))
    }
}
