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
