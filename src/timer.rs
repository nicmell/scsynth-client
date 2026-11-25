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

/// The [`Timer`] of the browser, in a page or in a worker: each sleep is a
/// `setTimeout` of the global object, and ends when its callback runs.
///
/// A sleep over `i32::MAX` milliseconds, about 24.8 days, ends after
/// `i32::MAX` milliseconds. Where the global object has no `setTimeout`,
/// each sleep ends at once.
#[cfg(all(feature = "web", target_arch = "wasm32"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WebTimer;

#[cfg(all(feature = "web", target_arch = "wasm32"))]
impl Timer for WebTimer {
    fn sleep(&self, limit: Duration) -> Sleep {
        Box::pin(web::Timeout::start(limit))
    }
}

#[cfg(all(feature = "web", target_arch = "wasm32"))]
mod web {
    use std::cell::RefCell;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use js_sys::{Function, Object, Reflect};
    use wasm_bindgen::closure::Closure;
    use wasm_bindgen::{JsCast, JsValue};

    /// A sleep of a [`WebTimer`](super::WebTimer).
    pub(super) struct Timeout {
        fired: Rc<RefCell<Fired>>,
        /// The id of the `setTimeout`: `None` when the call failed.
        id: Option<JsValue>,
        _callback: Closure<dyn Fn()>,
    }

    #[derive(Default)]
    struct Fired {
        done: bool,
        waker: Option<Waker>,
    }

    impl Timeout {
        /// A sleep of `limit`, from this call.
        pub(super) fn start(limit: Duration) -> Self {
            let fired = Rc::new(RefCell::new(Fired::default()));
            let callback = {
                let fired = Rc::clone(&fired);
                Closure::<dyn Fn()>::new(move || {
                    let waker = {
                        let mut fired = fired.borrow_mut();
                        fired.done = true;
                        fired.waker.take()
                    };
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                })
            };
            // `setTimeout` reads its delay as a signed 32-bit integer: a longer
            // delay wraps.
            let delay = (limit.as_secs_f64() * 1000.0)
                .ceil()
                .min(f64::from(i32::MAX));
            let id = global("setTimeout").and_then(|(global, set_timeout)| {
                let delay = JsValue::from_f64(delay);
                set_timeout.call2(&global, callback.as_ref(), &delay).ok()
            });
            if id.is_none() {
                fired.borrow_mut().done = true;
            }
            Self {
                fired,
                id,
                _callback: callback,
            }
        }
    }

    impl Future for Timeout {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let mut fired = self.fired.borrow_mut();
            if fired.done {
                Poll::Ready(())
            } else {
                fired.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    impl Drop for Timeout {
        fn drop(&mut self) {
            if self.fired.borrow().done {
                return;
            }
            if let (Some(id), Some((global, clear_timeout))) = (&self.id, global("clearTimeout")) {
                let _ = clear_timeout.call1(&global, id);
            }
        }
    }

    /// The function `name` of the global object, with that object.
    fn global(name: &str) -> Option<(Object, Function)> {
        // A worker has no `window`: the global object is the window in a page,
        // and the scope in a worker.
        let global = js_sys::global();
        let function = Reflect::get(&global, &JsValue::from_str(name)).ok()?;
        Some((global, function.dyn_into::<Function>().ok()?))
    }
}
