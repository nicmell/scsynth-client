use std::collections::VecDeque;
use std::future::{self, Future};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll, Waker};

use crate::timer::Sleep;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The place of the one value of a [`oneshot`].
struct Slot<T> {
    value: Option<T>,
    /// Whether the sender is gone.
    sent: bool,
    waker: Option<Waker>,
}

/// A channel for one value: its sender and its receiver.
pub(crate) fn oneshot<T>() -> (Sender<T>, Receiver<T>) {
    let slot = Arc::new(Mutex::new(Slot {
        value: None,
        sent: false,
        waker: None,
    }));
    (Sender(Arc::clone(&slot)), Receiver(slot))
}

/// The end of a [`oneshot`] that gives the value.
pub(crate) struct Sender<T>(Arc<Mutex<Slot<T>>>);

/// The end of a [`oneshot`] that waits for the value: a future that gives
/// `None` when the sender is dropped without a value.
pub(crate) struct Receiver<T>(Arc<Mutex<Slot<T>>>);

impl<T> Sender<T> {
    /// Gives the value to the receiver, and wakes it.
    pub(crate) fn send(self, value: T) {
        lock(&self.0).value = Some(value);
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let waker = {
            let mut slot = lock(&self.0);
            slot.sent = true;
            slot.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut slot = lock(&self.0);
        if let Some(value) = slot.value.take() {
            Poll::Ready(Some(value))
        } else if slot.sent {
            Poll::Ready(None)
        } else {
            slot.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// The values of a [`queue`] that its reader did not take yet.
struct Queue<T> {
    items: VecDeque<T>,
    closed: bool,
    waker: Option<Waker>,
}

/// A queue of values for one reader: its writer and its reader.
pub(crate) fn queue<T>() -> (Writer<T>, Reader<T>) {
    let queue = Arc::new(Mutex::new(Queue {
        items: VecDeque::new(),
        closed: false,
        waker: None,
    }));
    (Writer(Arc::downgrade(&queue)), Reader(queue))
}

/// The end of a [`queue`] that pushes. Once the reader is dropped, a push
/// does nothing.
pub(crate) struct Writer<T>(Weak<Mutex<Queue<T>>>);

/// The end of a [`queue`] that takes the values in order.
pub(crate) struct Reader<T>(Arc<Mutex<Queue<T>>>);

impl<T> Clone for Writer<T> {
    fn clone(&self) -> Self {
        Self(Weak::clone(&self.0))
    }
}

impl<T> Writer<T> {
    /// Whether the reader is still there.
    pub(crate) fn is_read(&self) -> bool {
        self.0.strong_count() > 0
    }

    /// Adds a value at the end, unless the queue is closed.
    pub(crate) fn push(&self, item: T) {
        self.update(|queue| {
            if !queue.closed {
                queue.items.push_back(item);
            }
        });
    }

    /// Closes the queue: the reader gets the values that wait, then `None`.
    pub(crate) fn close(&self) {
        self.update(|queue| queue.closed = true);
    }

    fn update(&self, change: impl FnOnce(&mut Queue<T>)) {
        let Some(queue) = self.0.upgrade() else {
            return;
        };
        let waker = {
            let mut queue = lock(&queue);
            change(&mut queue);
            queue.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Reader<T> {
    /// The next value, or `None` once the queue is closed and empty.
    pub(crate) async fn next(&mut self) -> Option<T> {
        future::poll_fn(|cx| {
            let mut queue = lock(&self.0);
            if let Some(item) = queue.items.pop_front() {
                Poll::Ready(Some(item))
            } else if queue.closed {
                Poll::Ready(None)
            } else {
                queue.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
}

/// What a [`race`] gives first.
pub(crate) enum Raced<T> {
    /// The receiver ended: its value, or `None` when its sender was dropped.
    Received(Option<T>),
    /// The sleep ended first.
    Elapsed,
}

/// Waits for `receiver` within a [`Sleep`] that `start` makes at the first
/// poll. Each poll polls the receiver first.
pub(crate) fn race<T, F>(receiver: &mut Receiver<T>, start: F) -> Race<'_, T, F>
where
    F: FnOnce() -> Sleep + Unpin,
{
    Race {
        receiver,
        start: Some(start),
        sleep: None,
    }
}

/// The future of [`race`].
pub(crate) struct Race<'a, T, F> {
    receiver: &'a mut Receiver<T>,
    /// `None` once the sleep is made.
    start: Option<F>,
    sleep: Option<Sleep>,
}

impl<T, F> Future for Race<'_, T, F>
where
    F: FnOnce() -> Sleep + Unpin,
{
    type Output = Raced<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Raced<T>> {
        let this = self.get_mut();
        if let Poll::Ready(value) = Pin::new(&mut *this.receiver).poll(cx) {
            return Poll::Ready(Raced::Received(value));
        }
        if let Some(start) = this.start.take() {
            this.sleep = Some(start());
        }
        match &mut this.sleep {
            Some(sleep) => sleep.as_mut().poll(cx).map(|()| Raced::Elapsed),
            None => Poll::Pending,
        }
    }
}
