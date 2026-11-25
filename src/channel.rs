use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use scsynth_osc::{Command, Packet};

use crate::client::{ClientError, Event, Incoming, Listener, MaybeSend, OscClient, SendError};

#[cfg(not(target_arch = "wasm32"))]
type Handler = Box<dyn FnMut(&ChannelPeer, Packet<Command>) + Send>;

#[cfg(target_arch = "wasm32")]
type Handler = Box<dyn FnMut(&ChannelPeer, Packet<Command>)>;

/// The [`OscClient`] end of a pair in memory, which carries typed values and
/// no bytes. Its other end is a [`ChannelPeer`].
///
/// A call delivers what it adds before it returns, in the same call stack.
/// A call from inside a listener or a handler only queues: the delivery that
/// runs delivers it after that listener or handler returns. So a listener
/// or a handler never runs inside another, and no lock is held while one
/// runs. Each direction delivers in the order of arrival. A call from
/// another thread while a delivery runs also only queues, and the thread of
/// that delivery delivers it.
pub struct ChannelClient {
    link: Link,
}

/// The other end of a [`ChannelClient`]: a test that plays scsynth, or a
/// host of the caller. It delivers as [`ChannelClient`] does.
pub struct ChannelPeer {
    link: Link,
}

impl ChannelClient {
    /// A client and its peer, open.
    #[must_use]
    #[cfg_attr(
        target_arch = "wasm32",
        allow(
            clippy::arc_with_non_send_sync,
            reason = "the listener and the handler are Send on a native target alone"
        )
    )]
    pub fn pair() -> (ChannelClient, ChannelPeer) {
        let link = Link(Arc::new(Mutex::new(State::default())));
        (ChannelClient { link: link.clone() }, ChannelPeer { link })
    }
}

impl OscClient for ChannelClient {
    fn send(&self, packet: &Packet<Command>) -> Result<(), SendError> {
        self.link.update(|state| {
            if state.closed {
                return Err(SendError::Closed);
            }
            state.push_packet(packet.clone());
            Ok(())
        })
    }

    fn listen(&self, listener: Listener) {
        self.link.update(|state| state.listener = Some(listener));
    }

    fn close(&self) {
        self.link.close();
    }
}

impl ChannelPeer {
    /// The oldest packet that the client sent and no handler got.
    #[must_use]
    pub fn take(&self) -> Option<Packet<Command>> {
        self.link
            .lock()
            .packets
            .pop_front()
            .map(|(_, packet)| packet)
    }

    /// Sets the one handler, which gets the peer and each packet of the
    /// client in place of [`Self::take`]: first the packets that wait, in
    /// order. A second call replaces the first handler.
    pub fn on_packet<F>(&self, handler: F)
    where
        F: FnMut(&ChannelPeer, Packet<Command>) + MaybeSend + 'static,
    {
        self.link
            .update(|state| state.handler = Some(Box::new(handler)));
    }

    /// Sends one message to the listener of the client. After the close, the
    /// message is dropped.
    pub fn send(&self, incoming: Incoming) {
        self.event(Event::Message(incoming));
    }

    /// Sends one [`Event::Error`] to the listener of the client. After the
    /// close, the error is dropped.
    pub fn error(&self, error: ClientError) {
        self.event(Event::Error(error));
    }

    /// Closes the pair. The listener of the client gets [`Event::Closed`]
    /// once, after the events that wait.
    pub fn close(&self) {
        self.link.close();
    }

    /// Whether either end closed the pair.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.link.lock().closed
    }

    fn event(&self, event: Event) {
        self.link.update(|state| {
            if !state.closed {
                state.push_event(event);
            }
        });
    }
}

#[derive(Clone)]
struct Link(Arc<Mutex<State>>);

#[derive(Default)]
struct State {
    /// The packets of the client, with their place in the order of arrival.
    packets: VecDeque<(u64, Packet<Command>)>,
    /// The events for the client, with their place in the order of arrival.
    events: VecDeque<(u64, Event)>,
    arrival: u64,
    /// `None` while it runs.
    handler: Option<Handler>,
    /// `None` while it runs.
    listener: Option<Listener>,
    closed: bool,
    /// Whether a call delivers: one call at a time.
    delivering: bool,
}

/// What the delivery calls next, out of the lock.
enum Delivery {
    Packet(Packet<Command>, Handler),
    Event(Event, Listener),
}

impl Link {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn update<R>(&self, change: impl FnOnce(&mut State) -> R) -> R {
        let mut state = self.lock();
        let result = change(&mut state);
        if !state.delivering {
            state.delivering = true;
            drop(state);
            self.deliver();
        }
        result
    }

    fn close(&self) {
        self.update(|state| {
            if !state.closed {
                state.closed = true;
                state.push_event(Event::Closed);
            }
        });
    }

    fn deliver(&self) {
        let mut state = self.lock();
        while let Some(delivery) = state.next() {
            drop(state);
            match delivery {
                Delivery::Packet(packet, mut handler) => {
                    handler(&ChannelPeer { link: self.clone() }, packet);
                    state = self.lock();
                    if state.handler.is_none() {
                        state.handler = Some(handler);
                    }
                }
                Delivery::Event(event, mut listener) => {
                    listener(event);
                    state = self.lock();
                    if state.listener.is_none() {
                        state.listener = Some(listener);
                    }
                }
            }
        }
        state.delivering = false;
    }
}

impl State {
    fn push_packet(&mut self, packet: Packet<Command>) {
        self.packets.push_back((self.arrival, packet));
        self.arrival += 1;
    }

    fn push_event(&mut self, event: Event) {
        self.events.push_back((self.arrival, event));
        self.arrival += 1;
    }

    fn next(&mut self) -> Option<Delivery> {
        let packet = self
            .handler
            .as_ref()
            .and(self.packets.front())
            .map(|(arrival, _)| *arrival);
        let event = self
            .listener
            .as_ref()
            .and(self.events.front())
            .map(|(arrival, _)| *arrival);
        if packet.is_some_and(|packet| event.is_none_or(|event| packet < event)) {
            let (_, packet) = self.packets.pop_front()?;
            Some(Delivery::Packet(packet, self.handler.take()?))
        } else if event.is_some() {
            let (_, event) = self.events.pop_front()?;
            Some(Delivery::Event(event, self.listener.take()?))
        } else {
            None
        }
    }
}
