use std::cell::RefCell;
use std::collections::VecDeque;

use wasm_bindgen::{JsCast, JsValue};

use crate::client::{ClientError, Event, Listener, decode};

mod port;
mod websocket;

pub use port::PortClient;
pub use websocket::WebSocketClient;

/// The listener of a client of the browser, and the events that wait for
/// it.
#[derive(Default)]
struct Delivery {
    /// `None` while it runs.
    listener: Option<Listener>,
    kept: VecDeque<Event>,
    /// Whether a call delivers: one call at a time.
    delivering: bool,
    /// Whether [`Event::Closed`] came or the client dropped: no event
    /// follows.
    ended: bool,
}

impl Delivery {
    /// Sets the one listener, and gives it the events that wait.
    fn listen(cell: &RefCell<Self>, listener: Listener) {
        // Out of the borrow: the listener that it replaces can own the client.
        let replaced = cell.borrow_mut().listener.replace(listener);
        drop(replaced);
        Self::deliver(cell);
    }

    /// Adds `events` for the listener, and delivers them. After the end, it
    /// drops them.
    fn emit(cell: &RefCell<Self>, events: impl IntoIterator<Item = Event>) {
        {
            let mut delivery = cell.borrow_mut();
            for event in events {
                if delivery.ended {
                    break;
                }
                delivery.ended = matches!(event, Event::Closed);
                delivery.kept.push_back(event);
            }
        }
        Self::deliver(cell);
    }

    /// Adds one [`Event::Error`] with the text of a JavaScript error.
    fn failed(cell: &RefCell<Self>, error: &JsValue) {
        let error = ClientError::Transport(error_text(error));
        Self::emit(cell, [Event::Error(error)]);
    }

    /// Ends the delivery at the drop of the client: the events that wait
    /// and those that come are dropped.
    fn end(cell: &RefCell<Self>) {
        let mut delivery = cell.borrow_mut();
        delivery.ended = true;
        delivery.kept.clear();
    }

    /// Gives the events that wait to the listener, one call at a time. A
    /// call from inside the listener returns at once: the delivery that runs
    /// delivers what it added.
    fn deliver(cell: &RefCell<Self>) {
        {
            let mut delivery = cell.borrow_mut();
            if delivery.delivering {
                return;
            }
            delivery.delivering = true;
        }
        loop {
            let next = cell.borrow_mut().next();
            let Some((event, mut listener)) = next else {
                return;
            };
            listener(event);
            let replaced = {
                let mut delivery = cell.borrow_mut();
                if delivery.listener.is_none() {
                    delivery.listener = Some(listener);
                    None
                } else {
                    Some(listener)
                }
            };
            drop(replaced);
        }
    }

    /// The next event and the listener, taken out for the call. `None` ends
    /// the delivery.
    fn next(&mut self) -> Option<(Event, Listener)> {
        if self.listener.is_none() || self.kept.is_empty() {
            self.delivering = false;
            return None;
        }
        Some((self.kept.pop_front()?, self.listener.take()?))
    }
}

/// The events of the bytes of one OSC packet: its messages, or one
/// [`Event::Error`] when they do not decode.
fn packet_events(bytes: &[u8]) -> Vec<Event> {
    match decode(bytes) {
        Ok(messages) => messages.into_iter().map(Event::Message).collect(),
        Err(error) => vec![Event::Error(error)],
    }
}

/// The text of a JavaScript error.
fn error_text(error: &JsValue) -> String {
    if let Some(error) = error.dyn_ref::<js_sys::Error>() {
        String::from(error.to_string())
    } else if let Some(text) = error.as_string() {
        text
    } else {
        format!("{error:?}")
    }
}
