use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use js_sys::{Reflect, Uint8Array};
use scsynth_client::scsynth_osc::{Command, Packet};
use scsynth_client::{ClientError, Event, Listener, OscClient, SendError, decode};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    /// An object of the caller with the three methods of an `OscClient`.
    #[wasm_bindgen(typescript_type = "OscClient")]
    pub type JsOscClient;

    #[wasm_bindgen(method, catch, js_name = send)]
    fn send_packet(this: &JsOscClient, packet: &Uint8Array) -> Result<(), JsValue>;

    #[wasm_bindgen(method, catch, js_name = listen)]
    fn set_listener(this: &JsOscClient, listener: Closure<dyn Fn(JsValue)>) -> Result<(), JsValue>;

    #[wasm_bindgen(method, catch, js_name = close)]
    fn close_object(this: &JsOscClient) -> Result<(), JsValue>;
}

/// An [`OscClient`] over an object of the caller, whose methods carry the
/// bytes of the packets.
///
/// It gives the listener of the object a function at its start, and keeps
/// what that function gets until a listener of its own is set. `close` calls
/// the `close` of the object, and gives [`Event::Closed`] at once: what the
/// object gives after it is dropped. A client that drops closes the object.
pub(crate) struct ObjectClient {
    object: JsOscClient,
    delivery: Rc<RefCell<Delivery>>,
}

/// The listener of an [`ObjectClient`], and the events that wait for it.
#[derive(Default)]
struct Delivery {
    /// `None` while it runs.
    listener: Option<Listener>,
    kept: VecDeque<Event>,
    /// Whether a call delivers: one call at a time.
    delivering: bool,
    /// Whether [`Event::Closed`] came or the client dropped.
    ended: bool,
}

impl ObjectClient {
    /// A client over `object`, whose listener it sets.
    pub(crate) fn new(object: JsOscClient) -> Self {
        let delivery = Rc::new(RefCell::new(Delivery::default()));
        let weak = Rc::downgrade(&delivery);
        // Passed by value, the function belongs to the object: a call after
        // the drop of the client finds no delivery, and no dropped closure.
        let listener = Closure::<dyn Fn(JsValue)>::new(move |event: JsValue| {
            if let Some(delivery) = weak.upgrade() {
                Delivery::emit(&delivery, events(&event));
            }
        });
        if let Err(error) = object.set_listener(listener) {
            Delivery::emit(&delivery, [failed(&error)]);
        }
        Self { object, delivery }
    }

    fn ended(&self) -> bool {
        self.delivery.borrow().ended
    }
}

impl OscClient for ObjectClient {
    fn send(&self, packet: &Packet<Command>) -> Result<(), SendError> {
        if self.ended() {
            return Err(SendError::Closed);
        }
        // A buffer of its own, and no view on the memory of wasm, which a
        // transfer would detach.
        let bytes = Uint8Array::new_from_slice(&packet.encode());
        if let Err(error) = self.object.send_packet(&bytes) {
            Delivery::emit(&self.delivery, [failed(&error)]);
        }
        Ok(())
    }

    fn listen(&self, listener: Listener) {
        // Out of the borrow: the listener that it replaces can own the client.
        let replaced = self.delivery.borrow_mut().listener.replace(listener);
        drop(replaced);
        Delivery::deliver(&self.delivery);
    }

    fn close(&self) {
        if self.ended() {
            return;
        }
        let mut events = Vec::new();
        if let Err(error) = self.object.close_object() {
            events.push(failed(&error));
        }
        events.push(Event::Closed);
        Delivery::emit(&self.delivery, events);
    }
}

impl Drop for ObjectClient {
    fn drop(&mut self) {
        if !self.ended() {
            let _ = self.object.close_object();
        }
        let listener = {
            let mut delivery = self.delivery.borrow_mut();
            delivery.ended = true;
            delivery.kept.clear();
            delivery.listener.take()
        };
        drop(listener);
    }
}

impl Delivery {
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
            let next = {
                let mut delivery = cell.borrow_mut();
                if delivery.listener.is_none() || delivery.kept.is_empty() {
                    delivery.delivering = false;
                    return;
                }
                delivery.kept.pop_front().zip(delivery.listener.take())
            };
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
}

/// The events of one `OscEvent` of the object.
fn events(event: &JsValue) -> Vec<Event> {
    let field = |name: &str| Reflect::get(event, &JsValue::from_str(name)).unwrap_or_default();
    match field("kind").as_string().as_deref() {
        Some("message") => match field("packet").dyn_into::<Uint8Array>() {
            Ok(packet) => match decode(&packet.to_vec()) {
                Ok(messages) => messages.into_iter().map(Event::Message).collect(),
                Err(error) => vec![Event::Error(error)],
            },
            Err(_) => vec![transport("a message event whose packet is no Uint8Array")],
        },
        Some("error") => vec![transport(&field("message").as_string().unwrap_or_default())],
        Some("closed") => vec![Event::Closed],
        _ => vec![transport(
            "an event whose kind is no message, error or closed",
        )],
    }
}

/// An [`Event::Error`] with the text of a JS error.
fn failed(error: &JsValue) -> Event {
    let text = match error.dyn_ref::<js_sys::Error>() {
        Some(error) => String::from(error.message()),
        None => error.as_string().unwrap_or_else(|| format!("{error:?}")),
    };
    transport(&text)
}

fn transport(text: &str) -> Event {
    Event::Error(ClientError::Transport(text.to_owned()))
}
