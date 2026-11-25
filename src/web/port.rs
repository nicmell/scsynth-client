use std::cell::{Cell, RefCell};
use std::rc::Rc;

use js_sys::{Array, ArrayBuffer, Uint8Array};
use scsynth_osc::{Command, Packet};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{DedicatedWorkerGlobalScope, EventTarget, MessageEvent, MessagePort, Worker};

use super::{Delivery, packet_events};
use crate::client::{ClientError, Event, Listener, OscClient, SendError};

/// An [`OscClient`] over a target that posts messages: a `MessagePort`, a
/// `Worker` on the side of the page, or the `DedicatedWorkerGlobalScope` on
/// the side of the worker. One message is one packet, each way.
///
/// [`OscClient::send`] posts a `Uint8Array` of the bytes of the packet, and
/// transfers its buffer. A message whose data is a `Uint8Array` or an
/// `ArrayBuffer` comes as its messages, in order. Any other data, and bytes
/// that are no OSC packet, come as an [`Event::Error`]: the target carries
/// the packets alone.
///
/// A `Worker` and a worker scope dispatch each message once, so a message
/// that comes before the client exists is lost. A `MessagePort` keeps its
/// messages until the client starts it.
///
/// A port has no close event: when the other end closes it, the client gets
/// no event, and `send` goes on. [`OscClient::close`] removes the handler of
/// the client, closes a `MessagePort`, and gives [`Event::Closed`] once.
/// Then `send` gives [`SendError::Closed`]. It does not terminate a worker,
/// and does not close a worker scope. A client that drops removes its
/// handler and closes a `MessagePort`: its listener gets no more event.
pub struct PortClient {
    target: Target,
    delivery: Rc<RefCell<Delivery>>,
    on_message: Closure<dyn Fn(MessageEvent)>,
    open: Cell<bool>,
}

/// What a [`PortClient`] posts to.
enum Target {
    Port(MessagePort),
    Worker(Worker),
    Scope(DedicatedWorkerGlobalScope),
}

impl PortClient {
    /// A client over `port`, which it starts.
    #[must_use]
    pub fn from_port(port: MessagePort) -> Self {
        Self::new(Target::Port(port))
    }

    /// A client over `worker`, on the side of the page that made it.
    #[must_use]
    pub fn from_worker(worker: Worker) -> Self {
        Self::new(Target::Worker(worker))
    }

    /// A client over `scope`, the global scope of a dedicated worker, on the
    /// side of the worker.
    #[must_use]
    pub fn from_worker_scope(scope: DedicatedWorkerGlobalScope) -> Self {
        Self::new(Target::Scope(scope))
    }

    fn new(target: Target) -> Self {
        let delivery = Rc::new(RefCell::new(Delivery::default()));
        let on_message = {
            let delivery = Rc::clone(&delivery);
            Closure::<dyn Fn(MessageEvent)>::new(move |message: MessageEvent| {
                Delivery::emit(&delivery, message_events(&message.data()));
            })
        };
        let _ = target
            .events()
            .add_event_listener_with_callback("message", on_message.as_ref().unchecked_ref());
        if let Target::Port(port) = &target {
            // `addEventListener` does not start a port, as `onmessage` does.
            port.start();
        }
        Self {
            target,
            delivery,
            on_message,
            open: Cell::new(true),
        }
    }

    /// Removes the handler, and closes a `MessagePort`.
    fn detach(&self) {
        let _ = self.target.events().remove_event_listener_with_callback(
            "message",
            self.on_message.as_ref().unchecked_ref(),
        );
        if let Target::Port(port) = &self.target {
            port.close();
        }
    }
}

impl OscClient for PortClient {
    fn send(&self, packet: &Packet<Command>) -> Result<(), SendError> {
        if !self.open.get() {
            return Err(SendError::Closed);
        }
        // A copy in a buffer of its own: the transfer of a view on the memory
        // of wasm would detach that memory.
        let bytes = Uint8Array::new_from_slice(&packet.encode());
        let transfer = Array::of1(&bytes.buffer());
        if let Err(error) = self.target.post(&bytes, &transfer) {
            Delivery::failed(&self.delivery, &error);
        }
        Ok(())
    }

    fn listen(&self, listener: Listener) {
        Delivery::listen(&self.delivery, listener);
    }

    fn close(&self) {
        if self.open.replace(false) {
            self.detach();
            Delivery::emit(&self.delivery, [Event::Closed]);
        }
    }
}

impl Drop for PortClient {
    fn drop(&mut self) {
        if self.open.replace(false) {
            self.detach();
        }
        Delivery::end(&self.delivery);
    }
}

impl Target {
    fn events(&self) -> &EventTarget {
        match self {
            Self::Port(port) => port,
            Self::Worker(worker) => worker,
            Self::Scope(scope) => scope,
        }
    }

    fn post(&self, message: &JsValue, transfer: &JsValue) -> Result<(), JsValue> {
        match self {
            Self::Port(port) => port.post_message_with_transferable(message, transfer),
            Self::Worker(worker) => worker.post_message_with_transfer(message, transfer),
            Self::Scope(scope) => scope.post_message_with_transfer(message, transfer),
        }
    }
}

/// The events of the data of one message.
fn message_events(data: &JsValue) -> Vec<Event> {
    if let Some(bytes) = data.dyn_ref::<Uint8Array>() {
        packet_events(&bytes.to_vec())
    } else if let Some(buffer) = data.dyn_ref::<ArrayBuffer>() {
        packet_events(&Uint8Array::new(buffer).to_vec())
    } else {
        let error = "a message whose data is no Uint8Array or ArrayBuffer";
        vec![Event::Error(ClientError::Transport(error.to_owned()))]
    }
}
