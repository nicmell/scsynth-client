use std::cell::{Cell, RefCell};
use std::rc::Rc;

use js_sys::{ArrayBuffer, Uint8Array};
use scsynth_osc::{Command, Packet};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{BinaryType, CloseEvent, MessageEvent, WebSocket};

use super::{Delivery, error_text, packet_events};
use crate::client::{ClientError, Event, Listener, OscClient, SendError};
use crate::wait::{self, Sender};

/// An [`OscClient`] over one `WebSocket` of the browser: one binary frame
/// for each packet, each way.
///
/// A frame from the other end comes as its messages, in order. Bytes that
/// are no OSC packet, and a text frame, come as an [`Event::Error`].
/// [`OscClient::send`] hands the frame to the browser, which queues it, and
/// gives [`SendError::Closed`] once the socket is not open. The listener gets
/// [`Event::Closed`] once, at the `close` event of the socket: after
/// [`OscClient::close`], or when the other end or the network closes it. An
/// `error` event of the open socket comes as an [`Event::Error`] before it.
///
/// A client that drops removes its handlers and closes its socket: its
/// listener gets no more event.
pub struct WebSocketClient {
    socket: WebSocket,
    delivery: Rc<RefCell<Delivery>>,
    _on_open: Closure<dyn Fn()>,
    _on_message: Closure<dyn Fn(MessageEvent)>,
    _on_error: Closure<dyn Fn()>,
    _on_close: Closure<dyn Fn(CloseEvent)>,
}

/// The wait of [`WebSocketClient::open`]: `None` once the socket opened,
/// failed or closed.
type Opening = Rc<Cell<Option<Sender<Result<(), ClientError>>>>>;

impl WebSocketClient {
    /// A client of a new `WebSocket` to `url`, with the binary type
    /// `arraybuffer`. It ends at the `open` event of the socket.
    ///
    /// # Errors
    ///
    /// [`ClientError::Transport`] with the text of the error when the
    /// browser refuses `url`, and when the socket gets an `error` or a
    /// `close` event before its `open` event.
    pub async fn open(url: &str) -> Result<Self, ClientError> {
        let socket =
            WebSocket::new(url).map_err(|error| ClientError::Transport(error_text(&error)))?;
        socket.set_binary_type(BinaryType::Arraybuffer);
        let delivery = Rc::new(RefCell::new(Delivery::default()));
        let (sender, opened) = wait::oneshot();
        let opening: Opening = Rc::new(Cell::new(Some(sender)));

        let on_open = {
            let opening = Rc::clone(&opening);
            Closure::<dyn Fn()>::new(move || {
                if let Some(sender) = opening.take() {
                    sender.send(Ok(()));
                }
            })
        };
        let on_message = {
            let delivery = Rc::clone(&delivery);
            Closure::<dyn Fn(MessageEvent)>::new(move |message: MessageEvent| {
                Delivery::emit(&delivery, frame_events(&message.data()));
            })
        };
        let on_error = {
            let (delivery, opening) = (Rc::clone(&delivery), Rc::clone(&opening));
            Closure::<dyn Fn()>::new(move || {
                if let Some(sender) = opening.take() {
                    let error = "the websocket failed to open".to_owned();
                    sender.send(Err(ClientError::Transport(error)));
                } else {
                    let error = ClientError::Transport("the websocket failed".to_owned());
                    Delivery::emit(&delivery, [Event::Error(error)]);
                }
            })
        };
        let on_close = {
            let (delivery, opening) = (Rc::clone(&delivery), Rc::clone(&opening));
            Closure::<dyn Fn(CloseEvent)>::new(move |close: CloseEvent| match opening.take() {
                Some(sender) => sender.send(Err(ClientError::Transport(format!(
                    "the websocket closed before it opened, with the code {}",
                    close.code()
                )))),
                None => Delivery::emit(&delivery, [Event::Closed]),
            })
        };
        socket.set_onopen(Some(on_open.as_ref().unchecked_ref()));
        socket.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        socket.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        socket.set_onclose(Some(on_close.as_ref().unchecked_ref()));

        let client = Self {
            socket,
            delivery,
            _on_open: on_open,
            _on_message: on_message,
            _on_error: on_error,
            _on_close: on_close,
        };
        // The handlers hold the sender until they send: `None` does not come.
        opened.await.unwrap_or(Err(ClientError::Transport(
            "the websocket closed before it opened".to_owned(),
        )))?;
        Ok(client)
    }
}

impl OscClient for WebSocketClient {
    fn send(&self, packet: &Packet<Command>) -> Result<(), SendError> {
        if self.socket.ready_state() != WebSocket::OPEN {
            return Err(SendError::Closed);
        }
        if let Err(error) = self.socket.send_with_u8_array(&packet.encode()) {
            Delivery::failed(&self.delivery, &error);
        }
        Ok(())
    }

    fn listen(&self, listener: Listener) {
        Delivery::listen(&self.delivery, listener);
    }

    fn close(&self) {
        let _ = self.socket.close();
    }
}

impl Drop for WebSocketClient {
    fn drop(&mut self) {
        self.socket.set_onopen(None);
        self.socket.set_onmessage(None);
        self.socket.set_onerror(None);
        self.socket.set_onclose(None);
        let _ = self.socket.close();
        Delivery::end(&self.delivery);
    }
}

/// The events of the data of one frame.
fn frame_events(data: &JsValue) -> Vec<Event> {
    if let Some(buffer) = data.dyn_ref::<ArrayBuffer>() {
        packet_events(&Uint8Array::new(buffer).to_vec())
    } else {
        let error = "a text frame, where each packet is a binary frame";
        vec![Event::Error(ClientError::Transport(error.to_owned()))]
    }
}
