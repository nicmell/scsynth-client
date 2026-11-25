//! scsynth-client for the browser: what the Vite example calls. `Scsynth`
//! wraps the client of the crate, over a `WebSocketClient` or a `PortClient`
//! of this module, or over any object with the three methods of an
//! `OscClient`. Bytes cross as a `Uint8Array`, a message of scsynth as the
//! serde form of scsynth-osc, and an error as a JS `Error` with the text of
//! the error of the crate.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fmt::Display;
use std::future::{self, Future};
use std::pin::pin;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

use js_sys::{Array, Function, Object, Promise, Reflect};
use scsynth_client::scsynth_osc::{ControlSetting, NodeId};
use scsynth_client::scsynth_synthdef::{Arg, SynthDefBuilder, UGenInput};
use scsynth_client::{Event, Incoming, Options, Subscription, Target};
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{future_to_promise, spawn_local};
use web_sys::{MessagePort, Worker};

mod object;

use object::{JsOscClient, ObjectClient};

#[wasm_bindgen(typescript_custom_section)]
const TYPES: &str = r#"
/** What a transport of the caller gives to its listener. */
export type OscEvent =
  | { kind: "message"; packet: Uint8Array }
  | { kind: "error"; message: string }
  | { kind: "closed" };

/** A transport of the caller: a connection to scsynth that carries the bytes of OSC packets. */
export interface OscClient {
  /** Queues one packet, and returns at once. The packet is a buffer of its own, which it may transfer. */
  send(packet: Uint8Array): void;
  /** Sets the one listener, which gets the events in order, one call at a time. */
  listen(listener: (event: OscEvent) => void): void;
  /** Closes the connection. */
  close(): void;
}

/** The options of a Scsynth: those of scsynth-client, with the limit in milliseconds. */
export interface Options {
  timeoutMs?: number;
  clientId?: number;
  maxLogins?: number;
  buffers?: number;
  controlBuses?: number;
  audioBuses?: number;
  inputs?: number;
  outputs?: number;
}

/** Where a new node goes: the head or the tail of a group, or just before or just after a node. */
export type Target = { head: number } | { tail: number } | { before: number } | { after: number };

/** The values of the controls of a synth, by name. */
export type Controls = Record<string, number>;

/** The login of a client of scsynth. */
export interface Registration {
  clientId: number;
  maxLogins: number;
}

/** A message of scsynth: the serde form of a reply of scsynth-osc, or of its RawMessage. */
export interface OscMessage {
  address: string;
  args?: unknown;
}
"#;

/// A transport over a `WebSocket` of the browser: the `WebSocketClient` of
/// scsynth-client.
#[wasm_bindgen(js_name = WebSocketClient)]
pub struct WebSocketClientJs {
    inner: scsynth_client::WebSocketClient,
}

#[wasm_bindgen(js_class = WebSocketClient)]
impl WebSocketClientJs {
    /// A client of a new `WebSocket` to `url`. It ends at the `open` event of
    /// the socket.
    ///
    /// # Errors
    ///
    /// When the browser refuses `url`, or the socket fails or closes before
    /// it opens.
    pub async fn open(url: String) -> Result<WebSocketClientJs, JsError> {
        let inner = scsynth_client::WebSocketClient::open(&url)
            .await
            .map_err(|error| JsError::new(&error.to_string()))?;
        Ok(Self { inner })
    }
}

/// A transport over a target that posts messages: the `PortClient` of
/// scsynth-client.
#[wasm_bindgen(js_name = PortClient)]
pub struct PortClientJs {
    inner: scsynth_client::PortClient,
}

#[wasm_bindgen(js_class = PortClient)]
impl PortClientJs {
    /// A client over `port`, which it starts.
    #[wasm_bindgen(js_name = fromPort)]
    #[must_use]
    pub fn from_port(port: MessagePort) -> PortClientJs {
        Self {
            inner: scsynth_client::PortClient::from_port(port),
        }
    }

    /// A client over `worker`, on the side of the page that made it.
    #[wasm_bindgen(js_name = fromWorker)]
    #[must_use]
    pub fn from_worker(worker: Worker) -> PortClientJs {
        Self {
            inner: scsynth_client::PortClient::from_worker(worker),
        }
    }
}

/// What `new Scsynth` takes: a client of this module, or an object of the
/// caller.
#[wasm_bindgen(fallback)]
pub enum Client {
    /// A `WebSocketClient`, which `Scsynth` takes.
    WebSocket(WebSocketClientJs),
    /// A `PortClient`, which `Scsynth` takes.
    Port(PortClientJs),
    /// Any other value: an `OscClient` object of the caller.
    Object(JsOscClient),
}

/// A client of scsynth over one transport: the `Scsynth` of scsynth-client,
/// whose limits are `setTimeout` calls of the browser.
#[wasm_bindgen(js_name = Scsynth)]
pub struct ScsynthJs {
    scsynth: scsynth_client::Scsynth,
    handlers: Rc<RefCell<Handlers>>,
}

/// The loops of [`ScsynthJs::on`], by their id.
#[derive(Default)]
struct Handlers {
    last: u32,
    stops: HashMap<u32, Rc<Stop>>,
}

/// The end of one loop of [`ScsynthJs::on`].
#[derive(Default)]
struct Stop {
    stopped: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

/// The options of a `Scsynth`, as JS gives them.
// serde-wasm-bindgen reads the fields of a struct by name, and sees no other
// key: `deny_unknown_fields` refuses nothing.
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsOptions {
    timeout_ms: Option<f64>,
    client_id: Option<i32>,
    max_logins: Option<i32>,
    buffers: Option<i32>,
    control_buses: Option<i32>,
    audio_buses: Option<i32>,
    inputs: Option<i32>,
    outputs: Option<i32>,
}

/// A target, as JS gives it: `{ head: node }` and the like.
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum JsTarget {
    Head(i32),
    Tail(i32),
    Before(i32),
    After(i32),
}

/// A registration, as JS gets it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsRegistration {
    client_id: i32,
    max_logins: i32,
}

#[wasm_bindgen(js_class = Scsynth)]
impl ScsynthJs {
    /// A client over `client`: a `WebSocketClient` or a `PortClient` of this
    /// module, which it takes, or an `OscClient` object of the caller.
    ///
    /// # Errors
    ///
    /// When `options` are not `Options`. A `WebSocketClient` or a
    /// `PortClient` is taken and closed even then.
    #[wasm_bindgen(constructor)]
    pub fn new(
        client: Client,
        #[wasm_bindgen(unchecked_optional_param_type = "Options")] options: Option<JsValue>,
    ) -> Result<ScsynthJs, JsError> {
        let options = options_of(options)?;
        let scsynth = match client {
            Client::WebSocket(client) => scsynth_client::Scsynth::new(client.inner, options),
            Client::Port(client) => scsynth_client::Scsynth::new(client.inner, options),
            Client::Object(object) => {
                scsynth_client::Scsynth::new(ObjectClient::new(object), options)
            }
        };
        Ok(Self {
            scsynth,
            handlers: Rc::default(),
        })
    }

    /// Registers this client for the notifications of scsynth: `/notify 1`.
    #[wasm_bindgen(unchecked_return_type = "Promise<Registration>")]
    pub fn register(&self) -> Promise {
        let scsynth = self.scsynth.clone();
        future_to_promise(async move {
            let registration = scsynth.register().await.map_err(js_error)?;
            to_js(&JsRegistration {
                client_id: registration.client_id,
                max_logins: registration.max_logins,
            })
        })
    }

    /// Ends the registration of this client: `/notify 0`.
    #[wasm_bindgen(unchecked_return_type = "Promise<void>")]
    pub fn unregister(&self) -> Promise {
        let scsynth = self.scsynth.clone();
        future_to_promise(async move { done(scsynth.unregister().await) })
    }

    /// Closes the transport, after `/notify 0` when the client is
    /// registered.
    #[wasm_bindgen(unchecked_return_type = "Promise<void>")]
    pub fn close(&self) -> Promise {
        let scsynth = self.scsynth.clone();
        future_to_promise(async move { done(scsynth.close().await) })
    }

    /// Ends once scsynth has done what the requests before it asked.
    #[wasm_bindgen(unchecked_return_type = "Promise<void>")]
    pub fn sync(&self) -> Promise {
        let scsynth = self.scsynth.clone();
        future_to_promise(async move { done(scsynth.sync().await) })
    }

    /// The load of the server: the serde form of its `/status.reply`.
    #[wasm_bindgen(unchecked_return_type = "Promise<Record<string, number>>")]
    pub fn status(&self) -> Promise {
        let scsynth = self.scsynth.clone();
        future_to_promise(async move { to_js(&scsynth.status().await.map_err(js_error)?) })
    }

    /// Sends the bytes of a synth definition file: `/d_recv`.
    #[wasm_bindgen(js_name = addSynthdef, unchecked_return_type = "Promise<void>")]
    pub fn add_synthdef(&self, bytes: &[u8]) -> Promise {
        let scsynth = self.scsynth.clone();
        let bytes = bytes.to_vec();
        future_to_promise(async move { done(scsynth.add_synthdef_bytes(&bytes).await) })
    }

    /// Makes a group at `target`, and gives its node id: `/g_new`.
    #[wasm_bindgen(unchecked_return_type = "Promise<number>")]
    pub fn group(
        &self,
        #[wasm_bindgen(unchecked_param_type = "Target")] target: JsValue,
    ) -> Promise {
        let scsynth = self.scsynth.clone();
        future_to_promise(async move {
            let group = scsynth.group(target_of(target)?).await.map_err(js_error)?;
            Ok(group.0.into())
        })
    }

    /// Makes a synth of the definition `name` at `target`, with the values
    /// of `controls`, and gives its node id: `/s_new`.
    #[wasm_bindgen(unchecked_return_type = "Promise<number>")]
    pub fn synth(
        &self,
        name: String,
        #[wasm_bindgen(unchecked_param_type = "Controls")] controls: JsValue,
        #[wasm_bindgen(unchecked_param_type = "Target")] target: JsValue,
    ) -> Promise {
        let scsynth = self.scsynth.clone();
        future_to_promise(async move {
            let controls = controls_of(&controls).map_err(js_error)?;
            let target = target_of(target)?;
            let synth = scsynth
                .synth(name, controls, target)
                .await
                .map_err(js_error)?;
            Ok(synth.0.into())
        })
    }

    /// Sets `controls` of `node`: a plain send of `/n_set`, which waits for
    /// nothing.
    ///
    /// # Errors
    ///
    /// When `controls` are not `Controls`, and when the transport refuses
    /// the packet.
    pub fn set(
        &self,
        node: i32,
        #[wasm_bindgen(unchecked_param_type = "Controls")] controls: &JsValue,
    ) -> Result<(), JsError> {
        let controls = controls_of(controls).map_err(|error| JsError::new(&error))?;
        self.scsynth
            .set(NodeId(node), controls)
            .map_err(|error| JsError::new(&error.to_string()))
    }

    /// Frees `node`: `/n_free`.
    // `free` is the name of the destructor that wasm-bindgen gives each
    // class, and of its `Symbol.dispose`: a method of that name replaces both.
    #[wasm_bindgen(js_name = freeNode, unchecked_return_type = "Promise<void>")]
    pub fn free_node(&self, node: i32) -> Promise {
        let scsynth = self.scsynth.clone();
        future_to_promise(async move { done(scsynth.free(NodeId(node)).await) })
    }

    /// Calls `callback` with each message of scsynth whose address is
    /// `address`, or with every message for `"*"`, from now on, and gives
    /// the id that [`Self::off`] takes.
    #[must_use]
    pub fn on(
        &self,
        address: String,
        #[wasm_bindgen(unchecked_param_type = "(message: OscMessage) => void")] callback: Function,
    ) -> u32 {
        let mut subscription = self.scsynth.subscribe();
        let stop = Rc::new(Stop::default());
        let id = {
            let mut handlers = self.handlers.borrow_mut();
            handlers.last += 1;
            let id = handlers.last;
            handlers.stops.insert(id, Rc::clone(&stop));
            id
        };
        let handlers = Rc::downgrade(&self.handlers);
        spawn_local(async move {
            while let Some(event) = next(&mut subscription, &stop).await {
                let Event::Message(incoming) = event else {
                    continue;
                };
                if address == "*" || address == address_of(&incoming) {
                    let called = message_of(&incoming)
                        .and_then(|message| callback.call1(&JsValue::NULL, &message));
                    if let Err(error) = called {
                        report(&error);
                    }
                }
            }
            if let Some(handlers) = handlers.upgrade() {
                handlers.borrow_mut().stops.remove(&id);
            }
        });
        id
    }

    /// Stops the calls of the callback of [`Self::on`] with the id `id`.
    pub fn off(&self, id: u32) {
        let stop = self.handlers.borrow_mut().stops.remove(&id);
        if let Some(stop) = stop {
            stop.stopped.set(true);
            if let Some(waker) = stop.waker.take() {
                waker.wake();
            }
        }
    }
}

/// The bytes of the synth definition `sine`:
/// `SynthDef(\sine, { |freq = 440, amp = 0.1| Out.ar(0, SinOsc.ar(freq, 0, amp) ! 2) })`.
///
/// # Errors
///
/// When scsynth-synthdef refuses the definition.
#[wasm_bindgen(js_name = sineDef)]
pub fn sine_def() -> Result<Vec<u8>, JsError> {
    let args = vec![Arg::new("freq", 440.0), Arg::new("amp", 0.1)];
    let mut builder = SynthDefBuilder::new("sine", args)?;
    let freq = builder.arguments()[0].clone();
    let amp = builder.arguments()[1].clone();
    let sine = builder.ugen(
        "SinOsc",
        "ar",
        &[("freq", freq.into()), ("mul", amp.into())],
    )?;
    let both = UGenInput::Array(vec![sine.clone(), sine]);
    builder.ugen(
        "Out",
        "ar",
        &[("bus", 0.0.into()), ("channelsArray", both.into())],
    )?;
    Ok(builder.finish()?.as_bytes()?)
}

/// The next event of `subscription`, or `None` once `stop` stops its loop.
async fn next(subscription: &mut Subscription, stop: &Stop) -> Option<Event> {
    let mut next = pin!(subscription.next());
    future::poll_fn(|cx| {
        if stop.stopped.get() {
            return Poll::Ready(None);
        }
        stop.waker.replace(Some(cx.waker().clone()));
        next.as_mut().poll(cx)
    })
    .await
}

/// The options of the crate from those of JS.
fn options_of(value: Option<JsValue>) -> Result<Options, JsError> {
    let given: JsOptions = match value {
        Some(value) => serde_wasm_bindgen::from_value(value)
            .map_err(|error| JsError::new(&format!("the options: {error}")))?,
        None => JsOptions::default(),
    };
    let defaults = Options::default();
    let timeout = match given.timeout_ms {
        Some(ms) => Duration::try_from_secs_f64(ms / 1000.0)
            .map_err(|_| JsError::new("timeoutMs is no number of milliseconds, 0 or more"))?,
        None => defaults.timeout,
    };
    Ok(Options {
        timeout,
        client_id: given.client_id,
        max_logins: given.max_logins,
        buffers: given.buffers.unwrap_or(defaults.buffers),
        control_buses: given.control_buses.unwrap_or(defaults.control_buses),
        audio_buses: given.audio_buses.unwrap_or(defaults.audio_buses),
        inputs: given.inputs.unwrap_or(defaults.inputs),
        outputs: given.outputs.unwrap_or(defaults.outputs),
    })
}

/// The target of the crate from a `Target` of JS.
fn target_of(value: JsValue) -> Result<Target, JsValue> {
    let target = serde_wasm_bindgen::from_value(value)
        .map_err(|error| js_error(format!("the target: {error}")))?;
    Ok(match target {
        JsTarget::Head(node) => Target::head(NodeId(node)),
        JsTarget::Tail(node) => Target::tail(NodeId(node)),
        JsTarget::Before(node) => Target::before(NodeId(node)),
        JsTarget::After(node) => Target::after(NodeId(node)),
    })
}

/// The control settings of a `Controls` of JS, in the order of its keys.
fn controls_of(value: &JsValue) -> Result<Vec<ControlSetting>, String> {
    let object = value
        .dyn_ref::<Object>()
        .ok_or("the controls are no object")?;
    Object::entries(object)
        .iter()
        .map(|entry| {
            let entry: Array = entry.unchecked_into();
            let name = entry.get(0).as_string().unwrap_or_default();
            let value = entry
                .get(1)
                .as_f64()
                .ok_or_else(|| format!("the control {name} is no number"))?;
            #[allow(
                clippy::cast_possible_truncation,
                reason = "scsynth takes a control value as a 32-bit float"
            )]
            Ok(ControlSetting::new(name, value as f32))
        })
        .collect()
}

/// The address of a message of scsynth.
fn address_of(incoming: &Incoming) -> &str {
    match incoming {
        Incoming::Reply(reply) => reply.address(),
        Incoming::Other(raw) => &raw.address,
    }
}

/// A message of scsynth as JS gets it: the serde form of its reply, or of
/// its `RawMessage`.
fn message_of(incoming: &Incoming) -> Result<JsValue, JsValue> {
    match incoming {
        Incoming::Reply(reply) => to_js(reply),
        Incoming::Other(raw) => to_js(raw),
    }
}

/// `value` as a JS value, with `None` as `null`, as in the JSON of
/// scsynth-osc.
fn to_js(value: &impl Serialize) -> Result<JsValue, JsValue> {
    let serializer = serde_wasm_bindgen::Serializer::new().serialize_missing_as_null(true);
    value.serialize(&serializer).map_err(js_error)
}

/// `undefined`, or the error as a JS `Error`.
fn done(outcome: Result<(), scsynth_client::Error>) -> Result<JsValue, JsValue> {
    outcome.map(|()| JsValue::UNDEFINED).map_err(js_error)
}

/// A JS `Error` with the text of `error`.
fn js_error(error: impl Display) -> JsValue {
    JsError::new(&error.to_string()).into()
}

/// Reports an error of a callback to the console as an uncaught error,
/// through the `reportError` of the global object.
fn report(error: &JsValue) {
    let report = Reflect::get(&js_sys::global(), &JsValue::from_str("reportError"))
        .ok()
        .and_then(|report| report.dyn_into::<Function>().ok());
    if let Some(report) = report {
        let _ = report.call1(&JsValue::NULL, error);
    }
}
