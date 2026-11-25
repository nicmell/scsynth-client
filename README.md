# scsynth-client

A client of scsynth, the synthesis server of SuperCollider, for Rust:
typed requests that wait for their replies, over UDP, TCP, a websocket or
a message port. The requests and the replies are the types of
[scsynth-osc](https://github.com/nicmell/scsynth-osc).

The crate is in development, and version 0.1 is not released.

## The transport

`OscClient` is a connection to scsynth: a transport. It sends the packets
of commands of scsynth-osc, `Packet<Command>`, and gives what arrives to
one listener. Three rules hold for every transport:

- `send` queues one packet and returns. It waits for no network and no
  reply. It refuses a packet over the limit of the transport with
  `SendError::TooLarge`, and any packet after the close with
  `SendError::Closed`.
- The listener gets the messages in the order of their arrival, one call
  at a time. `listen` sets the one listener, and a second call replaces
  the first. The transport keeps what arrives before `listen`.
- A write that fails after `send` returned comes to the listener as an
  event.

`close` closes the connection. The listener then gets `Event::Closed` once.

On a native target, an `OscClient` is `Send` and `Sync`, and a `Listener`
is `Send`. On `wasm32` they need not be, because a value of JavaScript is
not `Send`. The traits `MaybeSend` and `MaybeSync` state this rule.

### The events

| Event | Meaning |
| --- | --- |
| `Event::Message(incoming)` | One message from the other end. A bundle arrives as its messages, in order. |
| `Event::Error(error)` | Bytes that are no OSC packet, or an error that the connection survives |
| `Event::Closed` | The connection is closed. No event follows. |

An `Incoming` is one of two:

- `Incoming::Reply` holds a `Reply` of scsynth-osc.
- `Incoming::Other` holds a `RawMessage`: a message that does not decode
  as a reply, whatever the error. Its address can be unknown to
  scsynth-osc, such as the reply of a `SendReply` unit generator, or its
  arguments can be wrong for its address.

A `ClientError` is `ClientError::Decode`, with the `DecodeError` of
scsynth-osc, or `ClientError::Transport`, with the text of the error.

### Bytes

`decode(bytes)` gives the messages of the bytes of one OSC packet, in
order. Each transport of bytes uses it. It types each message that
decodes as a `Reply`, and keeps each other message as `Incoming::Other`.
So a bundle with one unknown message still gives its replies. Bytes that
are no OSC packet give `ClientError::Decode`.

## The channel pair

`ChannelClient::pair()` gives a `ChannelClient` and its `ChannelPeer`. The
pair carries typed values in memory, and no bytes. The client is an
`OscClient`. The peer is the other end: a test that plays scsynth, or a
host that carries the packets.

- `take` gives the oldest packet of the client that no handler got.
- `on_packet` sets the one handler, which gets each packet of the client
  in place of `take`, with the peer to answer through. The packets that
  wait go to it first, in order.
- `send` gives one message to the listener of the client, and `error`
  gives one `Event::Error`.
- `close` closes the pair, and `is_closed` says whether either end closed
  it.

A close from either end closes both. The listener of the client gets
`Event::Closed` once, after the events that wait. Then `ChannelClient::send`
gives `SendError::Closed`, and the peer drops what it sends.

The pair delivers in the same call stack. A call delivers what it adds
before it returns. A call from inside a listener or a handler only queues
what it adds. The delivery that runs delivers it after that listener or
handler returns. So a listener or a handler never runs inside another,
and a handler can answer from inside its call with no deadlock. No lock
is held while a listener or a handler runs. Each direction delivers in the
order of arrival. A call from another thread while a delivery runs also
only queues, and the thread of that delivery delivers it.

```rust
use std::sync::mpsc;

use scsynth_client::scsynth_osc::{Command, Packet, Reply, SyncId};
use scsynth_client::{ChannelClient, Event, Incoming, OscClient};

let (client, peer) = ChannelClient::pair();

// The peer plays scsynth: it answers each /sync with its /synced.
peer.on_packet(|peer, packet| {
    for command in packet.into_messages() {
        if let Command::Sync(id) = command {
            peer.send(Incoming::Reply(Reply::Synced(id)));
        }
    }
});

let (events, received) = mpsc::channel();
client.listen(Box::new(move |event| {
    let _ = events.send(event);
}));

// The answer comes before `send` returns.
client.send(&Packet::Message(Command::Sync(SyncId(1))))?;
let synced = Incoming::Reply(Reply::Synced(SyncId(1)));
assert_eq!(received.try_recv()?, Event::Message(synced));

client.close();
assert_eq!(received.try_recv()?, Event::Closed);
assert!(peer.is_closed());
# Ok::<(), Box<dyn std::error::Error>>(())
```

## The transports of tokio

With the feature `tokio`, on a native target, the crate has two transports
over the sockets of tokio: `UdpClient` and `TcpClient`. Each one runs a
reader task and a writer task. So its `connect` must run inside a runtime
of tokio.

- `send` encodes the packet, hands its bytes to the writer task, and
  returns. It waits for no network.
- Only the reader task calls the listener, one event at a time, with no
  lock held. So the listener can call `send`, `listen` and `close`. The
  events that arrive before `listen` wait in the reader task, and come at
  `listen`, in order.
- `close` stops both tasks, and closes the socket. The writer task first
  writes the packets that `send` queued before the close. The listener
  gets `Event::Closed` once. Then `send` gives `SendError::Closed`, and a
  second `close` does nothing.
- A client that drops closes as `close` does. It can drop inside its own
  listener, because the drop waits for no task.

### UDP

`UdpClient::connect(addr)` binds port 0 of the unspecified address of the
family of `addr`, and connects to `addr`. Each packet is one datagram,
each way. A datagram from scsynth comes as its messages, in order. Bytes
that are no OSC packet come as an `Event::Error` with
`ClientError::Decode`. The client receives into a buffer of 65,535 bytes.

`send` refuses a packet over the largest datagram with
`SendError::TooLarge`, and nothing leaves. The largest datagram is
`DEFAULT_MAX_DATAGRAM`, 8,192 bytes, unless
`UdpClient::connect_with_max_datagram` sets another. The system has a
limit of its own: macOS refused a datagram of 9,316 bytes and sent one of
9,016, and its `sysctl net.inet.udp.maxdgram` is 9,216.

An I/O error of a send or of a receive comes as an `Event::Error` with
`ClientError::Transport`, and the client goes on. On a connected UDP
socket, a scsynth that does not run answers "connection refused", and
scsynth can come back on the same port.

### TCP

`TcpClient::connect(addr)` connects a TCP stream to `addr`, with
`TCP_NODELAY` on. scsynth 3.14.1, started with `-t`, frames each packet
with its length: 32 bits in network byte order before the packet, each
way. The client writes the length and the packet in one write. It reads a
packet that arrives in any pieces, and one read can hold several packets.
scsynth 3.14.1 took a packet of 60,016 bytes over TCP, and `send` sets no
size limit of its own. It refuses only a packet over `i32::MAX` bytes,
with `SendError::TooLarge`: scsynth reads the length as a signed integer
(`SC_ComPort.cpp:368`).

A length that is negative or over 64 MiB comes as an `Event::Error`, and
then the client closes: after it, the stream cannot be read. scsynth sets
no such limit. The limit only guards against a stream that lost its
framing. An I/O error of a read or of a write also comes as an
`Event::Error`, and then the client closes. The end of the stream from
scsynth closes the client too.

A TCP connection that closes keeps its login in scsynth: see `close` in
[The methods](#the-methods).

scsynth takes `-u` and `-t` with the same port number: scsynth 3.14.1
answered over UDP and over TCP on one number.

### The timer of tokio

`TokioTimer` is a `Timer` whose sleeps are those of `tokio::time::sleep`.
`Scsynth::new(client, options)` is
`Scsynth::with_timer(client, TokioTimer, options)`. The runtime that polls
the awaited methods needs the time driver of tokio, which `enable_time` or
`enable_all` turns on. `#[tokio::main]` and `#[tokio::test]` turn it on.

### A session over UDP

```rust,no_run
use scsynth_client::scsynth_osc::{ControlSetting, NodeId};
use scsynth_client::scsynth_synthdef::{Arg, SynthDefBuilder};
use scsynth_client::{Error, Options, Scsynth, Target, UdpClient};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = UdpClient::connect("127.0.0.1:57110").await?;
    let scsynth = Scsynth::new(client, Options::default());
    scsynth.register().await?;

    // SynthDef(\sine, { |freq = 440| Out.ar(0, SinOsc.ar(freq)) })
    let mut builder = SynthDefBuilder::new("sine", vec![Arg::new("freq", 440.0)])?;
    let freq = builder.arguments()[0].clone();
    let sine = builder.ugen("SinOsc", "ar", &[("freq", freq.into())])?;
    builder.ugen("Out", "ar", &[("bus", 0.0.into()), ("channelsArray", sine.into())])?;
    scsynth.add_synthdef(&builder.finish()?).await?;

    let group = scsynth.group(Target::head(NodeId::ROOT)).await?;
    let freq = ControlSetting::new("freq", 440.0);
    let synth = scsynth.synth("sine", [freq], Target::tail(group)).await?;
    scsynth.set(synth, [ControlSetting::new("freq", 660.0)])?;

    // scsynth has no definition `missing`, and refuses the synth.
    match scsynth.synth("missing", [], Target::tail(group)).await {
        Err(Error::Failed { message, .. }) => println!("scsynth refused: {message}"),
        Err(error) => return Err(error.into()),
        Ok(node) => println!("{node:?} plays"),
    }

    scsynth.free(synth).await?;
    scsynth.close().await?;
    Ok(())
}
```

## The transports of the browser

With the feature `web`, on `wasm32`, the crate has two transports over
the objects of the browser: `WebSocketClient` and `PortClient`. They run
in a page and in a worker. On a native target, the feature holds nothing.
These transports build for `wasm32-unknown-unknown`, but no test runs
them yet.

- `send` encodes the packet, hands its bytes to the browser, and returns.
  The browser queues them, and `send` waits for no network.
- The handlers of the events of the browser call the listener, one event
  at a time, with no borrow held. So the listener can call `send`,
  `listen` and `close`. A call from inside the listener does not call the
  listener again: the events that it adds come after the listener
  returns. The events that arrive before `listen` wait, and come at
  `listen`, in order.
- The listener gets `Event::Closed` once, and no event after it.
- A client that drops removes its handlers, and its listener gets no more
  event.

### WebSocket

`WebSocketClient::open(url)` makes a `WebSocket` of the browser, with the
binary type `arraybuffer`, and ends at its `open` event. If the browser
refuses the URL, or if an `error` or a `close` event comes before the
`open` event, it gives `ClientError::Transport`.

Each packet is one binary frame, each way. A frame from the other end
comes as its messages, in order. Bytes that are no OSC packet come as an
`Event::Error` with `ClientError::Decode`, and a text frame as an
`Event::Error` with `ClientError::Transport`. Once the socket is not open,
`send` gives `SendError::Closed`.

`close` closes the socket. The other end and the network can close it
too. At the `close` event of the socket, the listener gets
`Event::Closed`. An `error` event of the open socket comes as an
`Event::Error` before it. A client that drops closes its socket.

scsynth has no websocket. So the other end of a `WebSocketClient` is a
bridge: a program that carries each packet between the websocket and
scsynth.

### Message ports

`PortClient` carries the packets over a target that posts messages:

| Constructor | Target |
| --- | --- |
| `PortClient::from_port(port)` | a `MessagePort`, which it starts |
| `PortClient::from_worker(worker)` | a `Worker`, on the side of the page that made it |
| `PortClient::from_worker_scope(scope)` | the `DedicatedWorkerGlobalScope`, on the side of the worker |

Each packet is one message, each way. `send` posts a `Uint8Array` of the
bytes of the packet, and transfers its buffer. A message whose data is a
`Uint8Array` or an `ArrayBuffer` comes as its messages, in order. Any
other data comes as an `Event::Error`, so the target carries the packets
alone.

A `Worker` and a worker scope dispatch each message once, so a message
that comes before the client exists is lost. A `MessagePort` keeps its
messages until the client starts it.

A port has no close event: when the other end closes it, the client gets
no event, and `send` goes on. `close` removes the handler of the client,
closes a `MessagePort`, and gives `Event::Closed`. Then `send` gives
`SendError::Closed`. `close` does not terminate a worker, and does not
close a worker scope. A client that drops does as `close`, with no
`Event::Closed`.

### The timer of the browser

`WebTimer` is a `Timer` whose sleeps are `setTimeout` calls of the global
object, so it runs in a page and in a worker. With the feature `web`, on
`wasm32`, `Scsynth::new(client, options)` is
`Scsynth::with_timer(client, WebTimer, options)`. `setTimeout` reads its
delay as a signed 32-bit integer. So a sleep over `i32::MAX`
milliseconds, about 24.8 days, ends after `i32::MAX` milliseconds.

## The client

`Scsynth` is a client of scsynth over one transport. Its awaited methods
send requests, and wait for the answer of scsynth within a limit. It is a
handle: its clones share one connection. On a native target it is `Send`
and `Sync`, and the future of each of its methods is `Send`.

`Scsynth::with_timer(client, timer, options)` makes a client over the
transport `client`, and sets the listener of that transport. The `Timer`
makes the sleeps that the awaited methods race against their limit: its
`sleep(limit)` gives a `Sleep`, a future that ends at the end of the
limit. The client reads no clock of its own, because
`std::time::Instant::now` panics on `wasm32-unknown-unknown`: the timer
makes every sleep. The client needs no runtime of its own: whatever polls
its futures and the sleeps of its timer runs it. With the feature
`tokio`, `Scsynth::new` takes the timer of tokio (see
[The timer of tokio](#the-timer-of-tokio)). With the feature `web`, on
`wasm32`, it takes the timer of the browser (see
[The timer of the browser](#the-timer-of-the-browser)). On another
runtime, the caller writes its own timer.

```rust
use std::future;
use std::time::Duration;

use scsynth_client::scsynth_osc::{Command, Packet};
use scsynth_client::{ChannelClient, Options, Scsynth, Sleep, Timer};

/// A timer whose sleeps never end: no call times out.
struct Never;

impl Timer for Never {
    fn sleep(&self, _limit: Duration) -> Sleep {
        Box::pin(future::pending())
    }
}

let (client, peer) = ChannelClient::pair();
let scsynth = Scsynth::with_timer(client, Never, Options::default());

// A plain send leaves at once, and waits for nothing.
let status = Packet::Message(Command::Status);
scsynth.send(&status)?;
assert_eq!(peer.take(), Some(status));
# Ok::<(), scsynth_client::Error>(())
```

### Requests

`request(commands)` sends a request, and waits for it. A request settles
in five steps:

1. It queues. Once no other request is in flight, it leaves as one
   immediate bundle: its commands, then `/sync k`.
2. While it is in flight, the client keeps every reply that arrives, and
   each `/fail` whose command is one of its commands. The commands of a
   completion count too.
3. `/synced k` settles it: with the first kept `/fail` as `Error::Failed`,
   else with the kept replies, in order.
4. The limit: when `/synced k` does not come within the limit, the request
   settles with `Error::Timeout`. The limit counts from the first poll of
   the call, so the time in the queue counts too.
5. The barrier: after a timeout of the request in flight, the client sends
   a lone `/sync b`. No request leaves until scsynth answers `/sync b` or
   the `/sync k` of the late request. So the late replies of that request
   land on no other request.

One request is in flight at a time. In 20 of 20 trials, scsynth 3.14.1
sent the `/fail` of a second request before the `/synced` of the first.
A `/fail` carries no id. So with two requests in flight, the client cannot
tell whose `/fail` it is.

The cost is one round trip for each request. On the loopback address, the
median was 7.8 ms over TCP and 8.0 ms over UDP. JACK ran at 44.1 kHz,
with blocks of 256 frames. `request` takes many commands, which leave in
one bundle for one round trip.

A call that is dropped before it settles counts as timed out. A queued
request leaves the queue and is never sent, and a request in flight leaves
a barrier.

### Plain sends

`send(packet)` sends one packet at once, outside the queue, and waits for
nothing. Only a refusal of the transport makes it fail. scsynth
answers it as any other packet: a request in flight keeps its replies, and
the subscriptions get them. A `/fail` of a plain send can land on a
request in flight that holds the same address. That request then settles
with `Error::Failed`.

### The methods

| Method | Sends | Gives |
| --- | --- | --- |
| `request(commands)` | the commands, then `/sync`, through the queue | the replies kept while it was in flight |
| `sync()` | `/sync`, through the queue | `()`, once scsynth has done what the requests before it asked |
| `status()` | `/status`, a plain send | the next `/status.reply` |
| `version()` | `/version`, a plain send | the next `/version.reply` |
| `quit()` | `/quit`, a plain send | `()`, at the next `/done /quit` |
| `register()` | `/notify 1`, through the queue | a `Registration`: the client id and the login limit |
| `unregister()` | `/notify 0`, through the queue | `()` |
| `close()` | for a registered client, `/notify 0`, then the close of the transport | `()`, or the error of `unregister` |
| `send(packet)` | the packet, a plain send | `()`, at once |
| `timeout(limit)` | nothing | the same client, with `limit` as the limit of its awaited methods |
| `subscribe()` | nothing | a `Subscription` |

`status`, `version` and `quit` wait outside the queue. Each one waits for
its reply first, then sends its command. Every call that waits for the
same reply gets the next one. `quit` sends no `/sync`, because scsynth
exits after `/quit`.

When `Options::client_id` is set, `register` asks for it. scsynth answers
`/done /notify clientID maxLogins`. When the reply has no login limit,
`register` takes `Options::max_logins`, and with neither it gives
`Error::NoReply`. scsynth 3.14.1 answers a `/notify 1` on a connection
that it registered already with `/fail /notify "notify: already
registered\n"` and the client id. With `Options::max_logins`, `register`
takes that as a registration. Without it, `register` gives the
`Error::Failed`. `unregister` makes the client forget its registration
once scsynth answers.

A TCP connection that closed without `/notify 0` keeps its login in
scsynth: with a limit of 4 logins, scsynth 3.14.1 answered the fifth
registration with "too many users". So `close` unregisters a registered
client first. Then it closes the transport in every case. If `unregister`
failed, `close` gives its error. A client whose last handle drops closes
its transport with no `/notify 0`: call `close` first.

### Options

| Field | Meaning | Default |
| --- | --- | --- |
| `timeout` | the limit of each awaited method | 3 seconds |
| `client_id` | the client id that `register` asks for | `None`: scsynth picks one |
| `max_logins` | the login limit that scsynth reports in the reply of `/notify`, for a reply that does not give it | `None` |
| `buffers` | the number of buffers of the server, its `-b` | 1024 |
| `control_buses` | the number of control buses of the server, its `-c` | 16384 |
| `audio_buses` | the number of audio buses of the server, its `-a`, with the outputs and the inputs | 1024 |
| `inputs` | the number of input buses of the server, its `-i` | 8 |
| `outputs` | the number of output buses of the server, its `-o` | 8 |

A caller writes `Options { timeout, ..Options::default() }`.

scsynth reports its `-l` rounded up to a power of two: scsynth 3.14.1
started with `-l 3` answered a limit of 4. So `max_logins` is then 4: with
3, the ranges of ids of two clients overlap.

The defaults of the five counts are those of scsynth. scsynth tells no
client its counts: no reply gives them. So they must match the flags of
the server. scsynth does not refuse an id over its counts: scsynth 3.14.1
with 1,024 buffers answered `/b_alloc 99999` with `/done /b_alloc 99999`,
and wrote buffer 0. sclang boots scsynth with `-i 2 -o 2` by default, so a
client that shares such a server sets `inputs` and `outputs` to 2.

### The ids

A registered client takes the ids of its nodes, its buffers and its buses
from ranges of its own. The ranges are those that sclang 3.14.1 gives to
the same client id under the same login limit. So the client can share a
scsynth with sclang, and with other clients of this crate. For the client
id `N` and the login limit `L`:

| Ids | Range | Next id |
| --- | --- | --- |
| nodes | `N << 26` to `(N << 26) + 0x03FF_FFFF` | from `(N << 26) + 1000`, up by one, then back to `(N << 26) + 1000` after the last |
| buffers | `s` ids from `s * N`, with `s = buffers / L` | the first free run that fits, lowest index first |
| control buses | `s` ids from `s * N`, with `s = control_buses / L` | the first free run that fits, lowest index first |
| audio buses | `s` ids from `f + s * N`, with `f = outputs + inputs` and `s = (audio_buses - f) / L` | the first free run that fits, lowest index first |

The divisions round down. The audio buses under `outputs + inputs` are the
outputs and the inputs of the hardware, and no client takes them. A login
limit of 0 or less, or a share of 0, leaves a range empty. A range that
ends over the count of the server is empty too. Each range of a client id
at or over the login limit is such a range. So each alloc there gives
`Error::Exhausted`, and no id passes the count. The reserved
counts of sclang are 0 by default, and this client has none. A free gives
a run back, and joins it with the free runs next to it. sclang picks among
its free runs in another order, so only the ranges are shared.

The login limit is the one of the `Registration`: the limit in the reply of
`/notify`, else `Options::max_logins`. Neither is the `-l` of the server
(see [Options](#options)).

sclang and this crate split the node ids among 32 clients, 0 to 31.
scsynth gives client ids under its login limit, which is 64 by default. So
for a client id under 0 or over 31, `register` sends `/notify 0`, so that
scsynth does not keep the login. Then it gives `Error::NoNodeRange`,
whatever scsynth answered, and the client is not registered.

When the client id that `/notify` asks for is free, scsynth gives it.
Else it gives the free id that waited longest: a freed id goes to the end
of its list (`SC_SequencedCommand.cpp:1271`). One connection of scsynth
3.14.1 that registered and unregistered four times got the ids 0, 1, 2
and 3. So after 32 registrations, a client without `Options::client_id`
can get an id over 31, even as the only client.

Each registration starts fresh ranges, and `unregister` forgets them. The
methods that take an id from the ranges of the client need `register`
first: before it, they give `Error::NotRegistered`. When no free run that
fits is left in a range, they give `Error::Exhausted`, with the `IdKind`
of the range.

### Errors

| Error | Meaning |
| --- | --- |
| `Error::Failed` | scsynth answered `/fail` for a command of the request: the command, the message and the value of the `/fail` |
| `Error::Timeout` | the answer did not come within the limit |
| `Error::Send` | the transport refused the packet, or it closed |
| `Error::NoReply` | scsynth answered without the reply, or the value of a reply, that the method reads |
| `Error::NotRegistered` | the method needs `register` first |
| `Error::NoNodeRange` | the client id that scsynth gave has no range of node ids: it is under 0 or over 31 |
| `Error::Exhausted` | no free id is left in the range of the client, for the `IdKind` that it holds |
| `Error::OutOfRange` | the id is at or over the count of its kind in the `Options`: the kind and the id |
| `Error::Read` | the bytes are no synth definition file: the `ReadError` of scsynth-synthdef |
| `Error::Limit` | the synth definition breaks a limit of scsynth: the `LimitError` of scsynth-synthdef |

A close of the transport settles every pending call with
`Error::Send(SendError::Closed)`, and each call after it gets the same
error.

### Subscriptions

`subscribe()` gives a `Subscription`: every event of the transport from
the call on, in order, after the client used it. That is every message,
also one that a request takes, every `Event::Error`, and `Event::Closed`
as the last one. Then its async `next()` gives `None`. It also gives
`None` once every handle of the client is dropped, and at once for a
subscription made after the close. A dropped subscription gets nothing
more.

### Nodes

The node methods make, move, query, set and free the synths and the
groups of scsynth. They take and give the types of scsynth-osc: `NodeId`,
`ControlSetting`, `ControlMapping`, `Control`, the `NodeState` of
`reply::node_notification` and the `GQueryTreeReply` of `reply::group`.

| Method | Sends | Gives |
| --- | --- | --- |
| `group(target)` | `/g_new` with the next node id, through the queue | the `NodeId` of the group |
| `synth(name, controls, target)` | `/s_new` with the next node id, through the queue | the `NodeId` of the synth |
| `free(node)` | `/n_free`, through the queue | `()` |
| `run(node, on)` | `/n_run`, through the queue | `()` |
| `free_all(group)` | `/g_freeAll`, through the queue | `()` |
| `deep_free(group)` | `/g_deepFree`, through the queue | `()` |
| `move_node(node, target)` | `/g_head`, `/g_tail`, `/n_before` or `/n_after`, by the target, through the queue | `()` |
| `query(node)` | `/n_query`, through the queue | the `NodeState` of the `/n_info` of the node |
| `tree(group, controls)` | `/g_queryTree`, through the queue | the `GQueryTreeReply` of the group. If `controls` is true, it holds the values of the controls. |
| `get(node, control)` | `/s_get` of one control, through the queue | the value of the control, an `f32`, from the `/n_set` of the node |
| `set(node, controls)` | `/n_set`, a plain send | `()`, at once |
| `map(node, mappings)` | `/n_map`, a plain send | `()`, at once |
| `ended(node, limit)` | nothing | a future that gives `()` at the `/n_end` of the node |

`free_all` frees every node in the group. `deep_free` frees every synth
in the group and in the groups inside it, and keeps the groups.
`controls` and `mappings` take any list: an array, a `Vec` or an
iterator. An empty list is `[]`.

A `Target` is where a new node goes, or where a node moves:

| Target | Place |
| --- | --- |
| `Target::head(group)` | at the head of the group, where the node runs first |
| `Target::tail(group)` | at the tail of the group, where the node runs last |
| `Target::before(node)` | just before the node, in its group |
| `Target::after(node)` | just after the node, in its group |

The root group of scsynth is node 0, `NodeId::ROOT`. `Target` does not
take the fifth add action of scsynth-osc, `Replace`.

`group` and `synth` take the next node id of the client (see
[The ids](#the-ids)). Before `register`, they give `Error::NotRegistered`,
and send nothing. If the request of a call fails, the node id that the
call took stays taken.

```rust
use std::time::Duration;

use scsynth_client::scsynth_osc::{ControlSetting, NodeId};
use scsynth_client::{Error, Scsynth, Target};

/// A synth of the definition `sine` in a new group: it plays at 440 Hz,
/// then at 660 Hz, until `free` ends it.
async fn play(scsynth: &Scsynth) -> Result<(), Error> {
    scsynth.register().await?;
    let group = scsynth.group(Target::head(NodeId::ROOT)).await?;
    let freq = ControlSetting::new("freq", 440.0);
    let synth = scsynth.synth("sine", [freq], Target::tail(group)).await?;
    let ended = scsynth.ended(synth, Duration::from_secs(10));
    scsynth.set(synth, [ControlSetting::new("freq", 660.0)])?;
    scsynth.free(synth).await?;
    ended.await
}
```

#### Notifications

scsynth 3.14.1 sends `/n_go`, `/n_end`, `/n_on`, `/n_off`, `/n_move` and
`/n_info` to every registered client, whichever client sent the command.
It sends `/synced`, `/fail` and `/g_queryTree.reply` to the sender alone.
A client that is not registered gets `/synced` and `/fail`, and no
`/n_go`.

So an awaited method settles at the `/synced` of its request, and needs
no `/n_go`. scsynth sends the `/n_go` of a bundle before its `/synced`.
So when `synth` or `group` gives `Ok`, the node exists.

scsynth 3.14.1 also sends the `/n_info` of `/n_query` and the `/n_set`
of `/s_get` before the `/synced` of their bundle. `query` takes the
`/n_info` of its node among the replies, because a query of another
client can put the `/n_info` of another node there. `get` takes the
`/n_set` of its node, and `tree` the reply of its group. Without that
reply, each one gives `Error::NoReply`. A client that is not registered
gets no `/n_info`, so its `query` gives `Error::NoReply`.

#### Set and map

`set` and `map` send one message at once, outside the queue, as `send`
does, and wait for nothing. No awaited method of the client sends `/n_set`
or `/n_map`. So only a `request` of the caller with the same address can
take the `/fail` of a `set` or a `map` (see [Plain sends](#plain-sends)).

#### Ended

`ended(node, limit)` gives a future that ends at the `/n_end` of the node.
It is no `async fn`: its wait starts at the call. So an `/n_end` that
comes between the call and the first poll is kept. Call it before the
command that ends the node, such as `free`. An `/n_end` that came before
the call is not seen, and the future then gives `Error::Timeout`.

Its limit is its own argument, and not the limit of the client, because
a node can live long. The sleep of the limit starts at the first poll, as
for every call. scsynth sends `/n_end` to registered clients alone. So a
client that is not registered at the call gets `Error::NotRegistered`
from the future, and no wait starts.

#### Failures

scsynth 3.14.1 answered these commands with a `/fail`, which the method
gives as `Error::Failed`, with the message:

| Command | Message |
| --- | --- |
| `/s_new` of a definition that scsynth does not have | `SynthDef not found` |
| `/s_new` with a node id in use | `duplicate node ID` |
| `/s_new` with a target group 9999 that does not exist | `Group 9999 not found` |
| `/n_free 9999` of a node that does not exist | `Node 9999 not found` |

### Asynchronous commands

scsynth runs `/d_recv`, `/d_load`, `/b_alloc`, `/b_allocRead` and
`/b_free` asynchronously, and answers each one with a `/done` to its
sender. scsynth 3.14.1 sends no reply at all for a `/b_alloc` that it
cannot allocate (`SC_SequencedCommand.cpp:292-302`): it answered a
`/b_alloc` of 0 frames with the `/synced` of the request alone. So each
method that sends one of these commands needs its `/done` among the
replies, and else gives `Error::NoReply`. The `/done` of a buffer command must name the buffer of
the method. `read_buffer` needs the `/b_info` of its completion in place
of the `/done`. `/d_free` is not asynchronous, and answers nothing, so
`free_synthdef` needs no reply.

### Synth definitions

The definition methods send synth definitions to scsynth, and remove them.
They take the `SynthDef` of scsynth-synthdef, which its `SynthDefBuilder`
builds.

| Method | Sends | Gives |
| --- | --- | --- |
| `add_synthdef(def)` | `/d_recv` with the bytes of `def`, through the queue | `()`, at the `/done /d_recv` |
| `add_synthdef_bytes(bytes)` | `/d_recv` with `bytes`, through the queue, once they read as a definition file | `()`, at the `/done /d_recv` |
| `load_synthdef(path)` | `/d_load`, through the queue | `()`, at the `/done /d_load` |
| `free_synthdef(name)` | `/d_free` of one name, through the queue | `()` |

`add_synthdef` writes the bytes with `SynthDef::as_bytes`. For a
definition that breaks a limit of scsynth, it gives `Error::Limit` with
the `LimitError` of scsynth-synthdef, and sends nothing.
`add_synthdef_bytes` reads the bytes with `read_def_file` first. For bytes
that are no definition file, it gives `Error::Read` with the `ReadError`
of scsynth-synthdef, and sends nothing.

```rust
use scsynth_client::Scsynth;
use scsynth_client::scsynth_synthdef::{Arg, SynthDefBuilder};

/// Sends `SynthDef(\sine, { |freq = 440| Out.ar(0, SinOsc.ar(freq)) })`.
async fn define(scsynth: &Scsynth) -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = SynthDefBuilder::new("sine", vec![Arg::new("freq", 440.0)])?;
    let freq = builder.arguments()[0].clone();
    let sine = builder.ugen("SinOsc", "ar", &[("freq", freq.into())])?;
    builder.ugen("Out", "ar", &[("bus", 0.0.into()), ("channelsArray", sine.into())])?;
    scsynth.add_synthdef(&builder.finish()?).await?;
    Ok(())
}
```

The traps:

- scsynth 3.14.1 answers `/d_recv` with `/done /d_recv` also for bytes
  that are no definition. So only the client can refuse them.
- A definition that needs more wire buffers than the `-w` of scsynth also
  gets `/done /d_recv`, and scsynth 3.14.1 does not load it. The client
  cannot see it. `SynthDef::wire_buffers` of scsynth-synthdef counts the
  wire buffers of a definition.
- The path of `/d_load` is a path on the host of scsynth, which reads the
  file. For a path where it finds no file, scsynth 3.14.1 answers
  `/fail /d_load` with the path as the message, and no `/done`.
  `load_synthdef` gives it as `Error::Failed`.
- scsynth 3.14.1 sends nothing for a `/d_free` of a name that it does not
  have. So `free_synthdef` gives `Ok` at the `/synced`.

### Buffers

The buffer methods allocate, read, query and free the buffers of
scsynth. They take and give the types of scsynth-osc: `BufferId`, and the
`BufferInfo` of `reply::buffer`, which holds the buffer, its frames, its
channels and its sample rate.

| Method | Sends | Gives |
| --- | --- | --- |
| `alloc_buffer(frames, channels)` | `/b_alloc` with the next free buffer id, at the sample rate of the server, through the queue | the `BufferId`, at the `/done /b_alloc` of the buffer |
| `read_buffer(path)` | one `/b_allocRead` of the whole file with the next free buffer id, with a `/b_query` of the buffer as its completion, through the queue | the `BufferInfo` of the buffer, from the `/b_info` of that query |
| `free_buffer(buffer)` | `/b_free`, through the queue | `()`, at the `/done /b_free` of the buffer |
| `query_buffer(buffer)` | `/b_query`, through the queue | the `BufferInfo` of the buffer, from its `/b_info` |
| `get_samples(buffer, start, count)` | `/b_getn` of one run, through the queue | the values of the run, each an `f32`, from the `/b_setn` of the buffer |

`alloc_buffer` and `read_buffer` take the next free buffer id of the
client (see [The ids](#the-ids)). Before `register`, they give
`Error::NotRegistered`, and send nothing. When the call fails or is
dropped, the buffer id goes back to the client, and the next call can take
it. If the client holds the buffer id, `free_buffer` gives it back at the
`/done /b_free` of the buffer. On an error, the client keeps it.
`free_buffer`,
`query_buffer` and `get_samples` take any buffer id, and need no
`register`.

scsynth does not refuse a buffer id over its count (see
[Options](#options)). So each method that takes a `BufferId` gives
`Error::OutOfRange` for one at or over `Options::buffers`, and sends
nothing.

`alloc_buffer` takes its `frames` and its `channels` as `u32`, and
`get_samples` takes its `start` as a `u32` and its `count` as a `u16`. So
no count is negative. scsynth sizes the reply of `/b_getn` by its count
before it checks the run. A negative count makes it write outside its
memory (see the traps of scsynth-osc). A value over `i32::MAX` goes as
`i32::MAX`.

The samples of `get_samples` come in one reply. scsynth builds a reply in
65,516 bytes (`big_scpacket` of `common/scsynthsend.h`), and a datagram
over IPv4 holds at most 65,507 bytes. scsynth raises the send buffer of
its UDP socket (`SC_ComPort.cpp:268-281`). The limit of macOS on a
datagram does not hold for its replies. So one reply of scsynth 3.14.1
held at most 13,095 samples over UDP, and 13,098 over TCP. Over UDP, a
larger reply does not leave scsynth, and `get_samples` gives
`Error::NoReply`. Over TCP, scsynth answers
`/fail /b_getn "buffer overflow"`, and `get_samples` gives it as
`Error::Failed`.

scsynth 3.14.1 runs the completion of `/b_allocRead` once it has read the
file, then answers `/done`. So the `/b_info` of the buffer comes before
the `/synced`. For a file that it cannot open, scsynth 3.14.1 answers
`/fail /b_allocRead "File '...' could not be opened: ...\n"` with the
buffer as its value, and runs no completion. `read_buffer` then gives
`Error::Failed`. Without a `/b_info` of the buffer, `read_buffer` gives
`Error::NoReply`. `query_buffer` and `get_samples` take the reply of their
buffer among the replies, and give `Error::NoReply` without it.

The traps:

- The path of a sound file is a path on the host of scsynth, which reads
  the file.
- scsynth 3.14.1 multiplies the frames by the channels of `/b_alloc` in
  an `int` (`SC_World.cpp:1048`). A product over `i32::MAX` wraps. Then
  scsynth fails to allocate, or allocates fewer samples than the frames
  and the channels that it keeps for the buffer.

### Buses

A bus of scsynth is only an index: scsynth allocates no bus, and knows no
owner of one. The bus methods give runs of indices from the ranges of the
client, and set and get the values of control buses.

| Method | Sends | Gives |
| --- | --- | --- |
| `control_bus(count)` | nothing | the first index of a run of `count` free control buses of the client |
| `audio_bus(count)` | nothing | the first index of a run of `count` free audio buses of the client |
| `free_control_bus(first)` | nothing | `()`: the run of control buses that starts at `first` goes back to the client |
| `free_audio_bus(first)` | nothing | `()`: the run of audio buses that starts at `first` goes back to the client |
| `set_bus(index, value)` | `/c_set` of one control bus, a plain send | `()`, at once |
| `get_bus(index)` | `/c_get` of one control bus, through the queue | the value of the bus, an `f32`, from the `/c_set` that answers |

An index is an `i32`, as `/c_set`, `/n_map` and `/n_mapa` take it in
scsynth-osc. `control_bus` and `audio_bus` take a run from the range of
the client (see [The ids](#the-ids)), and need `register` first, as do
`free_control_bus` and `free_audio_bus`. A count of 0 gives
`Error::Exhausted`. A free of a run that the client does not hold changes
nothing. `set_bus` and `get_bus` take any index of a control bus, and need
no `register`.

`set_bus` sends one message at once, outside the queue, as `send` does,
and waits for nothing. No awaited method of the client sends `/c_set`. So
only a `request` of the caller with `/c_set` can take the `/fail` of a
`set_bus` (see [Plain sends](#plain-sends)). For a `/c_get` of an index
out of range, scsynth 3.14.1 answers `/fail /c_get "index out of range"`.
`get_bus` gives it as `Error::Failed`. Without a `/c_set` that holds its
index, `get_bus` gives `Error::NoReply`.

## Tests

The unit tests and the tests of `tests/` play scsynth in memory, or on
the loopback address. `cargo test --all-features` runs them all.

`tests/scsynth.rs` checks the client against a real scsynth. Each test
proves one fact of scsynth 3.14.1 that this file states. Two tests go
through every method of the client, one over UDP and one over TCP. These
tests are ignored by default, and pass at once without `SCSYNTH_ADDR`.
Run them against a scsynth on the same host, with the default counts of
its flags, that no other client uses:

```sh
scsynth -u 57341 -t 57341 -B 127.0.0.1 &
SCSYNTH_ADDR=127.0.0.1:57341 cargo test --all-features --test scsynth -- --ignored
```

The tests run in parallel, and each one closes its clients. On the
server, they use:

- the client ids 1 to 18, with the nodes, the buffers and the control
  buses of their ranges
- the nodes from 2,013,266,920 to 2,013,266,959, in the range of the
  client id 30, with no registration
- buffer 0, which a `/b_alloc` of buffer 99,999 writes, and which they
  free
- synth definitions whose names start with `scsynth-client-`

No definition of the tests has an output, so no test makes a sound. The
tests write a WAV file in the temporary directory, and remove it. A test
that fails can leave its login in scsynth. Then a later run cannot get
the client id that it asks for: restart scsynth after a failed run.

## License

Licensed under either of the Apache License, Version 2.0
(`LICENSE-APACHE`) or the MIT license (`LICENSE-MIT`), at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this crate by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
