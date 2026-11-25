# scsynth-client

A client of scsynth, the synthesis server of SuperCollider, for Rust:
typed requests that wait for their replies, over UDP, TCP, a websocket or
a message port. The crate
[scsynth-osc](https://github.com/nicmell/scsynth-osc) gives the types of
the commands and the replies. The crate
[scsynth-synthdef](https://github.com/nicmell/scsynth-synthdef) gives the
synth definitions. This crate re-exports both, as
`scsynth_client::scsynth_osc` and `scsynth_client::scsynth_synthdef`.

The crate follows scsynth 3.14.1. Each fact of scsynth that this file
states comes from the source of scsynth, or from a measurement with it.
Version 0.1.0 is not released yet.

## A session

A session over UDP, with the feature `tokio`. It registers, and sends a
synth definition. It plays a synth in a group of its own, and changes its
frequency. It asks for a synth of a definition that scsynth does not
have. Then it frees the synth, and closes.

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

## Features

- With no feature, on any target, the crate has `Scsynth` with a timer of
  the caller, the trait `OscClient`, the channel pair and `decode`.
- `tokio`, on a native target: `UdpClient`, `TcpClient`,
  `DEFAULT_MAX_DATAGRAM`, the timer `TokioTimer`, and `Scsynth::new` with
  that timer. See [The transports of tokio](#the-transports-of-tokio).
- `web`, on `wasm32`: `WebSocketClient`, `PortClient`, the timer
  `WebTimer`, and `Scsynth::new` with that timer. See
  [The transports of the browser](#the-transports-of-the-browser).
- `serde`: the feature `serde` of scsynth-osc and of scsynth-synthdef, for
  their commands, replies and synth definitions. The types of this crate
  have no serde form.

`tokio` holds nothing on `wasm32`, and `web` holds nothing on a native
target. So `--all-features` builds on both, and `Scsynth::new` takes the
timer of the target.

## The client

`Scsynth` is a client of scsynth over one transport. Its awaited methods
send requests, and wait for the answer of scsynth within a limit. It is a
handle: its clones share one connection. On a native target it is `Send`
and `Sync`, and the future of each of its methods is `Send`.

`Scsynth::with_timer(client, timer, options)` makes a client over the
transport `client`, and sets the listener of that transport. The `Timer`
makes the sleep that each awaited method races against its limit: its
`sleep(limit)` gives a `Sleep`, a future that ends at the end of the
limit. The client reads no clock of its own, because
`std::time::Instant::now` panics on `wasm32-unknown-unknown`. It needs no
runtime of its own: whatever polls its futures and the sleeps of its timer
runs it. `Scsynth::new(client, options)` takes the timer of tokio or of
the browser (see [Features](#features)). On another runtime, the caller
writes a timer:

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
   land on no other request. A queued request that times out while the
   barrier waits sends one more `/sync`, and the answer of any of them
   ends the barrier. So a lost datagram holds the queue until the next
   timeout at most.

One request is in flight at a time, because of the first two
[traps](#traps). The cost is one round trip for each request. On the
loopback address, the median was 7.8 ms over TCP and 8.0 ms over UDP.
JACK ran at 44.1 kHz, with blocks of 256 frames. `request` takes many
commands, which leave in one bundle for one round trip.

A call that is dropped before it settles counts as timed out. A queued
request leaves the queue and is never sent, and a request in flight leaves
a barrier.

### Plain sends

`send(packet)` sends one packet at once, outside the queue, and waits for
nothing. Only a refusal of the transport makes it fail. scsynth answers it
as any other packet: a request in flight keeps its replies, and the
subscriptions get them. A `/fail` of a plain send can land on a request in
flight that holds the same address. That request then settles with
`Error::Failed`. A bundle with a time tag ahead goes through `send`,
because a request leaves at once.

### The session

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

`register` sends `/notify 1`. When `Options::client_id` is set, the
command asks for that client id. scsynth answers `/done /notify clientID maxLogins`, and `register` gives
both as a `Registration`. When the reply has no login limit, `register`
takes `Options::max_logins`, and with neither it gives `Error::NoReply`.
scsynth 3.14.1 answers a `/notify 1` on a connection that it registered
already with `/fail /notify "notify: already registered\n"` and the client
id. With `Options::max_logins`, `register` takes that as a registration.
Without it, `register` gives the `Error::Failed`. A client id under 0 or
over 31 has no range of node ids. For such an id, `register` sends
`/notify 0`, so that scsynth does not keep the login. Then it gives
`Error::NoNodeRange`, whatever scsynth answered, and the client is not
registered.

`unregister` makes the client forget its registration and its ids once
scsynth answers, with a `/done` or a `/fail`.

`close` unregisters a registered client first, because a connection that
closes keeps its login (see [Traps](#traps)). Then it closes the transport
in every case. If `unregister` failed, `close` gives its error. A client
whose last handle drops drops its transport with no `/notify 0`: call
`close` first.

### Subscriptions

`subscribe()` gives a `Subscription`: every event of the transport from
the call on, in order, after the client used it. That is every message,
also one that a request takes, every `Event::Error`, and `Event::Closed`
as the last one. Then its async `next()` gives `None`. It also gives
`None` once every handle of the client is dropped, and at once for a
subscription made after the close. A dropped subscription gets nothing
more.

## The methods

The methods below send the commands of the nodes, the synth definitions,
the buffers and the buses. Any other command of scsynth-osc goes through
`request` or `send`.

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

#### Notifications

An awaited method settles at the `/synced` of its request, and needs no
`/n_go`. A client that is not registered gets no `/n_go` (see
[Traps](#traps)). scsynth sends the `/n_go` of a bundle before its
`/synced`. So when `synth` or `group` gives `Ok`, the node exists.

scsynth 3.14.1 also sends the `/n_info` of `/n_query` and the `/n_set` of
`/s_get` before the `/synced` of their bundle. It sends `/synced`, `/fail`
and `/g_queryTree.reply` to the sender alone. `query` takes the `/n_info`
of its node among the replies, because a query of another client puts the
`/n_info` of another node there. `get` takes the `/n_set` of its node, and
`tree` the reply of its group. Without that reply, each one gives
`Error::NoReply`.

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

```rust
use std::time::Duration;

use scsynth_client::scsynth_osc::NodeId;
use scsynth_client::{Error, Scsynth};

/// Frees `synth`, and waits for its `/n_end`.
async fn stop(scsynth: &Scsynth, synth: NodeId) -> Result<(), Error> {
    let ended = scsynth.ended(synth, Duration::from_secs(1));
    scsynth.free(synth).await?;
    ended.await
}
```

Its limit is its own argument, and not the limit of the client, because
a node can live long. The sleep of the limit starts at the first poll, as
for every call. A client that is not registered at the call gets no
`/n_end`. So it gets `Error::NotRegistered` from the future, and no wait
starts.

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
sender. A `/b_alloc` that fails gets no reply at all (see
[Traps](#traps)). So each method that sends one of these commands needs
its `/done` among the replies, and else gives `Error::NoReply`. The
`/done` of a buffer command must name the buffer of the method.
`read_buffer` needs the `/b_info` of its completion in place of the
`/done`. `/d_free` is not asynchronous, and answers nothing, so
`free_synthdef` needs no reply.

### Synth definitions

The definition methods send synth definitions to scsynth, and remove them.
They take the `SynthDef` of scsynth-synthdef, which its `SynthDefBuilder`
builds, as [A session](#a-session) shows.

| Method | Sends | Gives |
| --- | --- | --- |
| `add_synthdef(def)` | `/d_recv` with the bytes of `def`, through the queue | `()`, at the `/done /d_recv` |
| `add_synthdef_bytes(bytes)` | `/d_recv` with `bytes`, through the queue, once they read as a definition file | `()`, at the `/done /d_recv` |
| `load_synthdef(path)` | `/d_load`, through the queue | `()`, at the `/done /d_load` |
| `free_synthdef(name)` | `/d_free` of one name, through the queue | `()` |

`add_synthdef` writes the bytes with `SynthDef::as_bytes`. For a
definition that breaks a limit of scsynth or of the format, it gives
`Error::Limit` with the `LimitError` of scsynth-synthdef, and sends
nothing. `add_synthdef_bytes` reads the bytes with `read_def_file` first,
because scsynth takes any bytes (see [Traps](#traps)). For bytes that are
no definition file, it gives `Error::Read` with the `ReadError` of
scsynth-synthdef, and sends nothing.

The path of `/d_load` is a path on the host of scsynth, which reads the
file. For a path where it finds no file, scsynth 3.14.1 answers
`/fail /d_load` with the path as the message, and no `/done`.
`load_synthdef` gives it as `Error::Failed`.

scsynth 3.14.1 sends nothing for a `/d_free` of a name that it does not
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
`free_buffer`, `query_buffer` and `get_samples` take any buffer id, and
need no `register`.

scsynth takes a buffer id over its count (see [Traps](#traps)). So each
method that takes a `BufferId` refuses one at or over `Options::buffers`
with `Error::OutOfRange`, and sends nothing.

`alloc_buffer` takes its `frames` and its `channels` as `u32`, and
`get_samples` takes its `start` as a `u32` and its `count` as a `u16`. So
no count is negative. scsynth sizes the reply of `/b_getn` by its count
before it checks the run. A negative count makes it write outside its
memory (see the traps of scsynth-osc). A value over `i32::MAX` goes as
`i32::MAX`.

The samples of `get_samples` come in one reply, whose size scsynth limits
(see [Traps](#traps)). Past that limit, `get_samples` gives
`Error::NoReply` over UDP, and the `Error::Failed` of
`/fail /b_getn "buffer overflow"` over TCP.

scsynth 3.14.1 runs the completion of `/b_allocRead` once it has read the
file, then answers `/done`. So the `/b_info` of the buffer comes before
the `/synced`. For a file that it cannot open, scsynth 3.14.1 answers
`/fail /b_allocRead "File '...' could not be opened: ...\n"` with the
buffer as its value, and runs no completion. `read_buffer` then gives
`Error::Failed`. Without a `/b_info` of the buffer, `read_buffer` gives
`Error::NoReply`. `query_buffer` and `get_samples` take the reply of their
buffer among the replies, and give `Error::NoReply` without it. The path
of a sound file is a path on the host of scsynth, which reads the file.

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

## The ids and the options

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

The defaults of the five counts are those of scsynth. scsynth tells no
client its counts: no reply gives them. So they must match the flags of
the server. `max_logins` is the limit that scsynth reports, which can
differ from its `-l` (see [Traps](#traps)). sclang 3.14.1 boots scsynth
with `-i 2 -o 2 -l 1` by default (`Server.sc` of its class library). So a
client that shares such a server sets `inputs` and `outputs` to 2, and
gets `too many users` at `register` until sclang takes more logins.

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
`Error::Exhausted`, and no id passes the count. The reserved counts of
sclang are 0 by default, and this client has none. A free gives a run
back, and joins it with the free runs next to it. sclang picks among its
free runs in another order, so only the ranges are shared.

The login limit is the one of the `Registration`: the limit in the reply
of `/notify`, else `Options::max_logins`.

sclang and this crate split the node ids among 32 clients, 0 to 31. A
client id under 0 or over 31 has no range of node ids, and `register`
refuses it with `Error::NoNodeRange` (see [The session](#the-session)).
scsynth can give such an id (see [Traps](#traps)).

Each registration starts fresh ranges, and `unregister` forgets them. The
methods that take an id from the ranges of the client need `register`
first: before it, they give `Error::NotRegistered`. When no free run that
fits is left in a range, they give `Error::Exhausted`, with the `IdKind`
of the range.

## Errors

| Error | Meaning |
| --- | --- |
| `Error::Failed` | scsynth answered `/fail` for a command of the request: the command, the message and the value of the `/fail` |
| `Error::Timeout` | the answer did not come within the limit |
| `Error::Send` | the transport refused the packet, or it closed: the `SendError` |
| `Error::NoReply` | scsynth answered without the reply, or the value of a reply, that the method reads |
| `Error::NotRegistered` | the method needs `register` first |
| `Error::NoNodeRange` | the client id that scsynth gave has no range of node ids: it is under 0 or over 31 |
| `Error::Exhausted` | no free id is left in the range of the client, for the `IdKind` that it holds |
| `Error::OutOfRange` | the id is at or over the count of its kind in the `Options`: the kind and the id |
| `Error::Read` | the bytes are no synth definition file: the `ReadError` of scsynth-synthdef |
| `Error::Limit` | the synth definition breaks a limit of scsynth or of the format: the `LimitError` of scsynth-synthdef |

A close of the transport settles every pending call with
`Error::Send(SendError::Closed)`, and each awaited call and each send
after it gets the same error.

## The transports

### The trait

`OscClient` is a connection to scsynth: a transport. It sends the packets
of commands of scsynth-osc, `Packet<Command>`, and gives what arrives to
one listener. Three rules hold for every transport:

- `send` queues one packet and returns. It waits for no network and no
  reply. It refuses a packet over the limit of the transport with
  `SendError::TooLarge`, and any packet after the close with
  `SendError::Closed`.
- The listener gets the events in the order of their arrival, one call
  at a time. `listen` sets the one listener, and a second call replaces
  the first. The transport keeps what arrives before `listen`.
- A write that fails after `send` returned comes to the listener as an
  event.

`close` closes the connection. The listener then gets `Event::Closed` once.

On a native target, an `OscClient` is `Send` and `Sync`, and a `Listener`
is `Send`. On `wasm32` they need not be, because a value of JavaScript is
not `Send`. The traits `MaybeSend` and `MaybeSync` state this rule.

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

### The channel pair

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
gives `SendError::Closed`, and the peer drops what it sends. A drop of
either end closes nothing.

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

### The transports of tokio

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

#### UDP

`UdpClient::connect(addr)` binds port 0 of the unspecified address of the
family of `addr`, and connects to `addr`. Each packet is one datagram,
each way. A datagram from scsynth comes as its messages, in order. Bytes
that are no OSC packet come as an `Event::Error` with
`ClientError::Decode`. The client receives into a buffer of 65,535 bytes.

`send` refuses a packet over the largest datagram with
`SendError::TooLarge`, and nothing leaves. The largest datagram is
`DEFAULT_MAX_DATAGRAM`, 8,192 bytes, which macOS sends (see
[Traps](#traps)), unless `UdpClient::connect_with_max_datagram` sets
another.

An I/O error of a send or of a receive comes as an `Event::Error` with
`ClientError::Transport`, and the client goes on. When no scsynth runs on
the port, the operating system can report a datagram to it as an error:
macOS reported "connection refused". On the Linux runner of CI, no error
came within 5 s. In both cases the client goes on, and scsynth can come
back on the same port.

#### TCP

`TcpClient::connect(addr)` connects a TCP stream to `addr`, with
`TCP_NODELAY` on. scsynth 3.14.1, started with `-t`, frames each packet
with its length: 32 bits in network byte order before the packet, each
way. The client hands the length and the packet to the stream as one
buffer. It reads a packet that arrives in any pieces, and one read can
hold several packets. scsynth 3.14.1 took a packet of 60,016 bytes over
TCP, and `send` sets no size limit of its own. It refuses only a packet
over `i32::MAX` bytes, with `SendError::TooLarge`: scsynth reads the
length as a signed integer (`SC_ComPort.cpp:368`).

A length that is negative or over 64 MiB comes as an `Event::Error`, and
then the client closes: after it, the stream cannot be read. scsynth sets
no such limit. The limit only guards against a stream that lost its
framing. An I/O error of a read or of a write also comes as an
`Event::Error`, and then the client closes. The end of the stream from
scsynth closes the client too.

scsynth takes `-u` and `-t` with the same port number: scsynth 3.14.1
answered over UDP and over TCP on one number.

#### The timer of tokio

`TokioTimer` is a `Timer` whose sleeps are those of `tokio::time::sleep`.
`Scsynth::new(client, options)` is
`Scsynth::with_timer(client, TokioTimer, options)`. The runtime that polls
the awaited methods needs the time driver of tokio, which `enable_time` or
`enable_all` turns on. `#[tokio::main]` and `#[tokio::test]` turn it on.

### The transports of the browser

With the feature `web`, on `wasm32`, the crate has two transports over
the objects of the browser: `WebSocketClient` and `PortClient`. They run
in a page and in a worker. No test runs them (see [Tests](#tests)).

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

#### WebSocket

`WebSocketClient::open(url)` makes a `WebSocket` of the browser, with the
binary type `arraybuffer`, and ends at its `open` event. If the browser
refuses the URL, or if an `error` or a `close` event comes before the
`open` event, it gives `ClientError::Transport`.

`open` sets no limit. A server that takes the connection and never
answers the handshake keeps it waiting. The browser takes a relative URL
against the address of the page or the worker: from a page of
`http://localhost:5199`, Chrome 158 opened `not a url` as
`ws://localhost:5199/not%20a%20url`. The dev server of Vite took that
connection and did not answer its handshake in 30 seconds. A caller that
needs a limit races `open` against a timer of its own.

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
scsynth. The repository has one in [`examples/bridge`](examples/bridge/README.md).
It pipes each websocket to its own TCP connection to scsynth, and refuses
the pages of the origins that it does not know. Its README gives its
rules, and how to run it.

#### Message ports

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

#### The timer of the browser

`WebTimer` is a `Timer` whose sleeps are `setTimeout` calls of the global
object, so it runs in a page and in a worker. With the feature `web`, on
`wasm32`, `Scsynth::new(client, options)` is
`Scsynth::with_timer(client, WebTimer, options)`. `setTimeout` reads its
delay as a signed 32-bit integer. So a sleep over `i32::MAX`
milliseconds, about 24.8 days, ends after `i32::MAX` milliseconds. Where
the global object has no `setTimeout`, each sleep ends at once.

## A transport of your own

A transport of the caller implements `OscClient`, and keeps the rules of
[The trait](#the-trait). `Scsynth` asks nothing more of it:

- `Scsynth` calls `send`, `listen` and `close` with no lock held. So a
  transport can call the listener from inside them, as `ChannelClient`
  does.
- A transport of bytes encodes each packet with `Packet::encode` of
  scsynth-osc. `decode(bytes)` gives the messages of the bytes of one
  packet that arrives, in order. It types each message that decodes as a
  `Reply`, and keeps each other message as `Incoming::Other`. So a bundle
  with one unknown message still gives its replies. Bytes that are no OSC
  packet give `ClientError::Decode`, which the transport gives to its
  listener as an `Event::Error`.
- A host that carries the packets itself, as typed values, takes the
  channel pair: `Scsynth` takes the `ChannelClient`, and the host answers
  through the `ChannelPeer`.

The transports of the crate are models: `src/udp.rs` and `src/tcp.rs` over
tokio, `src/web/` in the browser, and `examples/vite/wasm/src/object.rs`
over an object of JavaScript. A runtime other than tokio and the browser
also needs a `Timer` (see [The client](#the-client)).

## Examples

The repository has two examples, each a crate of its own with its own
lock. [`examples/bridge`](examples/bridge/README.md) is the bridge of
[WebSocket](#websocket). [`examples/vite`](examples/vite/README.md) is a
Vite page that runs `Scsynth` in the browser, compiled to wasm, and plays
a sine through the bridge: over a `WebSocketClient`, over a `PortClient`
to a worker, or over a transport of its own in TypeScript.

## Traps

scsynth 3.14.1 does these, and each one costs a caller time. The client
rules out what it can.

- A `/fail` carries the address of its command, its message and at most
  one number, and no id of a request. So when a command of the request in
  flight has the address of a `/fail`, the client gives the `/fail` to
  that request. With two
  requests in flight, it cannot tell whose `/fail` it is.
- The `/fail` of a second bundle can come before the `/synced` of the
  first: in 20 of 20 trials, it did. So the client sends a request only
  after the `/synced` of the request before it.
- `/d_recv` answers `/done /d_recv` also for bytes that are no synth
  definition. So only the client can refuse them, as `add_synthdef_bytes`
  does.
- A definition that needs more wire buffers than the `-w` of scsynth, 64
  by default, also gets `/done /d_recv`, and does not load. The client
  cannot see it. `SynthDef::wire_buffers` of scsynth-synthdef counts the
  wire buffers of a definition.
- scsynth sends no reply at all for a `/b_alloc` that it cannot allocate
  (`SC_SequencedCommand.cpp:292-302`): it answered a `/b_alloc` of 0
  frames with the `/synced` of the request alone. So `alloc_buffer` gives
  `Error::NoReply` without the `/done` of its buffer.
- scsynth takes a buffer id over its count: with 1,024 buffers, it
  answered `/b_alloc 99999` with `/done /b_alloc 99999`, and wrote buffer
  0. So `Options::buffers` must be the `-b` of the server, and the client
  refuses a buffer id at or over it with `Error::OutOfRange`.
- scsynth multiplies the frames by the channels of `/b_alloc` in an `int`
  (`SC_World.cpp:1048`). A product over `i32::MAX` wraps. Then scsynth
  fails to allocate, or allocates fewer samples than the frames and the
  channels that it keeps for the buffer. `alloc_buffer` does not check the
  product: keep it at `i32::MAX` or under.
- A connection that closes keeps its login: scsynth ends a login at
  `/notify 0` alone (`SC_SequencedCommand.cpp:1268-1273`). With a limit of
  4 logins, scsynth answered the fifth registration with "too many users",
  after four TCP connections that registered and closed. So `close`
  unregisters a registered client first, and a client that drops sends no
  `/notify 0`.
- The client ids come from a queue. If the id that `/notify` asks for is
  free, scsynth gives it. Else it gives the free id that waited longest: a
  freed id goes to the back (`SC_SequencedCommand.cpp:1271`). One
  connection that registered and unregistered four times got the ids 0,
  1, 2 and 3. The default limit is 64 logins. So after 32 registrations, a
  client without `Options::client_id` can get an id over 31, even as the
  only client, and `register` gives `Error::NoNodeRange`. Start scsynth
  with `-l 32`, or set `Options::client_id`.
- scsynth rounds its `-l` up to a power of two (`scsynth_main.cpp:241`):
  started with `-l 3`, it answered a limit of 4. The ranges of the ids
  follow the limit that scsynth reports, so `Options::max_logins` must be
  that limit, and not the `-l`: with 3, the ranges of two clients overlap.
- macOS sends a UDP datagram of 9,216 bytes at most
  (`sysctl net.inet.udp.maxdgram`): it refused one of 9,316 bytes and sent
  one of 9,016. So `UdpClient` refuses a packet over 8,192 bytes by
  default. A larger synth definition goes over TCP, or as a file with
  `load_synthdef`.
- One reply of `/b_getn` held at most 13,095 samples over UDP, and 13,098
  over TCP. scsynth builds a reply in 65,516 bytes (`big_scpacket` of
  `common/scsynthsend.h`), and a datagram over IPv4 holds 65,507 bytes at
  most. scsynth raises the send buffer of its UDP socket
  (`SC_ComPort.cpp:268-281`), so the limit of macOS does not hold for its
  replies. Over UDP, a larger reply does not leave scsynth, and over TCP,
  scsynth answers `/fail /b_getn "buffer overflow"`. So one call of
  `get_samples` reads that many samples at most.
- A client that is not registered hears no notification: no `/n_go`, no
  `/n_end` and no `/n_info`. Its `/synced` and its `/fail` still come. So
  its `query` gives `Error::NoReply`, and its `ended` gives
  `Error::NotRegistered`.
- scsynth sends `/n_go`, `/n_end`, `/n_on`, `/n_off`, `/n_move` and
  `/n_info` to every registered client, whichever client sent the
  command. So `query` takes the `/n_info` of its own node among the
  replies, and a subscription hears the nodes of the other clients.

## Limits

Version 0.1 does not do these. [docs/plan.md](docs/plan.md) says what each
one needs.

- One awaited request is in flight at a time, so each one costs a round
  trip (see [Requests](#requests)).
- The core of the requests is private: a host with a loop of its own
  cannot drive it.
- An awaited request leaves at once: a bundle with a time tag ahead goes
  through `send`, and nothing waits for it.
- `register` cannot ask scsynth for a free client id under 32.
- `WebSocketClient::open` sets no limit (see [WebSocket](#websocket)).
- `alloc_buffer` does not check the product of its frames and its
  channels.
- `TcpClient` sends no `-p` password.
- A closed client stays closed: a reconnect takes a new `Scsynth`.
- The client follows scsynth 3.14.1 alone: no test ran supernova.

## Tests

`cargo test --all-features` runs the unit tests, the tests of `tests/`
and the examples of this file. They play scsynth in memory or on the
loopback address, and need no SuperCollider.

| Tests | What they check |
| --- | --- |
| `src/core.rs` | the rules of the requests, on replies that they script, and the orders of the replies of scsynth 3.14.1 |
| `src/ids.rs` | the ranges of the ids, against the formulas of sclang |
| `tests/channel.rs` | the channel pair and `decode` |
| `tests/client.rs` | `Scsynth` on a channel pair, with no runtime and a timer that the test fires |
| `tests/nodes.rs`, `tests/resources.rs` | the packet that each method sends, and the value that it gives |
| `tests/udp.rs`, `tests/tcp.rs` | the transports of tokio, against sockets on the loopback address |

No test runs the transports of the browser. They build for
`wasm32-unknown-unknown`, and the page of
[`examples/vite`](examples/vite/README.md) ran `WebSocketClient` and
`PortClient::from_worker` by hand in Chrome 158.

`tests/scsynth.rs` checks the client against a real scsynth. Each test
proves a fact of scsynth 3.14.1 that this file states, or runs the client
against it. Two tests go through every method of the client, one over UDP
and one over TCP. These tests are ignored by default, and pass at once
without `SCSYNTH_ADDR`. Run them against a scsynth on the same host, with
the default counts of its flags, that no other client uses:

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
tests write WAV files in the temporary directory, and remove them. A test
that fails can leave its login in scsynth. Then a later run cannot get
the client id that it asks for: restart scsynth after a failed run.

## License

Licensed under either of the Apache License, Version 2.0
(`LICENSE-APACHE`) or the MIT license (`LICENSE-MIT`), at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this crate by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
