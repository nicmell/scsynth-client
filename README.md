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

## The client

`Scsynth` is a client of scsynth over one transport. Its awaited methods
send requests, and wait for the answer of scsynth within a limit. It is a
handle: its clones share one connection. On a native target it is `Send`
and `Sync`, and the future of each of its methods is `Send`.

`Scsynth::with_timer(client, timer, options)` makes a client over the
transport `client`, and sets the listener of that transport. The `Timer`
makes the sleeps that the awaited methods race against their limit: its
`sleep(limit)` gives a `Sleep`, a future that ends at the end of the
limit. The client needs no runtime of its own: whatever polls its futures
and the sleeps of its timer runs it. The timers of the crate will come
with its features `tokio` and `web`, which hold nothing yet. Until then,
the caller writes its own timer.

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

A closed TCP connection keeps its login in scsynth. So `close`
unregisters a registered client first. Then it closes the transport in
every case. If `unregister` failed, `close` gives its error.

### Options

| Field | Meaning | Default |
| --- | --- | --- |
| `timeout` | the limit of each awaited method | 3 seconds |
| `client_id` | the client id that `register` asks for | `None`: scsynth picks one |
| `max_logins` | the login limit of the server, its `-l`, for a reply of `/notify` that does not give it | `None` |

A caller writes `Options { timeout, ..Options::default() }`.

### Errors

| Error | Meaning |
| --- | --- |
| `Error::Failed` | scsynth answered `/fail` for a command of the request: the command, the message and the value of the `/fail` |
| `Error::Timeout` | the answer did not come within the limit |
| `Error::Send` | the transport refused the packet, or it closed |
| `Error::NoReply` | scsynth answered without the reply, or the value of a reply, that the method reads |

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

## License

Licensed under either of the Apache License, Version 2.0
(`LICENSE-APACHE`) or the MIT license (`LICENSE-MIT`), at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this crate by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
