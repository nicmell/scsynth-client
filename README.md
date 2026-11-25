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

## License

Licensed under either of the Apache License, Version 2.0
(`LICENSE-APACHE`) or the MIT license (`LICENSE-MIT`), at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this crate by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
