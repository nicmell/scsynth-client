# The plan of scsynth-client

This file keeps only what is open after version 0.1. Each item says what
is known, and what a decision or a measurement must settle. The README,
the tests and the doc comments hold what version 0.1 does.

## The requests

### Several requests in flight

Known: the client sends a request only after the `/synced` of the request
before it. So each awaited request costs a round trip: a median of 7.8 ms
over TCP and 8.0 ms over UDP, on the loopback address. A `/fail` carries
no id of a request. The `/fail` of a second bundle came before the
`/synced` of the first in 20 of 20 trials (`tests/scsynth.rs`).

A way: send each request at once, and settle it at its evidence, the
`/n_go` of its node or the `/done` of its command. A `/fail` then goes to
the oldest request in flight that holds its address and still lacks its
evidence. This is exact only for the commands with a positive reply:
`/s_new` and `/g_new` of a registered client, and the asynchronous
commands with their `/done`. A command with no reply, such as `/n_set` or
`/d_free`, still needs a `/synced`.

To settle:

- Which commands share the flight.
- The order of `/n_go`, `/done`, `/fail` and `/synced` across bundles in
  flight together, over UDP and over TCP: a measurement.
- Whether the round trip that it saves matters. One request already
  takes many commands.

### A public core

Known: the core of the requests, `src/core.rs`, is a state machine with
no I/O and no clock. A request, a reply, a timeout, a refused send and a
close each give three kinds of effects: the packets to send, the calls to
settle and the messages to publish. The core is private in 0.1, and
`Scsynth` drives it under a lock.

To settle:

- Its public API and what it promises, such as its request ids and its
  effects.
- Whether the ids of a registration, `src/ids.rs`, go public with it.
- Whether `Scsynth` keeps its shape on top of it.

### An awaited request with a time tag ahead

Known: an awaited request leaves as an immediate bundle. A bundle with a
time tag ahead goes through `send`, and nothing waits for it. scsynth
3.14.1 answered the `/sync` of a bundle 500 ms ahead at its time
(`tests/scsynth.rs`). In the queue of 0.1, such a request holds every
request behind it until its time.

To settle:

- Whether it waits outside the queue.
- Whether its limit counts from its time tag or from the call.
- When the `/fail` of a scheduled command comes: a measurement.

## The ids

### A free client id under 32

Known: if the client id that `/notify` asks for is free, scsynth gives it.
Else it gives the free id that waited longest, and a freed id goes to the
back of the queue (`SC_SequencedCommand.cpp:1219-1271`). On a server with
more than 32 logins, a client can get an id over 31, which has no range of
node ids. `register` then sends `/notify 0`, and gives
`Error::NoNodeRange`. The advice of 0.1 is `scsynth -l 32`, or
`Options::client_id`.

A way: on an id over 31, register again with each id under 32 in turn,
until scsynth gives one. scsynth answers a taken id with the front of its
queue, so each try costs a `/notify 1` and a `/notify 0`.

To settle:

- Whether up to 32 tries of two round trips each are worth it.
- The race with the clients that register meanwhile.

## The transports

### A time limit for `WebSocketClient::open`

Known: `open` ends at the `open`, the `error` or the `close` event of the
socket. A server that takes the connection and never answers the
handshake leaves it pending: the dev server of Vite answered no handshake
in 30 seconds, measured with Chrome 158. The Vite page races `open`
against a `setTimeout` of its own, and frees a client that opens later.

To settle:

- Whether `open` takes a limit, or a second constructor does.
- That the socket closes at the end of the limit.

### The `-p` password of scsynth

Known: started with `-p`, scsynth reads the first packets of a TCP
connection as the password. It allows 4 tries. Each try is a length of 32
bits and at most 32 bytes, which scsynth compares with `strcmp`. It
sleeps 1 second after each try, the right one too
(`SC_ComPort.cpp:325-357`). UDP takes no password, and `TcpClient` sends
none.

To settle:

- The bytes of the password, and the end of its string.
- The API, such as a constructor of `TcpClient`.
- The wait: a measurement.

### A reconnect after a close

Known: after the close of its transport, a client settles every pending
call with `Error::Send(SendError::Closed)`, and refuses each awaited call
after it. scsynth keeps the login of a connection that closed. A caller
makes a new `Scsynth`.

To settle:

- Whether a client takes a new transport, and then registers again.
- Whether it then takes fresh ids, or the ids of its client id.
- What happens to the login of the old connection.

## The buffers

### The product of frames and channels

Known: scsynth 3.14.1 multiplies the frames by the channels of `/b_alloc`
in an `int` (`SC_World.cpp:1048`), which wraps over `i32::MAX`.
`alloc_buffer` takes both as `u32`, saturates each at `i32::MAX`, and
does not check their product.

To settle:

- What scsynth 3.14.1 does for a product that wraps to a negative number,
  and to a positive one: a measurement.
- Whether `alloc_buffer` refuses such a product with an error of its own.
  `Error` is `#[non_exhaustive]`, so a new variant breaks no caller.

## Other servers

Known: every fact of 0.1 comes from scsynth 3.14.1, from its source or
from a measurement. supernova, the other server of SuperCollider, differs
from scsynth in places (the README of scsynth-osc). No test ran it.

To settle: the facts of the README against supernova, and against other
versions of scsynth. They include the orders of the replies, `/notify`
and its client ids, and the failures.

## What no test runs

- `PortClient` over a `MessagePort` and over a worker scope. The Vite page
  runs `PortClient::from_worker` alone.
- The browser code in CI. CI runs clippy for `wasm32-unknown-unknown` and
  builds the page, but runs no browser. To settle: a browser test, its
  runner, and a scsynth or a peer that plays it.
- The MSRV of the browser code. The CI job `msrv` runs Rust 1.85 on a
  native target, where the feature `web` is empty. The 1.85 toolchain of
  the owner has no `wasm32` target. wasm-bindgen, js-sys and web-sys
  declare Rust 1.81.
- A real scsynth in CI. `tests/scsynth.rs` runs by hand.
- The error of a datagram to a closed UDP port on Linux: `tests/udp.rs`
  checks it on macOS alone (see [UDP](../README.md#udp)).
- The order of the replies on a network that reorders datagrams. UDP ran
  on the loopback address alone. A `/fail` that comes after the `/synced`
  of its request reaches no request, or the next one with the same
  address. TCP keeps the order.
- Whether a long read of a file holds the `/synced` of other requests. The
  command reference says that `/sync` answers after the asynchronous
  commands before it. So a `read_buffer` of a large file, from any client,
  can hold the `/synced` of a request past its limit.

## sc-app3

sc-app3 does not take this crate. Its pump talks to scsynth over UDP, from
a loop of its own. Its session is a state machine with no I/O, which many
pages share. To take this crate, the pump needs the public core first, so
that its own loop drives the requests. It also needs a decision on which
side owns the node ids, the groups and the buffers of a session. As a
submodule, the crate takes scsynth-osc and scsynth-synthdef through the
`[patch]` of sc-app3, which points both at the submodules of sc-app3. The
version ranges of this manifest must hold their versions.
