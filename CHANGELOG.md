# Changelog

## 0.1.0 (2026-11-25)

The first release: a client of scsynth 3.14.1. Its awaited methods wait
for the replies of scsynth within a limit, one request in flight at a
time. It runs over UDP and TCP with tokio, and over a
websocket or a message port in the browser. It takes scsynth-osc 0.2.1
and scsynth-synthdef 0.15.0 by git tag, and re-exports both as
`scsynth_osc` and `scsynth_synthdef`.

### Added

- `Scsynth`: a client of scsynth over one transport, a handle whose clones
  share one connection. `with_timer`, `timeout`, `send`, `request`,
  `sync`, `status`, `version`, `quit`, `register`, `unregister`, `close`
  and `subscribe`. See [The client](README.md#the-client).
- `Registration`: the client id and the login limit that `register` gives.
- `Subscription`: every event of the transport, after the client used it.
  See [Subscriptions](README.md#subscriptions).
- `Timer` and `Sleep`: the sleeps that the awaited methods race against
  their limit.
- The node methods of `Scsynth`: `group`, `synth`, `free`, `run`,
  `free_all`, `deep_free`, `move_node`, `query`, `tree` and `get` send
  requests through the queue. `set` and `map` are plain sends of `/n_set`
  and `/n_map`. `ended` gives a future of the `/n_end` of a node, whose
  wait starts at its call. See [Nodes](README.md#nodes).
- `Target`: where a new node goes, or where a node moves: the head or the
  tail of a group, or just before or just after a node.
- The synth definition methods of `Scsynth`: `add_synthdef`,
  `add_synthdef_bytes`, `load_synthdef` and `free_synthdef`. See
  [Synth definitions](README.md#synth-definitions).
- The buffer methods of `Scsynth`: `alloc_buffer`, `read_buffer`,
  `free_buffer`, `query_buffer` and `get_samples`. See
  [Buffers](README.md#buffers).
- The bus methods of `Scsynth`: `control_bus`, `audio_bus`,
  `free_control_bus`, `free_audio_bus`, `set_bus` and `get_bus`. See
  [Buses](README.md#buses).
- `Options`: the limit of the awaited methods, the client id to ask for,
  the login limit of the server, and its counts of buffers, control
  buses, audio buses, inputs and outputs. See [Options](README.md#options).
- The ids of a registered client: its node ids, buffers, control buses
  and audio buses, in the ranges that sclang 3.14.1 gives to the same
  client id. `IdKind` names the kind of a range. See
  [The ids](README.md#the-ids).
- `Error`: why a call gives no result. See [Errors](README.md#errors).
- `OscClient`: a connection to scsynth that sends `Packet<Command>` and
  gives what arrives to one listener, with `Listener`, `Event`,
  `Incoming`, `SendError` and `ClientError`. `MaybeSend` and `MaybeSync`
  require `Send` and `Sync` on a native target alone. See
  [The trait](README.md#the-trait).
- `decode`: the messages of the bytes of one OSC packet, typed as replies
  where they decode. See
  [A transport of your own](README.md#a-transport-of-your-own).
- `ChannelClient` and `ChannelPeer`: a pair in memory that delivers in the
  same call stack. See [The channel pair](README.md#the-channel-pair).
- The feature `tokio`, on a native target: `UdpClient` with
  `DEFAULT_MAX_DATAGRAM`, `TcpClient`, `TokioTimer`, and `Scsynth::new`
  with that timer. See
  [The transports of tokio](README.md#the-transports-of-tokio).
- The feature `web`, on `wasm32`: `WebSocketClient`, `PortClient`,
  `WebTimer`, and `Scsynth::new` with that timer. See
  [The transports of the browser](README.md#the-transports-of-the-browser).
- The feature `serde`, for the types of scsynth-osc and scsynth-synthdef.
- The tests on a real scsynth, `tests/scsynth.rs`, which run by hand. See
  [Tests](README.md#tests).
- `examples/bridge`, a program that lets a page reach scsynth: it pipes
  each websocket to its own TCP connection to scsynth. See
  [its README](examples/bridge/README.md).
- `examples/vite`, a Vite page that runs `Scsynth` in the browser through
  the bridge, over a `WebSocketClient`, a `PortClient` to a worker, or a
  transport of its own in TypeScript. See
  [its README](examples/vite/README.md).

### Known issues

- See [Traps](README.md#traps) for what scsynth does that the client does
  not rule out, and [Limits](README.md#limits) for what version 0.1 does
  not do.
