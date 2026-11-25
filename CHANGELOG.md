# Changelog

## 0.1.0 (unreleased)

The first release, in development. It takes scsynth-osc 0.2.1 and
scsynth-synthdef 0.15.0 by git tag.

### Added

- `OscClient`: a connection to scsynth that sends `Packet<Command>` and
  gives what arrives to one listener, with `Listener`, `Event`,
  `Incoming`, `SendError` and `ClientError`. `MaybeSend` and `MaybeSync`
  require `Send` and `Sync` on a native target only. See
  [The transport](README.md#the-transport).
- `decode`: the messages of the bytes of one OSC packet, typed as replies
  where they decode. See [Bytes](README.md#bytes).
- `ChannelClient` and `ChannelPeer`: a pair in memory that delivers in the
  same call stack. See [The channel pair](README.md#the-channel-pair).
- `Error`: why a request to scsynth gives no result: `Failed` with the
  `/fail` of scsynth, `Timeout`, `Send` with the `SendError` of the
  transport, `NoReply`: scsynth answered without the reply that the
  method reads, `NotRegistered`: the method needs `register` first,
  `NoNodeRange`: the client id has no range of node ids, `Exhausted`: no
  free id is left in the range of the client, `OutOfRange`: the id is at
  or over the count of the server, `Read` with the `ReadError` of
  scsynth-synthdef: the bytes are no synth definition file, or `Limit`
  with its `LimitError`: the definition breaks a limit of scsynth.
- `Scsynth`: a client of scsynth over one transport, whose awaited methods
  wait for the answer of scsynth within a limit, one request in flight at
  a time. `with_timer`, `timeout`, `send`, `request`, `sync`, `status`,
  `version`, `quit`, `register`, `unregister`, `close` and `subscribe`.
  See [The client](README.md#the-client).
- `Registration`: the client id and the login limit that `register` gives.
- `Subscription`: every event of the transport, after the client used it.
  See [Subscriptions](README.md#subscriptions).
- `Options`: the limit of the awaited methods, the client id to ask for,
  the login limit of the server, and its counts of buffers, control
  buses, audio buses, inputs and outputs. See [Options](README.md#options).
- The ids of a registered client: its node ids, buffers, control buses
  and audio buses, in the ranges that sclang gives to the same client id.
  A range that ends over the count of the server is empty.
  `register` refuses a client id under 0 or over 31. `IdKind` names the
  kind of a range. See [The ids](README.md#the-ids).
- `Timer` and `Sleep`: the clock that the awaited methods race against
  their limit.
- The node methods of `Scsynth`. `group`, `synth`, `free`, `run`,
  `free_all`, `deep_free`, `move_node`, `query`, `tree` and `get` send
  requests through the queue. `group` and `synth` take the next node id
  of the client. `set` and `map` are plain sends of `/n_set` and
  `/n_map`. `ended` gives a future of the `/n_end` of a node, whose wait
  starts at its call. See [Nodes](README.md#nodes).
- `Target`: where a new node goes, or where a node moves: the head or the
  tail of a group, or just before or just after a node.
- The synth definition methods of `Scsynth`. `add_synthdef`,
  `add_synthdef_bytes`, `load_synthdef` and `free_synthdef` send requests
  of `/d_recv`, `/d_load` and `/d_free` through the queue. The methods
  of `/d_recv` and `/d_load` need their `/done`. `add_synthdef_bytes` sends
  nothing for bytes that are no definition file. See [Synth definitions](README.md#synth-definitions).
- The buffer methods of `Scsynth`. `alloc_buffer`, `read_buffer`,
  `free_buffer`, `query_buffer` and `get_samples` send requests through
  the queue. `alloc_buffer` and `read_buffer` take the next free buffer id
  of the client. When the call fails, the id goes back. `alloc_buffer` and
  `free_buffer` need the `/done` of their buffer. `read_buffer` sends one
  `/b_allocRead` with a `/b_query` as its completion, and needs its
  `/b_info`. A buffer id at or over `Options::buffers` is refused. The
  counts are unsigned, and a value over `i32::MAX` goes as `i32::MAX`. See [Buffers](README.md#buffers).
- The bus methods of `Scsynth`. `control_bus` and `audio_bus` give runs of
  bus indices of the client, and `free_control_bus` and `free_audio_bus`
  give them back, with no message to scsynth. `set_bus` is a plain send of
  `/c_set`, and `get_bus` a request of `/c_get`. See
  [Buses](README.md#buses).
