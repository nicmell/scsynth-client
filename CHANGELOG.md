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
  `NoNodeRange`: the client id has no range of node ids, or `Exhausted`:
  no free id is left in the range of the client.
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
  `register` refuses a client id under 0 or over 31. `IdKind` names the
  kind of a range. See [The ids](README.md#the-ids).
- `Timer` and `Sleep`: the clock that the awaited methods race against
  their limit.
