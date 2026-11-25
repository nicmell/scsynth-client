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
  `/fail` of scsynth, `Timeout`, or `Send` with the `SendError` of the
  transport.
