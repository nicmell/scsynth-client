# The bridge

A program that lets a page reach scsynth. scsynth listens on UDP and TCP
only, and a page can open neither. The bridge accepts websockets, and pipes
each one to its own TCP connection to scsynth. A `WebSocketClient` of
scsynth-client, in a page, reaches scsynth through it.

The bridge is a crate of its own, with its own lock. It is no member of
the crate scsynth-client, which it takes by path.

## Running it

Start scsynth with a TCP port, `-t`:

```sh
scsynth -u 57110 -t 57110 -l 32
```

scsynth 3.14.1 takes `-u` and `-t` with the same port number, and answers
over UDP and over TCP on it. `-l 32` sets the login limit to 32. scsynth
takes the client ids from a queue, and puts a freed id at its back. The
default limit is 64 logins. On such a server, the 33rd registration gets
the client id 32, even as the only client. The client id 32 has no range
of node ids: see [The ids](../../README.md#the-ids). With `-l 32`, each
client id is under 32.

Then start the bridge, from the root of the repository:

```sh
cargo run --manifest-path examples/bridge/Cargo.toml
```

A page of the dev server of Vite, on `http://localhost:5173`, can then open
`ws://127.0.0.1:57120`.

## The arguments

| Argument | Meaning | Default |
| --- | --- | --- |
| `--listen <addr>` | the address of the websockets | `127.0.0.1:57120` |
| `--scsynth <addr>` | the TCP address of scsynth, its `-t` | `127.0.0.1:57110` |
| `--origin <origin>` | an origin whose pages can connect | `http://localhost:5173`, the dev server of Vite |
| `--help` | prints the usage | |

An address is an IP address and a port: `localhost:57110` is no address.
Each `--origin` adds one origin, and the first one replaces the default.
Any other argument prints the usage, and the bridge exits with a failure.

## The rules

1. **One TCP connection for each websocket.** At the accept of a
   websocket, the bridge opens its TCP connection. scsynth 3.14.1 sends
   `/fail`, `/done` and `/synced` to the sender alone. With one shared
   connection, each page gets the replies of the other pages, and all the
   pages share one client id. `TCP_NODELAY` is on,
   on the connection to scsynth and on the websocket, so that each packet
   leaves at once: without it, a small write can wait for the
   acknowledgment of the one before.
2. **One binary frame is one OSC packet.** Towards scsynth, the bridge
   writes the length of the packet, 32 bits in network byte order, then the
   packet, in one write: the framing of scsynth with `-t`. From scsynth, it
   reads such packets, in any pieces and several in one read, and sends
   each one as one binary frame. It decodes no OSC. A frame from the page
   holds at most 16 MiB, the default limit of tungstenite: a larger one
   ends the pair.
3. **Another origin gets HTTP 403.** A websocket has no same-origin rule:
   any page that the user opens can connect to `ws://127.0.0.1:57120`.
   Through the bridge, that page can send `/quit`, or `/b_write`, which
   writes a file on the host of scsynth. So a handshake whose `Origin`
   header is not in the list gets 403, and no TCP connection. The origin
   must match exactly, as the browser sends it: the scheme, the host and
   the port, with no path. A page on `http://127.0.0.1:5173` sends that
   origin, and not `http://localhost:5173`: add it with `--origin`. A
   handshake with no `Origin` header passes: a browser always sends one,
   and a program that is not a browser is no cross-site risk.
4. **`/notify 0` at the close.** Whatever ends the pair, the bridge writes
   one `/notify 0` on its TCP connection, then closes it. A TCP connection
   that closed without `/notify 0` kept its login in scsynth 3.14.1: with a
   limit of 4 logins, the fifth registration answered "too many users".
   The bridge cannot know whether the page registered, so it always sends
   `/notify 0`. For a connection that is not registered, scsynth answers
   `/fail`, which nobody reads. After `/notify 0`, the bridge ends its
   stream to scsynth, and reads until scsynth closes its end, for one
   second at most: a socket that closes with bytes unread sends a reset,
   and the reset can discard the `/notify 0`. A measurement through the
   bridge: scsynth 3.14.1, with `-l 32`, took 40 registrations in a row.
   Each page closed its websocket, and sent no `/notify 0` of its own.
   scsynth gave the client ids 0 to 31, then 0 to 7.
5. **The end of scsynth closes the websocket.** If scsynth closes the
   TCP connection, the bridge closes the websocket with the code 1001
   (going away) and the reason "scsynth closed the connection". If the
   connect to scsynth fails, the code is 1011 (internal error), and the
   reason names the address and the error. An I/O error of the TCP
   connection also gives 1011. A reason holds 123 bytes at most, and the
   bridge cuts a longer one.
6. **A text frame is no OSC packet.** The bridge closes that websocket with
   the code 1003 (unsupported data).
7. **A bad length from scsynth closes the pair.** A length that is
   negative or over 64 MiB, the limit of the `TcpClient` of scsynth-client
   (see [TCP](../../README.md#tcp)), closes the websocket with the code
   1011: after it, the stream cannot be read. The packets before it still
   go to the page.
8. **The loopback address, and one log line for each event.** The bridge
   listens on `127.0.0.1` unless `--listen` says otherwise. The origin rule
   guards against pages, and not against the programs of other hosts. It
   logs one line to stderr for its start, for each accepted and each
   refused handshake, and for each close:

   ```text
   bridge: listening on ws://127.0.0.1:57120 for the origins http://localhost:5173, to scsynth at 127.0.0.1:57110 over TCP
   bridge: 127.0.0.1:54578 accepted, origin http://localhost:5173
   bridge: 127.0.0.1:54580 refused: the origin http://evil.example is not allowed
   bridge: 127.0.0.1:54578 closed: the page closed the websocket
   ```

## Tests

`cargo test --manifest-path examples/bridge/Cargo.toml` runs the tests of
the rules. A TCP listener plays scsynth, and a websocket client of
tokio-tungstenite plays the page.

`tests/scsynth.rs` sends a `/status` through the bridge to a real scsynth,
and waits for its `/status.reply`. It is ignored by default, and passes at
once without `SCSYNTH_ADDR`. Run it against a scsynth of its own:

```sh
scsynth -u 57341 -t 57341 -B 127.0.0.1 -l 32 &
SCSYNTH_ADDR=127.0.0.1:57341 cargo test --manifest-path examples/bridge/Cargo.toml -- --ignored
```
