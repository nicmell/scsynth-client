# scsynth-client in a page

A Vite page that runs the `Scsynth` client of scsynth-client in the
browser, compiled to wasm. It reaches a scsynth through the websocket
[bridge](../bridge/README.md) of the repository, and plays one sine:

- Connect opens the transport, registers the client, and sends the synth
  definition `sine`.
- Play makes a group once, then a synth of `sine` in it. The slider sets
  its `freq`.
- Stop frees the synth. Missing asks for a synth of a definition that
  scsynth does not have, and shows its refusal: `SynthDef not found`.
- Disconnect frees the group, unregisters the client, and closes the
  transport.

The log shows each request with its time to settle, each refusal with its
text, and each message that scsynth sends, in its serde form.

A menu picks one of three transports:

| Transport | What carries the packets |
| --- | --- |
| websocket | the `WebSocketClient` of the module, in the page |
| worker port | a worker of `src/worker.ts` that owns the websocket, through the `PortClient` of the module, `PortClient.fromWorker(worker)` |
| own client | the same worker, through an `OscClient` object of the page, written in TypeScript: see [A client of your own](#a-client-of-your-own) |

In all three, the `Scsynth` runs in the page, with the limits of its
requests on `setTimeout`. The worker is plain TypeScript, with no wasm. It
relays the bytes of each packet each way with `postMessage`.

The synth plays to the first two outputs of scsynth. For a run that must
make no sound, start scsynth with no connection to the outputs of the
host.

## Run it

You need Rust with the target `wasm32-unknown-unknown`, wasm-pack, Node
22.12 or newer, and a scsynth. Start scsynth with a TCP port, then the
bridge, from the root of the repository:

```sh
scsynth -u 57110 -t 57110 -l 32
cargo run --manifest-path examples/bridge/Cargo.toml
```

The bridge README says why `-l 32`. Then, in this folder:

```sh
npm install
npm run wasm   # wasm-pack builds wasm/ into pkg/
npm run dev    # the page, on http://localhost:5173
```

Open `http://localhost:5173`, which is the origin that the bridge takes by
default, and press Connect. On another port, give the bridge its origin
with `--origin`.

## The JS API

`wasm/` is a crate of its own, `scsynth-client-wasm`, which exports these
classes and one function with wasm-bindgen:

```ts
import init, { Scsynth, WebSocketClient, sineDef } from '../pkg/scsynth_client_wasm.js'

await init()
const client = await WebSocketClient.open('ws://127.0.0.1:57120')
const scsynth = new Scsynth(client, { timeoutMs: 3000 })
const me = await scsynth.register() // { clientId, maxLogins }
await scsynth.addSynthdef(sineDef())
const group = await scsynth.group({ tail: 0 })
const synth = await scsynth.synth('sine', { freq: 440 }, { head: group })
scsynth.set(synth, { freq: 660 })
const id = scsynth.on('/n_end', (message) => console.log(message))
await scsynth.freeNode(synth)
scsynth.off(id)
await scsynth.close()
```

| Member | Does |
| --- | --- |
| `WebSocketClient.open(url)` | a transport over a new `WebSocket`, once it opens |
| `PortClient.fromPort(port)`, `PortClient.fromWorker(worker)` | a transport over a `MessagePort` or a `Worker` |
| `new Scsynth(client, options?)` | a client over a `WebSocketClient`, a `PortClient` or an `OscClient` object |
| `register()`, `unregister()` | `/notify 1` and `/notify 0` |
| `close()` | `/notify 0` for a registered client, then the close of the transport |
| `sync()`, `status()` | `/sync`, and the serde form of the `/status.reply` |
| `addSynthdef(bytes)` | `/d_recv` of the bytes of a synth definition file |
| `group(target)`, `synth(name, controls, target)` | `/g_new` and `/s_new`, with the next node id of the client, which they give |
| `set(node, controls)` | `/n_set`, a plain send that waits for nothing |
| `freeNode(node)` | `/n_free` |
| `on(address, callback)`, `off(id)` | each message whose address is `address`, or every message for `"*"`, to `callback` |
| `sineDef()` | the bytes of the definition `sine`, built by scsynth-synthdef: `freq` 440 and `amp` 0.1 to the outputs 0 and 1 |

Each method that waits for scsynth gives a promise. A refusal is a JS
`Error` with the text of the error of scsynth-client, such as
`scsynth failed /s_new: SynthDef not found`, or
`scsynth did not answer within the limit`. The text of each cause of the
error follows, after a colon:
`the transport did not send the packet: the client is closed`. The
[README of the crate](../../README.md#the-client) gives the rules of the
requests.

- A target is `{ head: node }`, `{ tail: node }`, `{ before: node }` or
  `{ after: node }`, and the root group is node 0. The controls are an
  object of numbers, such as `{ freq: 440 }`.
- `Options` holds the fields of the `Options` of the crate, in camel case,
  with the limit as `timeoutMs`.
- `new Scsynth` takes the `WebSocketClient` or the `PortClient` that it
  gets: that JS object is empty after the call.
- A message of `on` is the serde form of scsynth-osc: `{ address, args }`,
  such as `{ address: "/n_end", args: { node: 1001, ... } }`. A message
  that scsynth-osc does not type, such as `/d_removed`, comes as its
  `RawMessage`.
- The method of `/n_free` is `freeNode`, and not `free`: wasm-bindgen
  gives each class a `free()` that releases its memory in wasm, and
  `[Symbol.dispose]` calls it.
- The TypeScript types of `OscClient`, `OscEvent`, `Options`, `Target`,
  `Controls`, `Registration` and `OscMessage` come with `pkg/`, so `tsc`
  checks the page against them.

`WebSocketClient.open` sets no limit (see
[WebSocket](../../README.md#websocket) in the README of the crate). So the
page gives up on an open after its limit of 3 seconds, and frees a client
that opens later.

## A client of your own

`new Scsynth` takes any object with three methods:

```ts
type OscEvent =
  | { kind: 'message'; packet: Uint8Array }
  | { kind: 'error'; message: string }
  | { kind: 'closed' }

interface OscClient {
  send(packet: Uint8Array): void
  listen(listener: (event: OscEvent) => void): void
  close(): void
}
```

- `send` gets the bytes of one OSC packet, in a buffer of their own, which
  it can transfer. A throw comes to the client as an error event.
- `listen` gets the one listener at the start of the client. Each packet
  from scsynth goes to it as a `message` event: the client decodes its
  bytes.
- `close` closes the connection. The client takes it as closed at once,
  and drops what the object gives after it. When the other end closes,
  the object gives a `closed` event, which closes the client.

The own client of the page carries the bytes to the worker, which owns
the websocket:

```ts
class WorkerClient implements OscClient {
  readonly #worker: Worker
  readonly #kept: OscEvent[] = []
  #listener: ((event: OscEvent) => void) | null = null

  constructor(worker: Worker) {
    this.#worker = worker
    worker.onmessage = (event: MessageEvent<ArrayBuffer | FromWorker>) => {
      const data = event.data
      if (data instanceof ArrayBuffer) {
        this.#emit({ kind: 'message', packet: new Uint8Array(data) })
      } else if (data.type === 'error') {
        this.#emit({ kind: 'error', message: data.message })
      } else if (data.type === 'closed') {
        this.#emit({ kind: 'closed' })
      }
    }
  }

  send(packet: Uint8Array) {
    this.#worker.postMessage(packet, [packet.buffer as ArrayBuffer])
  }

  listen(listener: (event: OscEvent) => void) {
    this.#listener = listener
    for (const event of this.#kept.splice(0)) listener(event)
  }

  close() {
    this.#worker.postMessage({ type: 'close' } satisfies ToWorker)
  }

  #emit(event: OscEvent) {
    if (this.#listener) this.#listener(event)
    else this.#kept.push(event)
  }
}
```

## How it works

- `wasm-pack build --target web` makes an ES module that loads the
  `.wasm` through `new URL(…, import.meta.url)`, so Vite needs no plugin.
- `new Scsynth` takes its client as a dynamic union of wasm-bindgen, the
  enum `Client` of `wasm/src/lib.rs`. The union tries the classes
  `WebSocketClient` and `PortClient` first, and takes any other value as
  an `OscClient` object.
- `wasm/src/object.rs` holds the `OscClient` of the crate over an object
  of the caller.
- `src/main.ts` holds the page and its own client, and `src/worker.ts` the
  worker.
