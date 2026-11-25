import './style.css'
import init, {
  PortClient,
  Scsynth,
  WebSocketClient,
  sineDef,
  type Client,
  type OscClient,
  type OscEvent,
} from '../pkg/scsynth_client_wasm.js'
import type { FromWorker, ToWorker } from './worker.ts'

type Transport = 'websocket' | 'worker' | 'object'

/** The limit of each request, and of the open of a websocket, in ms. */
const LIMIT = 3000

/** The client of one connection, and what the page made with it. */
interface Session {
  kind: Transport
  scsynth: Scsynth
  /** The worker that owns the websocket, for the transports through one. */
  worker: Worker | null
  /** The id of the callback of `on("*")`. */
  listener: number
  group: number | null
  synth: number | null
}

/** A refusal that the log shows already. */
class Refused extends Error {}

/** An `OscClient` of the page: the worker owns the websocket, and the bytes
 * of each packet cross with `postMessage`, each way. */
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

await init()

const element = <T extends HTMLElement>(id: string) => document.querySelector<T>(`#${id}`)!
const url = element<HTMLInputElement>('url')
const transport = element<HTMLSelectElement>('transport')
const freq = element<HTMLInputElement>('freq')
const freqValue = element<HTMLOutputElement>('freq-value')
const state = element<HTMLElement>('state')
const log = element<HTMLOListElement>('log')
const button = (id: string) => element<HTMLButtonElement>(id)
const buttons = {
  connect: button('connect'),
  disconnect: button('disconnect'),
  play: button('play'),
  stop: button('stop'),
  missing: button('missing'),
}

let session: Session | null = null

function write(kind: 'request' | 'refusal' | 'message' | 'note', text: string) {
  const item = document.createElement('li')
  item.className = kind
  item.textContent = text
  log.append(item)
  item.scrollIntoView({ block: 'nearest' })
}

function text(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/** Runs `request`, and logs its result, or its refusal, with its time to
 * settle. A refusal throws `Refused`. */
async function timed<T>(
  label: string,
  request: () => Promise<T>,
  show: (value: T) => string = () => 'done',
): Promise<T> {
  const start = performance.now()
  const took = () => `${(performance.now() - start).toFixed(1)} ms`
  let value: T
  try {
    value = await request()
  } catch (error) {
    write('refusal', `${label} refused: ${text(error)} (${took()})`)
    throw new Refused(text(error))
  }
  write('request', `${label}: ${show(value)} (${took()})`)
  return value
}

function update() {
  buttons.connect.disabled = session !== null
  buttons.disconnect.disabled = session === null
  buttons.play.disabled = session === null || session.synth !== null
  buttons.stop.disabled = session?.synth == null
  buttons.missing.disabled = session === null
}

/** Logs what the worker says of its websocket. */
function watch(worker: Worker) {
  worker.addEventListener('message', (event: MessageEvent<ArrayBuffer | FromWorker>) => {
    const data = event.data
    if (data instanceof ArrayBuffer) return
    if (data.type === 'closed') write('note', `the websocket of the worker closed: ${data.code}`)
  })
}

const late = () => new Error(`the websocket did not open within ${LIMIT} ms`)

/** A worker of `worker.ts` whose websocket to `address` is open. */
function openWorker(address: string): Promise<Worker> {
  const worker = new Worker(new URL('./worker.ts', import.meta.url), { type: 'module' })
  return new Promise((resolve, reject) => {
    const fail = (error: Error) => {
      clearTimeout(timer)
      worker.onmessage = null
      worker.onerror = null
      worker.terminate()
      reject(error)
    }
    const timer = setTimeout(() => fail(late()), LIMIT)
    worker.onmessage = (event: MessageEvent<FromWorker>) => {
      if (event.data.type !== 'open') {
        fail(new Error(event.data.type === 'error' ? event.data.message : 'the websocket closed'))
        return
      }
      clearTimeout(timer)
      worker.onmessage = null
      worker.onerror = null
      watch(worker)
      resolve(worker)
    }
    worker.onerror = (event) => fail(new Error(event.message || 'the worker failed'))
    worker.postMessage({ type: 'open', url: address } satisfies ToWorker)
  })
}

/** A `WebSocketClient` of `address`, within the limit. A client that opens
 * after the limit is freed, which closes its socket. */
function openWebSocket(address: string): Promise<WebSocketClient> {
  const opening = WebSocketClient.open(address)
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(late())
      opening.then((client) => client.free(), () => {})
    }, LIMIT)
    opening.then(
      (client) => {
        clearTimeout(timer)
        resolve(client)
      },
      (error: unknown) => {
        clearTimeout(timer)
        reject(error)
      },
    )
  })
}

async function openClient(kind: Transport, address: string): Promise<[Client, Worker | null]> {
  switch (kind) {
    case 'websocket':
      return [await openWebSocket(address), null]
    case 'worker': {
      const worker = await openWorker(address)
      return [PortClient.fromWorker(worker), worker]
    }
    case 'object': {
      const worker = await openWorker(address)
      return [new WorkerClient(worker), worker]
    }
  }
}

async function connect() {
  const kind = transport.value as Transport
  const name = transport.selectedOptions[0].text
  const [client, worker] = await timed(
    `open ${name} ${url.value}`,
    () => openClient(kind, url.value),
    () => 'open',
  )
  const scsynth = new Scsynth(client, { timeoutMs: LIMIT })
  const listener = scsynth.on('*', (message) => {
    write('message', `← ${message.address} ${JSON.stringify(message.args ?? null)}`)
  })
  session = { kind, scsynth, worker, listener, group: null, synth: null }
  state.textContent = 'connected'
  update()
  const me = await timed(
    'register',
    () => scsynth.register(),
    (me) => `client ${me.clientId} of ${me.maxLogins}`,
  )
  state.textContent = `connected, client ${me.clientId}`
  await timed('addSynthdef sine', () => scsynth.addSynthdef(sineDef()))
}

async function play(current: Session) {
  if (current.group === null) {
    current.group = await timed(
      'group',
      () => current.scsynth.group({ tail: 0 }),
      (node) => `node ${node}`,
    )
  }
  const group = current.group
  current.synth = await timed(
    'synth sine',
    () => current.scsynth.synth('sine', { freq: Number(freq.value) }, { head: group }),
    (node) => `node ${node}`,
  )
  update()
}

async function stop(current: Session, synth: number) {
  await timed(`freeNode ${synth}`, () => current.scsynth.freeNode(synth))
  current.synth = null
  update()
}

async function missing(current: Session) {
  await timed(
    'synth missing',
    () => current.scsynth.synth('missing', {}, { tail: current.group ?? 0 }),
    (node) => `node ${node}`,
  )
}

async function disconnect(current: Session) {
  session = null
  update()
  try {
    const group = current.group
    if (group !== null) {
      await timed(`freeNode ${group}`, () => current.scsynth.freeNode(group)).catch(() => {})
    }
    await timed('close', () => current.scsynth.close())
  } finally {
    current.scsynth.off(current.listener)
    current.scsynth.free()
    // The own client posts its close itself.
    if (current.worker && current.kind === 'worker') {
      current.worker.postMessage({ type: 'close' } satisfies ToWorker)
    }
    state.textContent = 'disconnected'
  }
}

/** Runs `action` on a click. A refusal is in the log already: any other
 * error goes there too. */
function on(target: HTMLElement, action: () => Promise<void>) {
  target.addEventListener('click', () => {
    action().catch((error: unknown) => {
      if (!(error instanceof Refused)) write('refusal', `error: ${text(error)}`)
    })
  })
}

on(buttons.connect, connect)
on(buttons.play, async () => {
  if (session) await play(session)
})
on(buttons.stop, async () => {
  if (session?.synth != null) await stop(session, session.synth)
})
on(buttons.missing, async () => {
  if (session) await missing(session)
})
on(buttons.disconnect, async () => {
  if (session) await disconnect(session)
})

freq.addEventListener('input', () => {
  freqValue.value = freq.value
  if (session?.synth == null) return
  try {
    session.scsynth.set(session.synth, { freq: Number(freq.value) })
  } catch (error) {
    write('refusal', `set freq refused: ${text(error)}`)
  }
})
freq.addEventListener('change', () => {
  if (session?.synth != null) write('request', `set freq ${freq.value}: sent`)
})
update()
