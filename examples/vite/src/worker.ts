/** What the page posts to the worker, besides the bytes of a packet. */
export type ToWorker = { type: 'open'; url: string } | { type: 'close' }

/** What the worker posts to the page, besides the bytes of a packet. */
export type FromWorker =
  | { type: 'open' }
  | { type: 'error'; message: string }
  | { type: 'closed'; code: number; reason: string }

/** The global scope of this worker. The program also holds the page, whose
 * `self` is a window. */
interface Scope {
  onmessage: ((event: MessageEvent) => void) | null
  postMessage(message: unknown, transfer?: Transferable[]): void
  close(): void
}

const scope = self as unknown as Scope
let socket: WebSocket | null = null

function post(message: FromWorker) {
  scope.postMessage(message)
}

/** Opens the websocket of the worker, which then carries each packet as
 * one binary frame, each way. */
function open(url: string) {
  try {
    socket = new WebSocket(url)
  } catch (error) {
    post({ type: 'error', message: error instanceof Error ? error.message : String(error) })
    scope.close()
    return
  }
  socket.binaryType = 'arraybuffer'
  let opened = false
  socket.onopen = () => {
    opened = true
    post({ type: 'open' })
  }
  socket.onerror = () => {
    if (!opened) post({ type: 'error', message: 'the websocket failed to open' })
  }
  socket.onclose = (event) => {
    if (opened) post({ type: 'closed', code: event.code, reason: event.reason })
    scope.close()
  }
  socket.onmessage = (event) => {
    if (event.data instanceof ArrayBuffer) scope.postMessage(event.data, [event.data])
  }
}

scope.onmessage = (event) => {
  const data: unknown = event.data
  if (data instanceof Uint8Array) {
    socket?.send(data as Uint8Array<ArrayBuffer>)
    return
  }
  const message = data as ToWorker
  if (message.type === 'open') {
    open(message.url)
  } else if (socket) {
    socket.close()
  } else {
    scope.close()
  }
}
