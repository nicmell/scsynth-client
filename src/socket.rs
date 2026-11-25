use std::collections::VecDeque;
use std::future::poll_fn;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender, WeakUnboundedSender};

use crate::client::{ClientError, Event, Listener, SendError, decode};

/// The bytes of the buffer of one read: the largest payload of a UDP
/// datagram fits in it.
const READ_BUFFER: usize = 65_535;

/// The half of a socket that the reader task of a client reads.
pub(crate) trait Reader: Send + 'static {
    /// Whether an I/O error of a read or of a write closes the client.
    const ERRORS_CLOSE: bool;

    /// Reads one datagram, or the next bytes of a stream, into `buffer`, and
    /// gives their length.
    fn poll_read(&mut self, cx: &mut Context<'_>, buffer: &mut [u8]) -> Poll<io::Result<usize>>;

    /// Adds the events of the `bytes` of one read to `events`, and gives
    /// whether the connection goes on. `bytes` is empty at the end of a
    /// stream.
    fn received(&mut self, bytes: &[u8], events: &mut Vec<Event>) -> bool;
}

/// The half of a socket that the writer task of a client writes.
pub(crate) trait Writer: Send + 'static {
    /// Writes the start of `bytes`, and gives how many bytes it wrote.
    fn poll_write(&mut self, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>>;
}

/// The reader task and the writer task of a client, from the side of the
/// client.
///
/// Only the reader task calls the listener. When the client drops, the
/// reader task closes the client as [`Self::close`] does.
pub(crate) struct Tasks {
    outbox: Arc<Outbox>,
    control: UnboundedSender<Control>,
}

/// The queue of the writer task: `None` once the client is closed.
struct Outbox(Mutex<Option<UnboundedSender<Vec<u8>>>>);

/// What the reader task gets besides the bytes of its socket.
enum Control {
    Listen(Listener),
    /// A write failed: the text of its error.
    Failed(String),
    Close,
}

/// What the reader task waits for.
enum Step {
    /// `None` once the client is dropped.
    Control(Option<Control>),
    Read(io::Result<usize>),
}

/// The listener of the reader task, and the events that wait for one.
#[derive(Default)]
struct Delivery {
    listener: Option<Listener>,
    kept: VecDeque<Event>,
}

impl Tasks {
    /// Spawns the reader task and the writer task of a socket on the
    /// runtime of tokio of this call.
    pub(crate) fn spawn(reader: impl Reader, writer: impl Writer) -> Self {
        let (queue, queued) = mpsc::unbounded_channel();
        let (control, controls) = mpsc::unbounded_channel();
        let outbox = Arc::new(Outbox(Mutex::new(Some(queue))));
        tokio::spawn(write(writer, queued, control.downgrade()));
        tokio::spawn(read(reader, controls, Arc::clone(&outbox)));
        Self { outbox, control }
    }

    /// Queues `bytes` for the writer task.
    pub(crate) fn send(&self, bytes: Vec<u8>) -> Result<(), SendError> {
        match &*self.outbox.lock() {
            Some(queue) => queue.send(bytes).map_err(|_| SendError::Closed),
            None => Err(SendError::Closed),
        }
    }

    /// Hands `listener` to the reader task. After the end of that task, the
    /// listener is dropped: [`Event::Closed`] came already.
    pub(crate) fn listen(&self, listener: Listener) {
        let _ = self.control.send(Control::Listen(listener));
    }

    /// Ends the queue of the writer task, which writes what waits in it and
    /// then ends, and makes the reader task close the client. A second call
    /// does nothing.
    pub(crate) fn close(&self) {
        if self.outbox.close() {
            let _ = self.control.send(Control::Close);
        }
    }
}

impl Outbox {
    fn lock(&self) -> MutexGuard<'_, Option<UnboundedSender<Vec<u8>>>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Ends the queue, and gives whether it was open.
    fn close(&self) -> bool {
        self.lock().take().is_some()
    }
}

impl Delivery {
    fn emit(&mut self, event: Event) {
        match &mut self.listener {
            Some(listener) => listener(event),
            None => self.kept.push_back(event),
        }
    }

    fn listen(&mut self, mut listener: Listener) {
        for event in self.kept.drain(..) {
            listener(event);
        }
        self.listener = Some(listener);
    }
}

/// The reader task: it reads the socket and the controls, and it alone calls
/// the listener.
async fn read<R: Reader>(reader: R, mut controls: UnboundedReceiver<Control>, outbox: Arc<Outbox>) {
    // `None` once the client is closed.
    let mut reader = Some(reader);
    let mut delivery = Delivery::default();
    let mut buffer = vec![0; READ_BUFFER];
    let mut events = Vec::new();
    loop {
        let step = poll_fn(|cx| {
            if let Poll::Ready(control) = controls.poll_recv(cx) {
                return Poll::Ready(Step::Control(control));
            }
            match &mut reader {
                Some(reader) => reader.poll_read(cx, &mut buffer).map(Step::Read),
                None => Poll::Pending,
            }
        })
        .await;
        let Some(open_reader) = &mut reader else {
            // Closed: the kept events end in `Event::Closed`.
            match step {
                Step::Control(Some(Control::Listen(listener))) => {
                    delivery.listen(listener);
                    return;
                }
                Step::Control(None) => return,
                _ => continue,
            }
        };
        let open = match step {
            Step::Control(Some(Control::Listen(listener))) => {
                delivery.listen(listener);
                true
            }
            Step::Control(Some(Control::Failed(error))) => {
                events.push(Event::Error(ClientError::Transport(error)));
                !R::ERRORS_CLOSE
            }
            Step::Control(Some(Control::Close) | None) => false,
            Step::Read(Ok(length)) => open_reader.received(&buffer[..length], &mut events),
            Step::Read(Err(error)) => {
                events.push(Event::Error(ClientError::Transport(error.to_string())));
                !R::ERRORS_CLOSE
            }
        };
        for event in events.drain(..) {
            delivery.emit(event);
        }
        if !open {
            outbox.close();
            reader = None;
            delivery.emit(Event::Closed);
            if delivery.listener.is_some() {
                return;
            }
        }
    }
}

/// The writer task: it writes the queued bytes in order, until the end of
/// the queue.
async fn write<W: Writer>(
    mut writer: W,
    mut queued: UnboundedReceiver<Vec<u8>>,
    control: WeakUnboundedSender<Control>,
) {
    while let Some(bytes) = queued.recv().await {
        if let Err(error) = write_all(&mut writer, &bytes).await {
            if let Some(control) = control.upgrade() {
                let _ = control.send(Control::Failed(error.to_string()));
            }
        }
    }
}

async fn write_all<W: Writer>(writer: &mut W, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        let written = poll_fn(|cx| writer.poll_write(cx, bytes)).await?;
        if written == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        bytes = &bytes[written..];
    }
    Ok(())
}

/// Adds the messages of the bytes of one OSC packet to `events`, or one
/// [`Event::Error`] when they do not decode.
pub(crate) fn push_messages(bytes: &[u8], events: &mut Vec<Event>) {
    match decode(bytes) {
        Ok(messages) => events.extend(messages.into_iter().map(Event::Message)),
        Err(error) => events.push(Event::Error(error)),
    }
}
