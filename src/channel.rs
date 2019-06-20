use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use futures::task;

use futures::task::Task;
use futures::{Async, Future, Poll, Stream};

use crate::error::Error;

/// Common context for Flush future and FlushResolver.
struct FlushContext {
    result: Option<Result<(), Error>>,
    task: Option<Task>,
}

impl FlushContext {
    /// Create a new FlushContext.
    fn new() -> FlushContext {
        FlushContext {
            result: None,
            task: None,
        }
    }
}

/// Flush future resolver.
struct FlushResolver {
    context: Arc<Mutex<FlushContext>>,
}

impl FlushResolver {
    /// Create a new FlushResolver with a given shared context.
    fn new(context: Arc<Mutex<FlushContext>>) -> FlushResolver {
        FlushResolver { context }
    }

    /// Resolve the future with a given result.
    fn resolve(&mut self, res: Result<(), Error>) {
        let mut context = self.context.lock().unwrap();

        context.result = Some(res);

        if let Some(task) = context.task.take() {
            task.notify();
        }
    }
}

/// Flush future.
pub struct Flush {
    context: Arc<Mutex<FlushContext>>,
}

impl Flush {
    /// Create a new Flush future with a given context.
    fn new(context: Arc<Mutex<FlushContext>>) -> Flush {
        Flush { context }
    }
}

impl Future for Flush {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        let mut context = self.context.lock().unwrap();

        if let Some(res) = context.result.clone() {
            match res {
                Ok(()) => Ok(Async::Ready(())),
                Err(err) => Err(err),
            }
        } else {
            // let's wait until the result is available
            context.task = Some(task::current());

            Ok(Async::NotReady)
        }
    }
}

/// A queued message.
pub struct Message<T> {
    id: usize,
    payload: Option<T>,
    queue: Option<Arc<Mutex<Queue<T>>>>,
}

impl<T> Message<T> {
    /// Create a new Message with a given message ID and payload.
    fn new(id: usize, payload: T) -> Message<T> {
        Message {
            id,
            payload: Some(payload),
            queue: None,
        }
    }

    /// Delete the message. You should call this method once the message is
    /// safely delivered. Be careful though, the message gets also deleted on
    /// drop.
    pub fn delete(&mut self) {
        if let Some(queue) = self.queue.take() {
            queue.lock().unwrap().remove_in_flight(self.id)
        }
    }

    /// Deconstruct the message into a delete handle and the message payload.
    pub fn deconstruct(mut self) -> (T, MessageDeleteHandle<T>) {
        let payload = self.payload.take().unwrap();
        let dhandle = MessageDeleteHandle::new(self.id, self.queue.take());

        (payload, dhandle)
    }
}

impl<T> Drop for Message<T> {
    fn drop(&mut self) {
        self.delete()
    }
}

/// Delete handle for a queued message.
pub struct MessageDeleteHandle<T> {
    id: usize,
    queue: Option<Arc<Mutex<Queue<T>>>>,
}

impl<T> MessageDeleteHandle<T> {
    /// Create a new message delete handle.
    fn new(id: usize, queue: Option<Arc<Mutex<Queue<T>>>>) -> MessageDeleteHandle<T> {
        MessageDeleteHandle { id, queue }
    }

    /// Delete the message. You should call this method once the message is
    /// safely delivered. Be careful though, the message gets also deleted when
    /// the handle is dropped. Deleting the message does not invalidate the
    /// message payload.
    pub fn delete(&mut self) {
        if let Some(queue) = self.queue.take() {
            queue.lock().unwrap().remove_in_flight(self.id)
        }
    }
}

impl<T> Drop for MessageDeleteHandle<T> {
    fn drop(&mut self) {
        self.delete()
    }
}

/// Queue element. It is either a message or a flush resolver.
enum QueueItem<T> {
    Message(Message<T>),
    Flush(FlushResolver),
}

/// Channel queue.
struct Queue<T> {
    capacity: Option<usize>,
    messages: usize,
    pending: VecDeque<QueueItem<T>>,
    in_flight: HashSet<usize>,
    message_id: usize,

    receiver_task: Option<Task>,
    closed: bool,
}

impl<T> Queue<T> {
    /// Create a new message queue. The queue will have a given capacity or
    /// it will be unbounded if no capacity is given.
    fn new(capacity: Option<usize>) -> Queue<T> {
        if let Some(capacity) = capacity {
            assert!(capacity > 0);
        }

        Queue {
            capacity,
            messages: 0,
            pending: VecDeque::new(),
            in_flight: HashSet::new(),
            message_id: 0,

            receiver_task: None,
            closed: false,
        }
    }

    /// Send a given message or return an error if the queue is full or if the
    /// channel has been closed (i.e. the receiver has been dropped).
    fn send(&mut self, data: T) -> Result<(), Error> {
        if let Some(capacity) = self.capacity {
            if self.messages >= capacity {
                return Err(Error::from(
                    "unable to send a given message: the queue is full",
                ));
            }
        }

        if self.closed {
            return Err(Error::from(
                "unable to send a given message: the channel has been closed",
            ));
        }

        let message_id = self.message_id;

        self.message_id += 1;

        let message = Message::new(message_id, data);

        self.pending.push_back(QueueItem::Message(message));

        // increment the message counter
        self.messages += 1;

        // notify the receiver that there is a new message in the queue
        if let Some(task) = self.receiver_task.take() {
            task.notify();
        }

        Ok(())
    }

    /// Mark a given message ID as in-flight.
    fn add_in_flight(&mut self, message_id: usize) {
        self.in_flight.insert(message_id);
    }

    /// Remove a given in-flight message.
    fn remove_in_flight(&mut self, message_id: usize) {
        if self.in_flight.remove(&message_id) {
            // notify the receiver that the in-flight set is empty
            if self.in_flight.is_empty() {
                if let Some(task) = self.receiver_task.take() {
                    task.notify();
                }
            }
        }
    }

    /// Flush the queue. The method returns a future that will get resolved
    /// once all messages sent before the flush get deleted.
    fn flush(&mut self) -> Flush {
        let fcontext = Arc::new(Mutex::new(FlushContext::new()));

        let mut fresolver = FlushResolver::new(fcontext.clone());

        if self.closed {
            fresolver.resolve(Err(Error::from(
                "unable to flush the channel: the channel has been closed",
            )));
        } else {
            self.pending.push_back(QueueItem::Flush(fresolver));

            // notify the receiver about a pending flush
            if let Some(task) = self.receiver_task.take() {
                task.notify();
            }
        }

        Flush::new(fcontext)
    }

    /// Mark the channel as closed.
    fn close(&mut self) {
        self.closed = true;
    }
}

impl<T> Stream for Queue<T> {
    type Item = Message<T>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Message<T>>, ()> {
        // get rid of all flush handles at front if there is nothing in flight
        if self.in_flight.is_empty() {
            while let Some(item) = self.pending.pop_front() {
                if let QueueItem::Message(msg) = item {
                    // put the message back, we don't want to lose it
                    self.pending.push_front(QueueItem::Message(msg));

                    break;
                } else if let QueueItem::Flush(mut handle) = item {
                    handle.resolve(Ok(()));
                } else {
                    panic!("unhandled QueueItem variant");
                }
            }
        }

        if let Some(item) = self.pending.pop_front() {
            if let QueueItem::Message(msg) = item {
                // decrement the message counter
                self.messages -= 1;

                Ok(Async::Ready(Some(msg)))
            } else if let QueueItem::Flush(handle) = item {
                // put the flush handle back, we need to wait until the
                // in_flight set is empty
                self.pending.push_front(QueueItem::Flush(handle));

                self.receiver_task = Some(task::current());

                Ok(Async::NotReady)
            } else {
                panic!("unhandled QueueItem variant");
            }
        } else {
            // the queue is empty, wake me up again once there is something
            // inside
            self.receiver_task = Some(task::current());

            Ok(Async::NotReady)
        }
    }
}

/// Sender part of the channel.
#[derive(Clone)]
pub struct Sender<T> {
    queue: Arc<Mutex<Queue<T>>>,
}

impl<T> Sender<T> {
    /// Create a new sender.
    fn new(queue: Arc<Mutex<Queue<T>>>) -> Sender<T> {
        Sender { queue }
    }

    /// Send a given message or return an error if the queue is full or if the
    /// channel has been closed (i.e. the receiver has been dropped).
    pub fn send(&mut self, data: T) -> Result<(), Error> {
        self.queue.lock().unwrap().send(data)
    }

    /// Flush the queue. The method returns a future that will get resolved
    /// once all messages sent before the flush get deleted.
    pub fn flush(&mut self) -> Flush {
        self.queue.lock().unwrap().flush()
    }
}

/// Receiver part of the channel.
pub struct Receiver<T> {
    queue: Arc<Mutex<Queue<T>>>,
}

impl<T> Receiver<T> {
    /// Create a new receiver.
    fn new(queue: Arc<Mutex<Queue<T>>>) -> Receiver<T> {
        Receiver { queue }
    }
}

impl<T> Stream for Receiver<T> {
    type Item = Message<T>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Message<T>>, ()> {
        let res = self.queue.lock().unwrap().poll();

        if let Ok(Async::Ready(Some(mut message))) = res {
            message.queue = Some(self.queue.clone());

            // mark the message as in-flight
            self.queue.lock().unwrap().add_in_flight(message.id);

            Ok(Async::Ready(Some(message)))
        } else {
            res
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.queue.lock().unwrap().close()
    }
}

/// Create a new MPSC channel. It will be a bounded channel if a capacity is
/// given.
pub fn new<T>(capacity: Option<usize>) -> (Sender<T>, Receiver<T>) {
    let queue = Arc::new(Mutex::new(Queue::new(capacity)));

    let tx = Sender::new(queue.clone());
    let rx = Receiver::new(queue);

    (tx, rx)
}
