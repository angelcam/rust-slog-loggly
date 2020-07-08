use std::{
    collections::{HashSet, VecDeque},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use futures::{Stream, StreamExt};

use crate::error::Error;

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
    AsyncFlush(futures::channel::oneshot::Sender<Result<(), Error>>),
    BlockingFlush(std::sync::mpsc::Sender<Result<(), Error>>),
}

/// Channel queue.
struct Queue<T> {
    capacity: Option<usize>,
    messages: usize,
    pending: VecDeque<QueueItem<T>>,
    in_flight: HashSet<usize>,
    message_id: usize,

    receiver_task: Option<Waker>,
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
                return Err(Error::new(
                    "unable to send a given message: the queue is full",
                ));
            }
        }

        if self.closed {
            return Err(Error::new(
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
            task.wake();
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
                    task.wake();
                }
            }
        }
    }

    /// Flush the queue. The method returns a future that will get resolved
    /// once all messages sent before the flush get deleted.
    fn async_flush(&mut self) -> futures::channel::oneshot::Receiver<Result<(), Error>> {
        let (tx, rx) = futures::channel::oneshot::channel();

        if self.closed {
            let res = tx.send(Err(Error::new(
                "unable to flush the channel: the channel has been closed",
            )));

            res.unwrap_or_default();
        } else {
            self.pending.push_back(QueueItem::AsyncFlush(tx));

            // notify the receiver about a pending flush
            if let Some(task) = self.receiver_task.take() {
                task.wake();
            }
        }

        rx
    }

    /// Flush the queue. The method returns a blocking handle that will get
    /// resolved once all messages sent before the flush get deleted.
    fn blocking_flush(&mut self) -> std::sync::mpsc::Receiver<Result<(), Error>> {
        let (tx, rx) = std::sync::mpsc::channel();

        if self.closed {
            let res = tx.send(Err(Error::new(
                "unable to flush the channel: the channel has been closed",
            )));

            res.unwrap_or_default();
        } else {
            self.pending.push_back(QueueItem::BlockingFlush(tx));

            // notify the receiver about a pending flush
            if let Some(task) = self.receiver_task.take() {
                task.wake();
            }
        }

        rx
    }

    /// Mark the channel as closed.
    fn close(&mut self) {
        self.closed = true;
    }
}

impl<T> Stream for Queue<T>
where
    T: Unpin,
{
    type Item = Message<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // get rid of all flush handles at front if there is nothing in flight
        if self.in_flight.is_empty() {
            while let Some(item) = self.pending.pop_front() {
                match item {
                    QueueItem::Message(msg) => {
                        // put the message back, we don't want to lose it
                        self.pending.push_front(QueueItem::Message(msg));

                        break;
                    }
                    QueueItem::AsyncFlush(handle) => {
                        handle.send(Ok(())).unwrap_or_default();
                    }
                    QueueItem::BlockingFlush(handle) => {
                        handle.send(Ok(())).unwrap_or_default();
                    }
                }
            }
        }

        if let Some(item) = self.pending.pop_front() {
            match item {
                QueueItem::Message(msg) => {
                    // decrement the message counter
                    self.messages -= 1;

                    Poll::Ready(Some(msg))
                }
                flush => {
                    // put the flush handle back, we need to wait until the
                    // in_flight set is empty
                    self.pending.push_front(flush);

                    self.receiver_task = Some(cx.waker().clone());

                    Poll::Pending
                }
            }
        } else {
            // the queue is empty, wake me up again once there is something
            // inside
            self.receiver_task = Some(cx.waker().clone());

            Poll::Pending
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
    pub fn async_flush(&mut self) -> futures::channel::oneshot::Receiver<Result<(), Error>> {
        self.queue.lock().unwrap().async_flush()
    }

    /// Flush the queue. The method returns a blocking handle that will get
    /// resolved once all messages sent before the flush get deleted.
    pub fn blocking_flush(&mut self) -> std::sync::mpsc::Receiver<Result<(), Error>> {
        self.queue.lock().unwrap().blocking_flush()
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

impl<T> Stream for Receiver<T>
where
    T: Unpin,
{
    type Item = Message<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = self.queue.lock().unwrap().poll_next_unpin(cx);

        if let Poll::Ready(Some(mut message)) = res {
            message.queue = Some(self.queue.clone());

            // mark the message as in-flight
            self.queue.lock().unwrap().add_in_flight(message.id);

            Poll::Ready(Some(message))
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
