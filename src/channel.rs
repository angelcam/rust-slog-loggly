use futures;

use futures::{Poll, Stream};

use error::Error;

/// Bounded or unbounded message sender (i.e. a wrapper around MPSC senders
/// from the futures crate).
#[derive(Clone)]
pub enum Sender<T> {
    Bounded(futures::sync::mpsc::Sender<T>),
    Unbounded(futures::sync::mpsc::UnboundedSender<T>),
}

impl<T> Sender<T> {
    /// Send a given message.
    pub fn send(&mut self, msg: T) -> Result<(), Error> {
        match self {
            &mut Sender::Bounded(ref mut sender) => sender
                .try_send(msg)
                .map_err(|err| Error::from(format!("unable to send a message: {}", err))),
            &mut Sender::Unbounded(ref mut sender) => sender
                .unbounded_send(msg)
                .map_err(|err| Error::from(format!("unable to send a message: {}", err))),
        }
    }
}

/// Bounded or unbounded message receiver (i.e. a wrapper around MPSC receivers
/// from the futures crate).
pub enum Receiver<T> {
    Bounded(futures::sync::mpsc::Receiver<T>),
    Unbounded(futures::sync::mpsc::UnboundedReceiver<T>),
}

impl<T> Stream for Receiver<T> {
    type Item = T;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<T>, ()> {
        match self {
            &mut Receiver::Bounded(ref mut receiver) => receiver.poll(),
            &mut Receiver::Unbounded(ref mut receiver) => receiver.poll(),
        }
    }
}

/// Create a new bounded MPSC channel.
pub fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = futures::sync::mpsc::channel::<T>(capacity);

    let tx = Sender::Bounded(tx);
    let rx = Receiver::Bounded(rx);

    (tx, rx)
}

/// Create a new unbounded MPSC channel.
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = futures::sync::mpsc::unbounded::<T>();

    let tx = Sender::Unbounded(tx);
    let rx = Receiver::Unbounded(rx);

    (tx, rx)
}

/// Create a new MPSC channel. It will be a bounded channel if a capacity is
/// given.
pub fn new<T>(capacity: Option<usize>) -> (Sender<T>, Receiver<T>) {
    if let Some(capacity) = capacity {
        bounded(capacity)
    } else {
        unbounded()
    }
}
