use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::stream::{Fuse, Stream, StreamExt};

/// A simple extension to the futures::Stream allowing to take elements in
/// batches of a given maximum size.
pub trait BatchStream {
    /// Create a stream of batches of stream items. Each batch will contain
    /// at most max_batch_size elements.
    fn batch_stream(self, max_batch_size: usize) -> Batch<Self>
    where
        Self: Stream + Sized,
    {
        Batch::new(self, max_batch_size)
    }
}

impl<T> BatchStream for T where T: Stream {}

/// Batch combinator.
pub struct Batch<S> {
    stream: Fuse<S>,
    max_size: usize,
}

impl<S> Batch<S> {
    /// Create a new batch combinator.
    fn new(stream: S, max_batch_size: usize) -> Batch<S>
    where
        S: Stream,
    {
        Batch {
            stream: stream.fuse(),
            max_size: max_batch_size,
        }
    }
}

impl<S> Stream for Batch<S>
where
    S: Stream + Unpin,
{
    type Item = Vec<S::Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut res = Vec::new();

        while res.len() < self.max_size {
            if let Poll::Ready(item) = self.stream.poll_next_unpin(cx) {
                if let Some(item) = item {
                    res.push(item);
                } else if res.is_empty() {
                    return Poll::Ready(None);
                } else {
                    return Poll::Ready(Some(res));
                }
            } else {
                break;
            }
        }

        if res.is_empty() {
            Poll::Pending
        } else {
            Poll::Ready(Some(res))
        }
    }
}
