use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt};
use reqwest::{Client, Url};

use crate::{batch::BatchStream, channel::Message, error::Error};

/// Default request timeout in seconds.
const DEFAULT_REQUEST_TIMEOUT: u64 = 5;

/// LogglyClient builder.
pub struct LogglyClientBuilder {
    token: String,
    tag: String,
    request_timeout: Duration,
    debug: bool,
}

impl LogglyClientBuilder {
    /// Create a new builder. Use a given Loggly token and tag.
    fn new(token: &str, tag: &str) -> LogglyClientBuilder {
        let request_timeout = Duration::from_secs(DEFAULT_REQUEST_TIMEOUT);

        LogglyClientBuilder {
            token: token.to_string(),
            tag: tag.to_string(),
            request_timeout,
            debug: false,
        }
    }

    /// Enable or disable debug mode (it's disabled by default).  In the debug
    /// mode you'll be able to see some runtime info on stderr that will help
    /// you with setting up the drain (e.g. failed requests). With debug mode
    /// disabled, all errors will be silently ignored.
    pub fn debug_mode(mut self, enable: bool) -> LogglyClientBuilder {
        self.debug = enable;
        self
    }

    /// Set Loggly request timeout (the default is 5 seconds).
    pub fn request_timeout(mut self, timeout: Duration) -> LogglyClientBuilder {
        self.request_timeout = timeout;
        self
    }

    /// Create the LogglyClient.
    pub fn build(self) -> Result<LogglyClient, Error> {
        let client = Client::builder()
            .timeout(self.request_timeout)
            .build()
            .map_err(|err| Error::new(format!("unable to create an HTTP client: {err}")))?;

        let url = format!(
            "https://logs-01.loggly.com/bulk/{}/tag/{}/",
            self.token, self.tag
        );

        let url = url
            .parse()
            .map_err(|_| Error::new("unable to parse Loggly URL"))?;

        let url = Arc::new(url);

        let res = LogglyClient {
            url,
            debug: self.debug,
            client,
        };

        Ok(res)
    }
}

/// Loggly client.
#[derive(Clone)]
pub struct LogglyClient {
    url: Arc<Url>,
    debug: bool,
    client: Client,
}

impl LogglyClient {
    /// Create a new client builder.
    pub fn builder(token: &str, tag: &str) -> LogglyClientBuilder {
        LogglyClientBuilder::new(token, tag)
    }

    /// Send a given batch of messages.
    pub async fn batch_send<I>(&self, messages: I)
    where
        I: IntoIterator<Item = Bytes>,
    {
        let mut batch = Vec::<u8>::new();

        for msg in messages {
            batch.extend_from_slice(msg.as_ref());
            batch.push(b'\n');
        }

        self.send(Bytes::from(batch)).await
    }

    /// Return a future that will ensure sending a given log message.
    pub async fn send(&self, msg: Bytes) {
        loop {
            if self.try_send(msg.clone()).await.is_ok() {
                return;
            }
        }
    }

    /// Try to send a given log message.
    pub async fn try_send(&self, msg: Bytes) -> Result<(), Error> {
        let res = self.try_send_inner(msg).await;

        if self.debug {
            if let Err(err) = res.as_ref() {
                eprintln!("Loggly request failed: {}", err);
            }
        }

        res
    }

    /// Try to send a given log message.
    async fn try_send_inner(&self, msg: Bytes) -> Result<(), Error> {
        let url = &*self.url;

        let response = self
            .client
            .post(url.clone())
            .header("Content-Type", "text/plain")
            .body(msg)
            .send()
            .await
            .map_err(|err| Error::new(format!("unable to send a request: {}", err)))?;

        let status = response.status();

        let body = response
            .bytes()
            .await
            .map_err(|err| Error::new(format!("unable to read a response body: {}", err)))?;

        if status.is_success() {
            Ok(())
        } else {
            let body = String::from_utf8_lossy(&body);

            Err(Error::new(format!(
                "server responded with HTTP {}:\n{}",
                status, body
            )))
        }
    }

    /// Consume all messages from a given stream and send them to Loggly.
    pub fn send_all<S>(
        self,
        messages: S,
        batch_size: usize,
        sender_count: usize,
    ) -> LogglyMessageSender
    where
        S: Stream<Item = Message<Bytes>> + Send + Unpin + 'static,
    {
        let sender = messages
            .batch_stream(batch_size)
            .map(move |messages| {
                let mut batch = Vec::new();
                let mut dhandles = Vec::new();

                for msg in messages.into_iter() {
                    let (payload, dhandle) = msg.deconstruct();

                    batch.push(payload);
                    dhandles.push(dhandle);
                }

                let client = self.clone();

                async move {
                    client.batch_send(batch).await;

                    // mark all messages as deleted once they are sent
                    for mut dhandle in dhandles {
                        dhandle.delete();
                    }
                }
            })
            .buffer_unordered(sender_count)
            .for_each(|_| futures::future::ready(()));

        LogglyMessageSender {
            inner: Box::pin(sender),
        }
    }
}

/// A future driving the send of all log messages.
pub struct LogglyMessageSender {
    inner: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl Future for LogglyMessageSender {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.poll_unpin(cx)
    }
}
