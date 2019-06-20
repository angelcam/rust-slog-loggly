use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use futures;

use futures::{Future, Poll, Stream};

use hyper::client::HttpConnector;
use hyper::{Client, Request, Uri};

use hyper_tls::HttpsConnector;

use tokio_timer::Timeout;

use crate::batch::BatchStream;

use crate::channel::Message;

use crate::error::Error;

/// Default request timeout in seconds.
const DEFAULT_REQUEST_TIMEOUT: u64 = 5;

/// LogglyClient builder.
pub struct LogglyClientBuilder {
    token: String,
    tag: String,
    request_timeout: Duration,
    debug: bool,
    connector: Option<HttpsConnector<HttpConnector>>,
}

impl LogglyClientBuilder {
    /// Create a new builder. Use a given Loggly token and tag.
    fn new(token: &str, tag: &str) -> LogglyClientBuilder {
        let request_timeout = Duration::from_secs(DEFAULT_REQUEST_TIMEOUT);

        LogglyClientBuilder {
            token: token.to_string(),
            tag: tag.to_string(),
            request_timeout: request_timeout,
            debug: false,
            connector: None,
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

    /// Use a given HttpsConnector.
    pub fn connector(mut self, connector: HttpsConnector<HttpConnector>) -> LogglyClientBuilder {
        self.connector = Some(connector);
        self
    }

    /// Create the LogglyClient.
    pub fn build(self) -> Result<LogglyClient, Error> {
        let connector;

        if let Some(c) = self.connector {
            connector = c;
        } else {
            connector = HttpsConnector::new(1).map_err(|err| {
                Error::from(format!("unable to create a HTTPS connector: {}", err))
            })?;
        }

        let client = Client::builder().build(connector);

        let url = format!(
            "https://logs-01.loggly.com/bulk/{}/tag/{}/",
            self.token, self.tag
        );
        let url = url
            .parse()
            .map_err(|_| Error::from(format!("unable to parse Loggly URL")))?;
        let url = Arc::new(url);

        let res = LogglyClient {
            url: url,
            timeout: self.request_timeout,
            debug: self.debug,
            client: client,
        };

        Ok(res)
    }
}

/// Loggly client.
#[derive(Clone)]
pub struct LogglyClient {
    url: Arc<Uri>,
    timeout: Duration,
    debug: bool,
    client: Client<HttpsConnector<HttpConnector>>,
}

impl LogglyClient {
    /// Create a new client builder.
    pub fn builder(token: &str, tag: &str) -> LogglyClientBuilder {
        LogglyClientBuilder::new(token, tag)
    }

    /// Send a given batch of messages.
    pub fn batch_send<I>(&self, messages: I) -> impl Future<Item = (), Error = ()>
    where
        I: IntoIterator<Item = Bytes>,
    {
        let mut batch = Vec::<u8>::new();

        for msg in messages {
            batch.extend_from_slice(msg.as_ref());
            batch.push('\n' as u8);
        }

        self.send(Bytes::from(batch))
    }

    /// Return a future that will ensure sending a given log message.
    pub fn send(&self, msg: Bytes) -> impl Future<Item = (), Error = ()> {
        let client = self.clone();

        futures::stream::repeat(())
            .take_while(move |_| {
                client.try_send(msg.clone()).then(|res| match res {
                    Ok(_) => Ok(false),
                    Err(_) => Ok(true),
                })
            })
            .for_each(|_| Ok(()))
    }

    /// Return a future that will try to send a given log message.
    pub fn try_send(&self, msg: Bytes) -> impl Future<Item = (), Error = Error> {
        let request = Request::post(&*self.url)
            .header("Content-Type", "text/plain")
            .body(msg.into())
            .map_err(|_| Error::from("unable to create a request body"));

        let client = self.client.clone();

        let fut = futures::future::result(request)
            .and_then(move |request| {
                client
                    .request(request)
                    .map_err(|err| Error::from(format!("unable to send a request: {}", err)))
            })
            .and_then(|res| {
                let status = res.status();

                res.into_body()
                    .concat2()
                    .and_then(move |body| Ok((status, body)))
                    .map_err(|err| Error::from(format!("unable to read a response body: {}", err)))
            })
            .and_then(|(status, body)| {
                if status.is_success() {
                    Ok(())
                } else {
                    let body = String::from_utf8_lossy(&body);

                    Err(Error::from(format!(
                        "server responded with HTTP {}:\n{}",
                        status, body
                    )))
                }
            });

        let debug = self.debug;

        Timeout::new(fut, self.timeout).map_err(move |err| {
            if debug {
                eprintln!("Loggly request failed: {}", err);
            }

            if err.is_inner() {
                err.into_inner().unwrap()
            } else if err.is_elapsed() {
                Error::from("request timeout")
            } else {
                Error::from("timer error")
            }
        })
    }

    /// Consume all messages from a given stream and send them to Loggly.
    pub fn send_all<S>(
        self,
        messages: S,
        batch_size: usize,
        sender_count: usize,
    ) -> LogglyMessageSender
    where
        S: 'static + Stream<Item = Message<Bytes>, Error = ()> + Send,
    {
        let sender = messages
            .batch_stream(batch_size)
            .and_then(move |messages| {
                let mut batch = Vec::new();
                let mut dhandles = Vec::new();

                for msg in messages.into_iter() {
                    let (payload, dhandle) = msg.deconstruct();

                    batch.push(payload);
                    dhandles.push(dhandle);
                }

                let future = self.batch_send(batch).and_then(move |_| {
                    // mark all messages as deleted once they are sent
                    for mut dhandle in dhandles {
                        dhandle.delete();
                    }

                    Ok(())
                });

                Ok(future)
            })
            .buffered(sender_count)
            .for_each(|_| Ok(()));

        LogglyMessageSender {
            inner: Box::new(sender),
        }
    }
}

/// A future driving the send of all log messages.
pub struct LogglyMessageSender {
    inner: Box<Future<Item = (), Error = ()> + Send>,
}

impl Future for LogglyMessageSender {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        self.inner.poll()
    }
}
