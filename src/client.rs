use std::time::Duration;

use bytes::Bytes;

use futures;

use futures::{Future, Stream};

use hyper::client::HttpConnector;
use hyper::header::ContentType;
use hyper::{Client, Method, Request, Uri};

use hyper_tls::HttpsConnector;

use tokio_core::reactor::{Handle, Timeout};

use error::Error;

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
    pub fn build(self, handle: &Handle) -> Result<LogglyClient, Error> {
        let connector;

        if let Some(c) = self.connector {
            connector = c;
        } else {
            connector = HttpsConnector::new(1, handle).map_err(|err| {
                Error::from(format!("unable to create a HTTPS connector: {}", err))
            })?;
        }

        let client = Client::configure()
            .connector(connector)
            .keep_alive(false)
            .retry_canceled_requests(false)
            .build(handle);

        let url = format!(
            "https://logs-01.loggly.com/bulk/{}/tag/{}/",
            self.token, self.tag
        );
        let url = url.parse()
            .map_err(|_| Error::from(format!("unable to parse Loggly URL")))?;

        let res = LogglyClient {
            url: url,
            timeout: self.request_timeout,
            debug: self.debug,
            handle: handle.clone(),
            client: client,
        };

        Ok(res)
    }
}

/// Loggly client.
#[derive(Clone)]
pub struct LogglyClient {
    url: Uri,
    timeout: Duration,
    debug: bool,
    handle: Handle,
    client: Client<HttpsConnector<HttpConnector>>,
}

impl LogglyClient {
    /// Create a new client builder.
    pub fn builder(token: &str, tag: &str) -> LogglyClientBuilder {
        LogglyClientBuilder::new(token, tag)
    }

    /// Send a given batch of messages.
    pub fn batch_send<I>(&self, messages: I) -> Box<Future<Item = (), Error = ()>>
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
    pub fn send(&self, msg: Bytes) -> Box<Future<Item = (), Error = ()>> {
        let worker = self.clone();

        let fut = futures::stream::repeat(())
            .take_while(move |_| {
                worker.try_send(msg.clone()).then(|res| match res {
                    Ok(_) => Ok(false),
                    Err(_) => Ok(true),
                })
            })
            .for_each(|_| Ok(()));

        Box::new(fut)
    }

    /// Return a future that will try to send a given log message.
    pub fn try_send(&self, msg: Bytes) -> Box<Future<Item = (), Error = Error>> {
        let timeout = Timeout::new(self.timeout, &self.handle);

        let timeout = futures::future::result(timeout)
            .and_then(|timeout| timeout)
            .map_err(|err| Error::from(format!("timer error: {}", err)))
            .and_then(|_| Err(Error::from("request timeout")));

        let mut request = Request::new(Method::Post, self.url.clone());

        request.set_body(msg);
        request.headers_mut().set(ContentType::text());

        let fut = self.client
            .request(request)
            .and_then(|res| {
                let status = res.status();

                res.body()
                    .concat2()
                    .and_then(move |body| Ok((status, body)))
            })
            .map_err(|err| Error::from(format!("unable to send a request: {}", err)))
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

        let result = timeout
            .select(fut)
            .then(|res| match res {
                Ok((ok, _)) => Ok(ok),
                Err((err, _)) => Err(err),
            })
            .map_err(move |err| {
                if debug {
                    eprintln!("Loggly request failed: {}", err);
                }

                err
            });

        Box::new(result)
    }
}
