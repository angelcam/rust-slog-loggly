//! A Rust library providing an slog drain for sending log messages to Loggly.
//!
//! # Things to be aware of
//!
//! The drain serializes all log messages as JSON objects. If you use key-value
//! pairs in your loggers and log messages, you should know that one key-value
//! pair can override another if they both have the same key. The overrides
//! follow this simple rule:
//! 1. Derived loggers can override key-value pairs of their ancestors.
//! 2. Log messages can override key-value pairs of their loggers.
//! 3. The latest specified key-value pair overrides everything specified
//!    before.
//!
//! # Usage
//!
//! Please note that the Loggly drain is asynchronous and the log messages are
//! sent on background. If your application exits, there might be still some
//! log messages in the queue.
//!
//! ## Using the Loggly drain in an asynchronous application
//!
//! ```rust
//! extern crate futures;
//!
//! #[macro_use]
//! extern crate slog;
//! extern crate slog_loggly;
//! extern crate tokio_core;
//!
//! use slog::{Drain, Logger};
//!
//! use slog_loggly::LogglyDrain;
//!
//! use tokio_core::reactor::{Core, Handle};
//!
//! fn main() {
//!     // Your Loggly token and tag.
//!     let loggly_token = "your-loggly-token";
//!     let loggly_tag = "some-app";
//!
//!     let mut core = Core::new().unwrap();
//!
//!     let handle = core.handle();
//!
//!     // Create a custom Loggly drain.
//!     let drain = LogglyDrain::builder(loggly_token, loggly_tag)
//!         .spawn_task(&handle)
//!         .unwrap()
//!         .fuse();
//!
//!     // Create a logger.
//!     let logger = Logger::root(drain, o!());
//!
//!     debug!(logger, "debug"; "key" => "value");
//!     info!(logger, "info"; "key" => "value");
//!     warn!(logger, "warn"; "key" => "value");
//!     error!(logger, "error"; "key" => "value");
//!
//!     // do some asynchronous work...
//!     let res: Result<(), ()> = core.run(futures::future::ok(()));
//!
//!     res.unwrap();
//! }
//! ```
//!
//! ## Using the Loggly drain in a normal application
//!
//! ```rust
//! #[macro_use]
//! extern crate slog;
//! extern crate slog_loggly;
//!
//! use slog::{Drain, Logger};
//!
//! use slog_loggly::LogglyDrain;
//!
//! fn main() {
//!     // Your Loggly token and tag.
//!     let loggly_token = "your-loggly-token";
//!     let loggly_tag = "some-app";
//!
//!     // Create a custom Loggly drain.
//!     let drain = LogglyDrain::builder(loggly_token, loggly_tag)
//!         .spawn_thread()
//!         .fuse();
//!
//!     // Create a logger.
//!     let logger = Logger::root(drain, o!());
//!
//!     debug!(logger, "debug"; "key" => "value");
//!     info!(logger, "info"; "key" => "value");
//!     warn!(logger, "warn"; "key" => "value");
//!     error!(logger, "error"; "key" => "value");
//!
//!     // do some work...
//! }
//! ```

extern crate bytes;
extern crate futures;
extern crate hyper;
extern crate hyper_tls;
extern crate serde;
extern crate serde_json;
extern crate slog;
extern crate tokio_core;

mod batch;
mod channel;
mod client;
mod error;
mod serializer;

use std::str;
use std::thread;

use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;

use futures::Stream;

use hyper::client::HttpConnector;

use hyper_tls::HttpsConnector;

use slog::{Drain, OwnedKVList, Record, KV};
use slog::Serializer as SlogSerializer;

use tokio_core::reactor::{Core, Handle};

use batch::BatchStream;

use channel::Sender;

use client::LogglyClient;

use error::Error;

use serializer::LogglyMessageSerializer;

const DEFAULT_SENDER_COUNT: usize = 16;
const DEFAULT_BATCH_SIZE: usize = 20;

/// Loggly drain builder.
pub struct LogglyDrainBuilder {
    queue_max_size: Option<usize>,
    batch_size: usize,
    sender_count: usize,
    token: String,
    tag: String,
    request_timeout: Option<Duration>,
    connector: Option<HttpsConnector<HttpConnector>>,
    debug: bool,
}

impl LogglyDrainBuilder {
    /// Create a new builder. Use a given Loggly token and tag.
    fn new(token: &str, tag: &str) -> LogglyDrainBuilder {
        LogglyDrainBuilder {
            queue_max_size: None,
            batch_size: DEFAULT_BATCH_SIZE,
            sender_count: DEFAULT_SENDER_COUNT,
            token: token.to_string(),
            tag: tag.to_string(),
            request_timeout: None,
            connector: None,
            debug: false,
        }
    }

    /// Enable or disable debug mode (it's disabled by default).  In the debug
    /// mode you'll be able to see some runtime info on stderr that will help
    /// you with setting up the drain (e.g. failed requests). With debug mode
    /// disabled, all errors will be silently ignored.
    pub fn debug_mode(mut self, enable: bool) -> LogglyDrainBuilder {
        self.debug = enable;
        self
    }

    /// Set a given maximum size of the message queue (the default is unlimited).
    pub fn queue_max_size(mut self, size: usize) -> LogglyDrainBuilder {
        self.queue_max_size = Some(size);
        self
    }

    /// Maximum number of messages sent in one batch (the default is 20).
    pub fn batch_size(mut self, size: usize) -> LogglyDrainBuilder {
        self.batch_size = size;
        self
    }

    /// Set the number of concurrent senders (the default is 16).
    pub fn sender_count(mut self, count: usize) -> LogglyDrainBuilder {
        self.sender_count = count;
        self
    }

    /// Set Loggly request timeout (the default is 5 seconds).
    pub fn request_timeout(mut self, timeout: Duration) -> LogglyDrainBuilder {
        self.request_timeout = Some(timeout);
        self
    }

    /// Use a given HttpsConnector. The connector is used only if the log
    /// message sender is spawned as a task.
    pub fn connector(mut self, connector: HttpsConnector<HttpConnector>) -> LogglyDrainBuilder {
        self.connector = Some(connector);
        self
    }

    /// Spawn the log message sender as a separate task using a given handle
    /// and return the drain.
    pub fn spawn_task(self, handle: &Handle) -> Result<LogglyDrain, Error> {
        let (tx, rx) = channel::new::<Bytes>(self.queue_max_size);

        let mut builder = LogglyClient::builder(&self.token, &self.tag);

        if let Some(timeout) = self.request_timeout {
            builder = builder.request_timeout(timeout);
        }

        if let Some(connector) = self.connector {
            builder = builder.connector(connector);
        }

        let client = builder.debug_mode(self.debug).build(handle)?;

        let sender = rx.batch_stream(self.batch_size)
            .and_then(move |batch| Ok(client.batch_send(batch)))
            .buffered(self.sender_count)
            .for_each(|_| Ok(()));

        handle.spawn(sender);

        let res = LogglyDrain {
            sender: Mutex::new(tx),
            debug: self.debug,
        };

        Ok(res)
    }

    /// Spawn the log message sender as a separate thread and return the drain.
    /// You should not use this method in asynchronous applications.
    pub fn spawn_thread(self) -> LogglyDrain {
        let (tx, rx) = channel::new::<Bytes>(self.queue_max_size);

        let loggly_token = self.token;
        let loggly_tag = self.tag;
        let request_timeout = self.request_timeout;
        let batch_size = self.batch_size;
        let sender_count = self.sender_count;
        let debug = self.debug;

        thread::spawn(move || {
            let mut core = Core::new().expect("unable to create a tokio Core");

            let handle = core.handle();

            let mut builder = LogglyClient::builder(&loggly_token, &loggly_tag);

            if let Some(timeout) = request_timeout {
                builder = builder.request_timeout(timeout);
            }

            let client = builder
                .debug_mode(debug)
                .build(&handle)
                .expect("unable to create a Loggly client");

            let sender = rx.batch_stream(batch_size)
                .and_then(move |msg| Ok(client.batch_send(msg)))
                .buffered(sender_count)
                .for_each(|_| Ok(()));

            core.run(sender).unwrap();
        });

        LogglyDrain {
            sender: Mutex::new(tx),
            debug: self.debug,
        }
    }
}

/// Loggly drain.
pub struct LogglyDrain {
    sender: Mutex<Sender<Bytes>>,
    debug: bool,
}

impl LogglyDrain {
    /// Create a LogglyDrain builder for a given Loggly token and tag.
    pub fn builder(token: &str, tag: &str) -> LogglyDrainBuilder {
        LogglyDrainBuilder::new(token, tag)
    }
}

impl Drain for LogglyDrain {
    type Ok = ();
    type Err = ();

    fn log(&self, record: &Record, logger_values: &OwnedKVList) -> Result<(), ()> {
        let message = serialize(record, logger_values);

        if let Ok(message) = message {
            let res = self.sender.lock().unwrap().send(message.clone());

            if let Err(err) = res {
                if self.debug {
                    let message =
                        str::from_utf8(message.as_ref()).unwrap_or("unable to decode the message");

                    eprintln!(
                        "unable to send a log message: {}; message: {}",
                        err, message
                    );
                }
            }
        } else if let Err(err) = message {
            if self.debug {
                eprintln!("unable to serialize a log message: {}", err);
            }
        }

        Ok(())
    }
}

/// Serialize a given log record as as a Loggly JSON string.
fn serialize(record: &Record, logger_values: &OwnedKVList) -> slog::Result<Bytes> {
    let mut serializer = LogglyMessageSerializer::new();

    let level = record.level().as_str().to_lowercase();

    let file = record.file();
    let line = record.line();

    serializer.emit_str("level", &level)?;
    serializer.emit_arguments("file", &format_args!("{}:{}", file, line))?;
    serializer.emit_arguments("message", record.msg())?;

    logger_values.serialize(record, &mut serializer)?;

    record.kv().serialize(record, &mut serializer)?;

    let message = serializer.finish()?;

    Ok(message)
}
