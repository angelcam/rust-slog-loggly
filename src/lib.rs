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
//! use slog::{debug, error, info, o, warn, Drain, Logger};
//! use slog_loggly::LogglyDrain;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Your Loggly token and tag.
//!     let loggly_token = "your-loggly-token";
//!     let loggly_tag = "some-app";
//!
//!     // Create a custom Loggly drain.
//!     let (drain, mut fhandle) = LogglyDrain::builder(loggly_token, loggly_tag)
//!         .spawn_task()
//!         .unwrap();
//!
//!     // Create a logger.
//!     let logger = Logger::root(drain.fuse(), o!());
//!
//!     debug!(logger, "debug"; "key" => "value");
//!     info!(logger, "info"; "key" => "value");
//!     warn!(logger, "warn"; "key" => "value");
//!     error!(logger, "error"; "key" => "value");
//!
//!     // You can return the flush handle to make sure that all log
//!     // messages get sent before the process terminates.
//!     // fhandle.async_flush().await.unwrap();
//! }
//! ```
//!
//! ## Using the Loggly drain in a normal application
//!
//! ```rust
//! use slog::{debug, error, info, o, warn, Drain, Logger};
//! use slog_loggly::LogglyDrain;
//!
//! // Your Loggly token and tag.
//! let loggly_token = "your-loggly-token";
//! let loggly_tag = "some-app";
//!
//! // Create a custom Loggly drain.
//! let (drain, mut fhandle) = LogglyDrain::builder(loggly_token, loggly_tag)
//!     .spawn_thread()
//!     .unwrap();
//!
//! // Create a logger.
//! let logger = Logger::root(drain.fuse(), o!());
//!
//! debug!(logger, "debug"; "key" => "value");
//! info!(logger, "info"; "key" => "value");
//! warn!(logger, "warn"; "key" => "value");
//! error!(logger, "error"; "key" => "value");
//!
//! // You can use the flush handle to make sure that all log messages get
//! // sent before the process terminates.
//! // fhandle.blocking_flush().unwrap();
//! ```

mod batch;
mod channel;
mod client;
mod error;
mod serializer;
mod retry;

use std::{str, sync::Mutex, time::Duration};

#[cfg(feature = "runtime")]
use std::thread;

use bytes::Bytes;
use chrono::{SecondsFormat, Utc};
use hyper::client::HttpConnector;
use hyper_tls::HttpsConnector;
use slog::{Drain, Key, OwnedKVList, Record, Serializer as SlogSerializer, KV};

use crate::{channel::Sender, client::LogglyClient, serializer::LogglyMessageSerializer};

pub use crate::{
    client::LogglyMessageSender,
    error::Error,
    serializer::{AcceptAll, KVFilter},
};

const DEFAULT_SENDER_COUNT: usize = 16;
const DEFAULT_BATCH_SIZE: usize = 20;

/// Loggly drain builder.
pub struct LogglyDrainBuilder<F = AcceptAll> {
    field_filter: F,
    fallback_field: Key,
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
    fn new(token: &str, tag: &str) -> Self {
        Self {
            field_filter: AcceptAll,
            fallback_field: "",
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
}

impl<F> LogglyDrainBuilder<F> {
    /// Use a given key-value pair filter.
    ///
    /// All key-value pairs rejected by the given filter will be serialized
    /// under a given fallback field.
    ///
    /// This feature can be used if you want Loggly to index only a given
    /// subset of fields.
    pub fn kv_filter<K, T>(self, fallback_field: K, filter: T) -> LogglyDrainBuilder<T>
    where
        K: Into<Key>,
    {
        let mut fallback_field = fallback_field.into();

        if fallback_field.is_empty() {
            fallback_field = "misc";
        }

        LogglyDrainBuilder {
            field_filter: filter,
            fallback_field,
            queue_max_size: self.queue_max_size,
            batch_size: self.batch_size,
            sender_count: self.sender_count,
            token: self.token,
            tag: self.tag,
            request_timeout: self.request_timeout,
            connector: self.connector,
            debug: self.debug,
        }
    }

    /// Enable or disable debug mode (it's disabled by default).  In the debug
    /// mode you'll be able to see some runtime info on stderr that will help
    /// you with setting up the drain (e.g. failed requests). With debug mode
    /// disabled, all errors will be silently ignored.
    pub fn debug_mode(mut self, enable: bool) -> Self {
        self.debug = enable;
        self
    }

    /// Set a given maximum size of the message queue (the default is unlimited).
    pub fn queue_max_size(mut self, size: usize) -> Self {
        self.queue_max_size = Some(size);
        self
    }

    /// Maximum number of messages sent in one batch (the default is 20).
    /// Please note that all log messages are sent as soon as possible.
    /// Increasing batch size won't cause any delays in sending messages. If
    /// there is not enough messages in the internal queue to make a maximum
    /// size batch, a smaller batch is sent.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the number of concurrent senders (the default is 16).
    pub fn sender_count(mut self, count: usize) -> Self {
        self.sender_count = count;
        self
    }

    /// Set Loggly request timeout (the default is 5 seconds).
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Use a given HttpsConnector. The connector is used only if the log
    /// message sender is spawned as a task.
    pub fn connector(mut self, connector: HttpsConnector<HttpConnector>) -> Self {
        self.connector = Some(connector);
        self
    }

    /// Build a Loggly drain.
    pub fn build(self) -> Result<(LogglyDrain<F>, LogglyMessageSender, FlushHandle), Error> {
        let (tx, rx) = channel::new::<Bytes>(self.queue_max_size);

        let mut builder = LogglyClient::builder(&self.token, &self.tag);

        if let Some(timeout) = self.request_timeout {
            builder = builder.request_timeout(timeout);
        }

        if let Some(connector) = self.connector {
            builder = builder.connector(connector);
        }

        let sender = builder.debug_mode(self.debug).build()?.send_all(
            rx,
            self.batch_size,
            self.sender_count,
        );

        let fhandle = FlushHandle::new(tx.clone());

        let drain = LogglyDrain {
            field_filter: self.field_filter,
            fallback_field: self.fallback_field,
            sender: Mutex::new(tx),
            debug: self.debug,
        };

        Ok((drain, sender, fhandle))
    }

    /// Spawn a tokio task within the current executor context. The task will
    /// be responsible for sending all log messages.
    #[cfg(feature = "runtime")]
    pub fn spawn_task(self) -> Result<(LogglyDrain<F>, FlushHandle), Error> {
        let (drain, sender, flush_handle) = self.build()?;

        tokio::spawn(sender);

        Ok((drain, flush_handle))
    }

    /// Spawn a thread responsible for sending all log messages.
    #[cfg(feature = "runtime")]
    pub fn spawn_thread(self) -> Result<(LogglyDrain<F>, FlushHandle), Error> {
        let (drain, sender, flush_handle) = self.build()?;

        thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .expect("unable to create tokio runtime");

            runtime.block_on(sender)
        });

        Ok((drain, flush_handle))
    }
}

/// Loggly drain.
pub struct LogglyDrain<F = AcceptAll> {
    field_filter: F,
    fallback_field: Key,
    sender: Mutex<Sender<Bytes>>,
    debug: bool,
}

impl LogglyDrain {
    /// Create a LogglyDrain builder for a given Loggly token and tag.
    pub fn builder(token: &str, tag: &str) -> LogglyDrainBuilder {
        LogglyDrainBuilder::new(token, tag)
    }
}

impl<F> LogglyDrain<F>
where
    F: KVFilter,
{
    /// Serialize a given log record.
    fn serialize(&self, record: &Record, logger_values: &OwnedKVList) -> slog::Result<Bytes> {
        let mut serializer = LogglyMessageSerializer::new()
            .with_field_filter(self.fallback_field, &self.field_filter);

        let level = record.level().as_str().to_lowercase();

        let file = record.file();
        let line = record.line();

        serializer.emit_str("level", &level)?;
        serializer.emit_arguments("file", &format_args!("{}:{}", file, line))?;
        serializer.emit_arguments("message", record.msg())?;

        logger_values.serialize(record, &mut serializer)?;

        record.kv().serialize(record, &mut serializer)?;

        let timestamp = Utc::now();

        serializer.emit_str(
            "timestamp",
            &timestamp.to_rfc3339_opts(SecondsFormat::Micros, true),
        )?;

        serializer.finish()
    }
}

impl<F> Drain for LogglyDrain<F>
where
    F: KVFilter,
{
    type Ok = ();
    type Err = ();

    fn log(&self, record: &Record, logger_values: &OwnedKVList) -> Result<(), ()> {
        let message = self.serialize(record, logger_values);

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

/// A flush handle that can be used to flush all currently queued log messages.
#[derive(Clone)]
pub struct FlushHandle {
    sender: Sender<Bytes>,
}

impl FlushHandle {
    /// Create a new FlushHandle.
    fn new(sender: Sender<Bytes>) -> FlushHandle {
        FlushHandle { sender }
    }

    /// Flush all currently queued log messages. The method will be resolved
    /// once all messages that have been sent before calling this method get
    /// successfully sent to Loggly.
    pub async fn async_flush(&mut self) -> Result<(), Error> {
        self.sender
            .async_flush()
            .await
            .map_err(|_| Error::new("Loggly message sender task has been canceled"))?
    }

    /// Flush all currently queued log messages. The method will be resolved
    /// once all messages that have been sent before calling this method get
    /// successfully sent to Loggly.
    pub fn blocking_flush(&mut self) -> Result<(), Error> {
        self.sender
            .blocking_flush()
            .recv()
            .map_err(|_| Error::new("Loggly message sender task has been canceled"))?
    }
}
