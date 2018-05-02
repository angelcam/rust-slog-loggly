extern crate slog_loggly;

#[macro_use]
extern crate slog;

use std::thread;

use std::time::Duration;

use slog_loggly::LogglyDrain;

use slog::{Drain, Logger};

fn main() {
    // Your Loggly token and tag.
    let loggly_token = "your-loggly-token";
    let loggly_tag = "some-app";

    // Create a custom Loggly drain.
    let drain = LogglyDrain::builder(loggly_token, loggly_tag)
        .debug_mode(true)
        .spawn_thread()
        .fuse();

    // Create a logger.
    let logger = Logger::root(drain, o!());

    debug!(logger, "debug"; "key" => "value");
    info!(logger, "info"; "key" => "value");
    warn!(logger, "warn"; "key" => "value");
    error!(logger, "error"; "key" => "value");

    // wait a while to let the sender thread to finish its work
    thread::sleep(Duration::from_secs(10));
}
