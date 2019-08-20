use futures::Future;

use serde::Serialize;
use slog::{Drain, Logger};
use slog_derive::SerdeValue;
use slog_loggly::LogglyDrain;

#[derive(Clone, Serialize, SerdeValue)]
struct Item {
    first: u32,
    second: String,
}

fn main() {
    // Your Loggly token and tag.
    let loggly_token = "your-loggly-token";
    let loggly_tag = "some-app";

    // Create a custom Loggly drain.
    let (drain, mut fhandle) = LogglyDrain::builder(loggly_token, loggly_tag)
        .debug_mode(true)
        .spawn_thread()
        .unwrap();

    // Create a logger.
    let logger = Logger::root(drain.fuse(), slog::o!());

    let item = Item {
        first: 1,
        second: "2".to_string(),
    };

    slog::debug!(logger, "debug"; "item" => item);

    // flush all log messages
    fhandle.flush().wait().unwrap();
}
