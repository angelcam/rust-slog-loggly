use slog::{debug, error, info, o, warn, Drain, Logger};
use slog_loggly::LogglyDrain;

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
    let logger = Logger::root(drain.fuse(), o!());

    debug!(logger, "debug"; "key" => "value");
    info!(logger, "info"; "key" => "value");
    warn!(logger, "warn"; "key" => "value");
    error!(logger, "error"; "key" => "value");

    // flush all log messages
    fhandle.blocking_flush().unwrap();
}
