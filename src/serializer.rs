use std::fmt;
use std::io;

use std::cell::RefCell;
use std::fmt::Write;

use serde;

use serde::ser::SerializeMap;

use slog;

use slog::Key;

thread_local! {
    static TL_BUFFER: RefCell<String> = RefCell::new(String::with_capacity(128))
}

/// Slog serializer for constructing Loggly messages.
pub struct LogglyMessageSerializer<S: serde::Serializer> {
    map: S::SerializeMap,
}

impl<S> LogglyMessageSerializer<S>
where
    S: serde::Serializer,
{
    /// Create a new serializer wrapping a given Serde serialize.
    pub fn new(serializer: S) -> slog::Result<LogglyMessageSerializer<S>> {
        let map = serializer.serialize_map(None).map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "unable to serialize a log message")
        })?;

        let res = LogglyMessageSerializer { map: map };

        Ok(res)
    }

    /// Finish the message.
    pub fn finish(self) -> slog::Result<S::Ok> {
        let res = self.map
            .end()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "unable to finalize a log message"))?;

        Ok(res)
    }

    /// Emit a given key-value pair.
    fn emit<V>(&mut self, key: Key, value: &V) -> slog::Result
    where
        V: serde::Serialize,
    {
        let key: &str = key.as_ref();

        self.map.serialize_entry(key, value).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "unable to serialize a log message entry",
            )
        })?;

        Ok(())
    }
}

impl<S> slog::Serializer for LogglyMessageSerializer<S>
where
    S: serde::Serializer,
{
    fn emit_bool(&mut self, key: Key, val: bool) -> slog::Result {
        self.emit(key, &val)
    }

    fn emit_unit(&mut self, key: Key) -> slog::Result {
        self.emit(key, &())
    }

    fn emit_char(&mut self, key: Key, val: char) -> slog::Result {
        self.emit(key, &val)
    }

    fn emit_none(&mut self, key: Key) -> slog::Result {
        self.emit(key, &None as &Option<()>)
    }

    fn emit_u8(&mut self, key: Key, val: u8) -> slog::Result {
        self.emit(key, &val)
    }

    fn emit_i8(&mut self, key: Key, val: i8) -> slog::Result {
        self.emit(key, &val)
    }

    fn emit_u16(&mut self, key: Key, val: u16) -> slog::Result {
        self.emit(key, &val)
    }

    fn emit_i16(&mut self, key: Key, val: i16) -> slog::Result {
        self.emit(key, &val)
    }

    fn emit_usize(&mut self, key: Key, val: usize) -> slog::Result {
        self.emit(key, &val)
    }

    fn emit_isize(&mut self, key: Key, val: isize) -> slog::Result {
        self.emit(key, &val)
    }

    fn emit_u32(&mut self, key: Key, val: u32) -> slog::Result {
        self.emit(key, &val)
    }

    fn emit_i32(&mut self, key: Key, val: i32) -> slog::Result {
        self.emit(key, &val)
    }

    fn emit_f32(&mut self, key: Key, val: f32) -> slog::Result {
        self.emit(key, &val)
    }

    fn emit_u64(&mut self, key: Key, val: u64) -> slog::Result {
        self.emit(key, &val)
    }

    fn emit_i64(&mut self, key: Key, val: i64) -> slog::Result {
        self.emit(key, &val)
    }

    fn emit_f64(&mut self, key: Key, val: f64) -> slog::Result {
        self.emit(key, &val)
    }

    fn emit_str(&mut self, key: Key, val: &str) -> slog::Result {
        self.emit(key, &val)
    }

    fn emit_arguments(&mut self, key: Key, val: &fmt::Arguments) -> slog::Result {
        TL_BUFFER.with(|buf| {
            let mut buf = buf.borrow_mut();

            buf.write_fmt(*val).unwrap();

            let res = self.emit(key, &*buf);

            buf.clear();

            res
        })
    }

    #[cfg(feature = "nested-values")]
    fn emit_serde(&mut self, key: Key, value: &slog::SerdeValue) -> slog::Result {
        self.emit(key, val.as_serde())
    }
}
