use std::fmt;
use std::io;

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Write;

use bytes::Bytes;

use serde;
use serde_json;

use slog;

use slog::Key;

thread_local! {
    static TL_STRING_BUFFER: RefCell<String> = RefCell::new(String::with_capacity(128));
    static TL_BUFFER_POOL: RefCell<BufferPool> = RefCell::new(BufferPool::new());
}

/// A simple pool of byte vectors. It's used to avoid excessive allocations
/// when serializing JSON messages.
struct BufferPool {
    buffers: Vec<Vec<u8>>,
}

impl BufferPool {
    /// Create a new empty pool of byte vectors.
    fn new() -> BufferPool {
        BufferPool {
            buffers: Vec::new(),
        }
    }

    /// Take a byte vector (if available) or create a new one.
    fn take(&mut self) -> Vec<u8> {
        if let Some(buffer) = self.buffers.pop() {
            buffer
        } else {
            Vec::new()
        }
    }

    /// Put back a given byte vector.
    fn put_back(&mut self, mut buffer: Vec<u8>) {
        // we need to clear the buffer first
        buffer.clear();

        self.buffers.push(buffer);
    }
}

/// Slog serializer for constructing Loggly messages.
pub struct LogglyMessageSerializer {
    map: HashMap<Key, String>,
}

impl LogglyMessageSerializer {
    /// Create a new serializer.
    pub fn new() -> LogglyMessageSerializer {
        LogglyMessageSerializer {
            map: HashMap::new(),
        }
    }

    /// Finish the message.
    pub fn finish(self) -> slog::Result<Bytes> {
        let json = serde_json::to_vec(&self.map)
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "unable to finalize a log message"));

        TL_BUFFER_POOL.with(move |pool| {
            let mut bpool = pool.borrow_mut();

            for (_, value) in self.map.into_iter() {
                bpool.put_back(value.into_bytes());
            }
        });

        let res = Bytes::from(json?);

        Ok(res)
    }

    /// Emit a given key-value pair.
    fn emit<V>(&mut self, key: Key, value: &V) -> slog::Result
    where
        V: serde::Serialize,
    {
        let mut buffer = TL_BUFFER_POOL.with(|pool| pool.borrow_mut().take());

        if let Err(_) = serde_json::to_writer(&mut buffer, value) {
            // put back the buffer if we were not able to serialize the value
            TL_BUFFER_POOL.with(move |pool| {
                pool.borrow_mut().put_back(buffer);
            });

            return Err(slog::Error::from(io::Error::new(
                io::ErrorKind::Other,
                "unable to serialize a log message entry value",
            )));
        }

        // this is safe because the serialized JSON is a valid UTF-8 string
        let value = unsafe { String::from_utf8_unchecked(buffer) };

        if let Some(old) = self.map.insert(key, value) {
            // put back the old buffer if there is one
            TL_BUFFER_POOL.with(move |pool| {
                pool.borrow_mut().put_back(old.into_bytes());
            });
        }

        Ok(())
    }
}

impl slog::Serializer for LogglyMessageSerializer {
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
        TL_STRING_BUFFER.with(|buf| {
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use slog::{Key, Serializer};

    use super::*;

    /// Test that there will be only the last provided value in the serialized
    /// JSON if there were multiple key-value pairs having the same key.
    #[test]
    fn test_duplicate_keys() {
        let mut serializer = LogglyMessageSerializer::new();

        serializer.emit_str(Key::from("key"), "value").unwrap();
        serializer.emit_u32(Key::from("key"), 1).unwrap();
        serializer.emit_bool(Key::from("key"), true).unwrap();

        let mut expected = HashMap::new();

        expected.insert(Key::from("key"), true.to_string());

        assert_eq!(serializer.map, expected);
    }

    /// Test that the serialization buffers get reused.
    #[test]
    fn test_buffer_reuse() {
        let mut serializer = LogglyMessageSerializer::new();

        serializer.emit_str(Key::from("key_1"), "value").unwrap();
        serializer.emit_u32(Key::from("key_1"), 1).unwrap();
        serializer.emit_bool(Key::from("key_1"), true).unwrap();
        serializer.emit_str(Key::from("key_2"), "value").unwrap();
        serializer.emit_str(Key::from("key_3"), "value").unwrap();

        serializer.finish().unwrap();

        // this time there will be no allocations except the hash map and the
        // returned message
        let mut serializer = LogglyMessageSerializer::new();

        serializer.emit_str(Key::from("key_1"), "value").unwrap();
        serializer.emit_str(Key::from("key_2"), "value").unwrap();
        serializer.emit_str(Key::from("key_3"), "value").unwrap();

        serializer.finish().unwrap();

        let buffers = TL_BUFFER_POOL.with(|pool| pool.borrow().buffers.len());

        assert_eq!(buffers, 3);
    }
}
