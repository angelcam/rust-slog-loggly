use std::{
    cell::RefCell,
    collections::HashMap,
    fmt::{self, Write},
    io,
};

use bytes::Bytes;
use slog::Key;

thread_local! {
    static TL_STRING_BUFFER: RefCell<String> = RefCell::new(String::with_capacity(128));
    static TL_BUFFER_POOL: RefCell<BufferPool> = RefCell::new(BufferPool::new());
}

/// A simple pool of string buffers. It's used to avoid excessive allocations
/// when serializing JSON messages.
struct BufferPool {
    buffers: Vec<String>,
}

impl BufferPool {
    /// Create a new empty pool of string buffers.
    fn new() -> BufferPool {
        BufferPool {
            buffers: Vec::new(),
        }
    }

    /// Take a string buffer (if available) or create a new one.
    fn take(&mut self) -> String {
        if let Some(buffer) = self.buffers.pop() {
            buffer
        } else {
            String::new()
        }
    }

    /// Put back a given string buffer.
    fn put_back(&mut self, mut buffer: String) {
        // we need to clear the buffer first
        buffer.clear();

        self.buffers.push(buffer);
    }
}

/// Slog serializer for constructing Loggly messages.
pub struct LogglyMessageSerializer {
    map: HashMap<Key, serde_json::Value>,
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
                if let serde_json::Value::String(buffer) = value {
                    bpool.put_back(buffer);
                }
            }
        });

        let res = Bytes::from(json?);

        Ok(res)
    }

    /// Emit a given serde_json::Value key-value pair.
    fn emit_serde_json_value(&mut self, key: Key, val: serde_json::Value) -> slog::Result {
        if let Some(serde_json::Value::String(old)) = self.map.insert(key, val) {
            // put back the old buffer if there is one
            TL_BUFFER_POOL.with(move |pool| {
                pool.borrow_mut().put_back(old);
            });
        }

        Ok(())
    }

    /// Emit a null key-value pair.
    fn emit_serde_json_null(&mut self, key: Key) -> slog::Result {
        self.emit_serde_json_value(key, serde_json::Value::Null)
    }

    /// Emit a boolean key-value pair.
    fn emit_serde_json_bool(&mut self, key: Key, val: bool) -> slog::Result {
        self.emit_serde_json_value(key, serde_json::Value::Bool(val))
    }

    /// Emit a numeric key-value pair.
    fn emit_serde_json_number<V>(&mut self, key: Key, value: V) -> slog::Result
    where
        serde_json::Number: From<V>,
    {
        // convert a given number into serde_json::Number
        let num = serde_json::Number::from(value);

        self.emit_serde_json_value(key, serde_json::Value::Number(num))
    }

    /// Emit a string key-value pair.
    fn emit_serde_json_string(&mut self, key: Key, val: &str) -> slog::Result {
        let mut buffer = TL_BUFFER_POOL.with(|pool| pool.borrow_mut().take());

        buffer.push_str(val);

        self.emit_serde_json_value(key, serde_json::Value::String(buffer))
    }
}

impl slog::Serializer for LogglyMessageSerializer {
    fn emit_bool(&mut self, key: Key, val: bool) -> slog::Result {
        self.emit_serde_json_bool(key, val)
    }

    fn emit_unit(&mut self, key: Key) -> slog::Result {
        self.emit_serde_json_null(key)
    }

    fn emit_char(&mut self, key: Key, val: char) -> slog::Result {
        self.emit_arguments(key, &format_args!("{}", val))
    }

    fn emit_none(&mut self, key: Key) -> slog::Result {
        self.emit_serde_json_null(key)
    }

    fn emit_u8(&mut self, key: Key, val: u8) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    fn emit_i8(&mut self, key: Key, val: i8) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    fn emit_u16(&mut self, key: Key, val: u16) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    fn emit_i16(&mut self, key: Key, val: i16) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    fn emit_usize(&mut self, key: Key, val: usize) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    fn emit_isize(&mut self, key: Key, val: isize) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    fn emit_u32(&mut self, key: Key, val: u32) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    fn emit_i32(&mut self, key: Key, val: i32) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    fn emit_f32(&mut self, key: Key, val: f32) -> slog::Result {
        self.emit_f64(key, f64::from(val))
    }

    fn emit_u64(&mut self, key: Key, val: u64) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    fn emit_i64(&mut self, key: Key, val: i64) -> slog::Result {
        self.emit_serde_json_number(key, val)
    }

    fn emit_f64(&mut self, key: Key, val: f64) -> slog::Result {
        if let Some(num) = serde_json::Number::from_f64(val) {
            self.emit_serde_json_value(key, serde_json::Value::Number(num))
        } else {
            self.emit_serde_json_null(key)
        }
    }

    fn emit_str(&mut self, key: Key, val: &str) -> slog::Result {
        self.emit_serde_json_string(key, val)
    }

    fn emit_arguments(&mut self, key: Key, val: &fmt::Arguments) -> slog::Result {
        TL_STRING_BUFFER.with(|buf| {
            let mut buf = buf.borrow_mut();

            buf.write_fmt(*val).unwrap();

            let res = self.emit_serde_json_string(key, &*buf);

            buf.clear();

            res
        })
    }
}

#[cfg(test)]
mod tests {
    use slog::{Key, Serializer};
    use std::collections::HashMap;

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

        expected.insert(Key::from("key"), serde_json::Value::Bool(true));

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

    /// Test that different types are serialized correctly.
    #[test]
    fn test_serialized_values() {
        // test string serialization
        let mut serializer = LogglyMessageSerializer::new();
        serializer.emit_str(Key::from("key"), "val").unwrap();
        let result = serializer.finish().unwrap();
        let result = String::from_utf8(result.to_vec()).unwrap();
        assert_eq!(&result, "{\"key\":\"val\"}");

        // test char serialization
        let mut serializer = LogglyMessageSerializer::new();
        serializer.emit_char(Key::from("key"), 'v').unwrap();
        let result = serializer.finish().unwrap();
        let result = String::from_utf8(result.to_vec()).unwrap();
        assert_eq!(&result, "{\"key\":\"v\"}");

        // test null serialization
        let mut serializer = LogglyMessageSerializer::new();
        serializer.emit_unit(Key::from("key")).unwrap();
        let result = serializer.finish().unwrap();
        let result = String::from_utf8(result.to_vec()).unwrap();
        assert_eq!(&result, "{\"key\":null}");

        // test bool serialization
        let mut serializer = LogglyMessageSerializer::new();
        serializer.emit_bool(Key::from("key"), true).unwrap();
        let result = serializer.finish().unwrap();
        let result = String::from_utf8(result.to_vec()).unwrap();
        assert_eq!(&result, "{\"key\":true}");

        // test integer serialization
        let mut serializer = LogglyMessageSerializer::new();
        serializer.emit_i32(Key::from("key"), -5).unwrap();
        let result = serializer.finish().unwrap();
        let result = String::from_utf8(result.to_vec()).unwrap();
        assert_eq!(&result, "{\"key\":-5}");

        // test float serialization
        let mut serializer = LogglyMessageSerializer::new();
        serializer.emit_f64(Key::from("key"), 1.0).unwrap();
        let result = serializer.finish().unwrap();
        let result = String::from_utf8(result.to_vec()).unwrap();
        assert_eq!(&result, "{\"key\":1.0}");
    }
}
