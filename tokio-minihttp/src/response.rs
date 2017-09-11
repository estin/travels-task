use std::fmt::{self, Write};

use bytes::{BytesMut, BufMut};

pub struct Response {
    headers: Vec<(String, String)>,
    response: String,
    length: usize,
    status_message: StatusMessage,
}

enum StatusMessage {
    Ok,
    Custom(u32, String)
}

impl Response {
    pub fn new() -> Response {
        Response {
            headers: Vec::new(),
            response: String::new(),
            status_message: StatusMessage::Ok,
            length: 0,
        }
    }

    pub fn status_code(&mut self, code: u32, message: &str) -> &mut Response {
        self.status_message = StatusMessage::Custom(code, message.to_string());
        self
    }

    pub fn header(&mut self, name: &str, val: &str) -> &mut Response {
        self.headers.push((name.to_string(), val.to_string()));
        self
    }

    pub fn body(&mut self, length: usize, s: &str) -> &mut Response {
        self.length = length;
        self.response = s.to_string();
        self
    }
}

pub fn encode(msg: Response, buf: &mut BytesMut) {
    write!(FastWrite(buf), "\
        HTTP/1.1 {}\r\n\
        Server: Travels\r\n\
        Content-Length: {}\r\n\
        Content-Type: application/json\r\n\
    ", msg.status_message, msg.length).unwrap();

    // Connection: keep-alive\r\n\
    // Connection: close\r\n\

    push(buf, "\r\n".as_bytes());
    push(buf, msg.response.as_bytes());
}

fn push(buf: &mut BytesMut, data: &[u8]) {
    buf.reserve(data.len());
    unsafe {
        buf.bytes_mut()[..data.len()].copy_from_slice(data);
        buf.advance_mut(data.len());
    }
}

// TODO: impl fmt::Write for Vec<u8>
//
// Right now `write!` on `Vec<u8>` goes through io::Write and is not super
// speedy, so inline a less-crufty implementation here which doesn't go through
// io::Error.
struct FastWrite<'a>(&'a mut BytesMut);

impl<'a> fmt::Write for FastWrite<'a> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        push(&mut *self.0, s.as_bytes());
        Ok(())
    }

    fn write_fmt(&mut self, args: fmt::Arguments) -> fmt::Result {
        fmt::write(self, args)
    }
}

impl fmt::Display for StatusMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            StatusMessage::Ok => f.pad("200 OK"),
            StatusMessage::Custom(c, ref s) => write!(f, "{} {}", c, s),
        }
    }
}
