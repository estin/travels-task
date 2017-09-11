use std::{io, str, fmt};

use bytes::BytesMut;

use httparse;

pub struct Request {
    method: Slice,
    path: Slice,
    data: BytesMut,
}

type Slice = (usize, usize);

impl Request {
    pub fn method(&self) -> &str {
        str::from_utf8(self.slice(&self.method)).unwrap_or("")
    }

    pub fn path(&self) -> &str {
        str::from_utf8(self.slice(&self.path)).unwrap_or("")
    }

    pub fn body(&self) -> &str {
        str::from_utf8(self.data.as_ref()).unwrap().split("\r\n\r\n").nth(1).unwrap_or("")
    }

    fn slice(&self, slice: &Slice) -> &[u8] {
        &self.data[slice.0..slice.1]
    }

}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<HTTP Request {} {}>", self.method(), self.path())
    }
}

pub fn decode(buf: &mut BytesMut) -> io::Result<Option<Request>> {

    if buf.is_empty() {
        return Ok(None)
    }

    // TODO: we should grow this headers array if parsing fails and asks
    //       for more headers
    let (method, path) = {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut r = httparse::Request::new(&mut headers);
        let status = r.parse(buf).map_err(|e| {
            let msg = format!("failed to parse http request: {:?}", e);
            io::Error::new(io::ErrorKind::Other, msg)
        });

        if status.is_err() {
            return Ok(None);
        }

        match status.unwrap() {
            httparse::Status::Complete(_) => (),
            httparse::Status::Partial => return Ok(None),
        };

        let toslice = |a: &[u8]| {
            let start = a.as_ptr() as usize - buf.as_ptr() as usize;
            assert!(start < buf.len());
            (start, start + a.len())
        };

        (toslice(r.method.unwrap().as_bytes()),
         toslice(r.path.unwrap().as_bytes()))
    };

    Ok(Request {
        method: method,
        path: path,
        data: buf.take(),
    }.into())
}
