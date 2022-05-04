use tokio_util::codec::{Decoder, Encoder};
use tracing::{info, error};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::error::Error as StdError;
use std::io::{self, Cursor};
use std::fmt;
use std::collections::HashMap;
use std::mem;
use std::str;

const MAX_FRAME_LEN: usize = 64 * 1_024;
const PADDING: &'static [u8; 8] = &[b'\0'; 8];
const FCGI_KEEP_CONN: u8 = 1;

enum FcgiType {
    FcgiInvalid = 0,
    FcgiBeginRequest = 1,
    FcgiAbortRequest = 2,
    FcgiEndRequest = 3,
    FcgiParams = 4,
    FcgiStdin = 5,
    FcgiStdout = 6,
    FcgiStderr = 7,
    FcgiData = 8,
    FcgiGetValues = 9,
    FcgiGetValuesResult = 10,
}

impl FcgiType {
    fn from_u8(value: u8) -> FcgiType {
        match value {
            0 => FcgiType::FcgiInvalid,
            1 => FcgiType::FcgiBeginRequest,
            2 => FcgiType::FcgiAbortRequest,
            3 => FcgiType::FcgiEndRequest,
            4 => FcgiType::FcgiParams,
            5 => FcgiType::FcgiStdin,
            6 => FcgiType::FcgiStdout,
            7 => FcgiType::FcgiStderr,
            8 => FcgiType::FcgiData,
            9 => FcgiType::FcgiGetValues,
            10 => FcgiType::FcgiGetValuesResult,
            _ => panic!("Unknown value: {}", value),
        }
    }
}

enum FcgiRole {
    FcgiResponder = 1,
    _FcgiAuthorizer = 2,
}

#[derive(Debug, Clone, Copy)]
struct RecordHeader {
    version: u8,
    the_type: u8,
    id: u16,
    length: u16,
    padding: u8,
    unused: u8,
}

impl RecordHeader {
    fn new() -> RecordHeader {
        RecordHeader {
            version: 0,
            the_type: 0,
            id: 0,
            length: 0,
            padding: 0,
            unused: 0,
        }
    }
}

const RECORD_HEADER_LEN: usize = mem::size_of::<RecordHeader>();

pub struct FastcgiCodecError {
    _priv: (),
}

#[derive(Debug, Clone)]
pub struct FastcgiData {
    pub params: HashMap<String, String>,
    pub stdin: BytesMut,
}

impl FastcgiData {
    fn new() -> FastcgiData {
        FastcgiData {
            params: HashMap::new(),
            stdin: BytesMut::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FastcgiCodec {
    // Read state
    state: DecodeState,

    header: RecordHeader,

    params_stream: BytesMut,
    got_request: bool,
    keep_conn: bool,

    data: Option<FastcgiData>,
}

#[derive(Debug, Clone, Copy)]
enum DecodeState {
    Head,
    Data(usize),
    Shutdown,
}

// ===== impl FastcgiCodec ======

impl FastcgiCodec {
    pub fn new() -> Self {
        Self {
            state: DecodeState::Head,

            header: RecordHeader::new(),

            params_stream: BytesMut::new(),
            got_request: false,
            keep_conn: false,
    
            data: None,
        }
    }

    fn decode_begin_request(&mut self, n: usize, src: &mut BytesMut) -> bool {
        info!("decode_begin_request n:{} src:{}", n, src.len());
        let mut read_src = Cursor::new(&mut *src);
        let role = u16::from_be_bytes([read_src.chunk()[0], read_src.chunk()[1]]);
        read_src.advance(2);
        let flags = read_src.get_u8();
        src.advance(n);
        info!("decode_begin_request role:{} flags:{}", role, flags);
        
        if role == FcgiRole::FcgiResponder as u16 {
            self.keep_conn = flags == FCGI_KEEP_CONN;
            true
        } else {
            false
        }
    }

    fn read_len(&mut self) -> u32 {
        if self.params_stream.len() >= 1 {
            let byte = self.params_stream.chunk()[0];
            if byte & 0x80 > 0 {
                if self.params_stream.len() >= mem::size_of::<u32>() {
                    return self.params_stream.get_u32() & 0x7fffffff;
                } else {
                    return u32::MAX;
                }
            } else {
                return self.params_stream.get_u8() as u32;
            }
        } else {
            return u32::MAX;
        }
    }

    fn parse_all_params(&mut self) -> bool {
        while self.params_stream.len() > 0 {
            let name_len: u32 = self.read_len();
            if name_len == u32::MAX {
                return false;
            }
            let value_len: u32 = self.read_len();
            if value_len == u32::MAX {
                return false;
            }

            if self.params_stream.len() >= (name_len + value_len) as usize {
                if let Ok(name) = utf8(&self.params_stream.chunk()[0..name_len as usize]) {
                    if let Ok(value) = utf8(&self.params_stream.chunk()[name_len as usize .. (name_len+value_len) as usize]) {
                        if self.data.is_none() {
                            self.data = Some(FastcgiData::new());
                        }
                        if let Some(ref mut data) = self.data {
                            data.params.insert(name.to_string(), value.to_string());
                        }
                    }
                }

                self.params_stream.advance((name_len + value_len) as usize);
            } else {
                return false;
            }
        }
        true
    }

    fn decode_params(&mut self, n: usize, src: &mut BytesMut) -> bool {
        info!("decode_params n:{} src:{}", n, src.len());
        if n > 0 {
            self.params_stream.put_slice(&src.chunk()[0..n]);
        } else if !self.parse_all_params() {
            error!("parse_all_params failed");
            return false;
        }

        src.advance(n);

        true
    }
    
    fn decode_stdin(&mut self, n: usize, src: &mut BytesMut) {
        info!("decode_params n:{} src:{}", n, src.len());
        if n > 0 {
            if self.data.is_none() {
                self.data = Some(FastcgiData::new());
            }
            if let Some(ref mut data) = self.data {
                data.stdin.put_slice(&src.chunk()[0..n]); 
            }
        } else {
            self.got_request = true; 
        }

        src.advance(n);
    }

    fn encode_header(&mut self, header: RecordHeader, dst: &mut BytesMut) {
        dst.put_u8(header.version);
        dst.put_u8(header.the_type);
        dst.put_slice(&header.id.to_be_bytes());
        dst.put_slice(&header.length.to_be_bytes());
        dst.put_u8(header.padding);
        dst.put_u8(header.unused);
    }

    fn end_stdout(&mut self, dst: &mut BytesMut) {
        let header = RecordHeader {
            version: 1,
            the_type: FcgiType::FcgiStdout as u8,
            id: 1,
            length: 0,
            padding: 0,
            unused: 0,
        };
        self.encode_header(header, dst);
    }

    fn end_request(&mut self, dst: &mut BytesMut) {
        let header = RecordHeader {
            version: 1,
            the_type: FcgiType::FcgiEndRequest as u8,
            id: 1,
            length: RECORD_HEADER_LEN as u16,
            padding: 0,
            unused: 0,
        };
        self.encode_header(header, dst);
        dst.put_u32(0);
        dst.put_u32(0);
    }

    fn decode_head(&mut self, src: &mut BytesMut) -> io::Result<Option<usize>> {
        info!("decode_head src:{}", src.len());
        if src.len() < RECORD_HEADER_LEN {
            // Not enough data
            return Ok(None);
        }

        self.header.version = src.get_u8();
        self.header.the_type = src.get_u8();
        self.header.id = u16::from_be_bytes([src.chunk()[0], src.chunk()[1]]);
        src.advance(2);
        self.header.length = u16::from_be_bytes([src.chunk()[0], src.chunk()[1]]);
        src.advance(2);
        self.header.padding = src.get_u8();
        self.header.unused = src.get_u8();
        info!("decode_head header:{:?}", self.header);

        let n = self.header.length as usize + self.header.padding as usize;
        if n > MAX_FRAME_LEN {
            return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    FastcgiCodecError { _priv: () },
                    ));
        }

        // Ensure that the buffer has enough space to read the incoming
        // payload
        if n > 0 {
            src.reserve(n);
        }

        Ok(Some(n))
    }

    fn decode_data(&mut self, n: usize, src: &mut BytesMut) -> io::Result<Option<FastcgiData>> {
        info!("decode_data n:{} src:{}", n, src.len());
        // At this point, the buffer has already had the required capacity
        // reserved. All there is to do is read.
        if src.len() < n {
            return Ok(None);
        }

        let mut piece_n = n;

        loop {
            let res = self.decode_data_piece(piece_n, src)?;
            match res {
                None => {
                    piece_n = {
                        match self.decode_head(src)? {
                            Some(n) => {
                                n
                            }
                            None => return Ok(None),
                        }
                    };
                }
                Some(data) => {
                    return Ok(Some(data));
                }
            }
        }
    }

    fn decode_data_piece(&mut self, n: usize, src: &mut BytesMut) -> io::Result<Option<FastcgiData>> {
        info!("decode_data_piece n:{} src:{}", n, src.len());
        // At this point, the buffer has already had the required capacity
        // reserved. All there is to do is read.
        if src.len() < n {
            return Ok(None);
        }

        match FcgiType::from_u8(self.header.the_type) {
            FcgiType::FcgiBeginRequest => {
                let _ = self.decode_begin_request(n, src);
                self.state = DecodeState::Head;
                src.reserve(RECORD_HEADER_LEN);
                return Ok(None);
            },
            FcgiType::FcgiParams => {
                let _ = self.decode_params(n, src);
                self.state = DecodeState::Head;
                src.reserve(RECORD_HEADER_LEN);
                return Ok(None);
            },
            FcgiType::FcgiStdin => {
                self.decode_stdin(n, src);
                if !self.got_request {
                    self.state = DecodeState::Head;
                    src.reserve(RECORD_HEADER_LEN);
                    return Ok(None);
                } else {
                    self.params_stream.clear();
                    self.got_request = false;
               
                    if self.keep_conn {
                        self.state = DecodeState::Head;
                        src.reserve(RECORD_HEADER_LEN);
                        info!("decode_data FcgiStdin keep_conn");
                    } else {
                        self.state = DecodeState::Shutdown;
                        info!("decode_data FcgiStdin shutdown");
                    }

                    return Ok(self.data.take());
                }
            },
            _ => {
                info!("decode_data unknown");
                // FIXME
                return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        FastcgiCodecError { _priv: () },
                        ))
            }
        }
    }
}

impl Decoder for FastcgiCodec {
    type Item = FastcgiData;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<FastcgiData>> {
        let n = match self.state {
            DecodeState::Head => match self.decode_head(src)? {
                Some(n) => {
                    self.state = DecodeState::Data(n);
                    n
                }
                None => return Ok(None),
            },
            DecodeState::Data(n) => n,
            DecodeState::Shutdown => {
                return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "Shutdown"
                        ));
            }
        };

        return self.decode_data(n, src);
    }
}

impl Encoder<Bytes> for FastcgiCodec {
    type Error = io::Error;

    fn encode(&mut self, data: Bytes, dst: &mut BytesMut) -> Result<(), io::Error> {
        let n = data.len();

        if n > MAX_FRAME_LEN {
            return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    FastcgiCodecError { _priv: () },
                    ));
        }

        let mut padding = n as i32;
        padding = -padding & 7;

        let header = RecordHeader {
            version: 1,
            the_type: FcgiType::FcgiStdout as u8,
            id: 1,
            length: n as u16,
            padding: padding as u8,    // FIXME
            unused: 0,
        };
        
        let capacity: usize = header.length as usize + header.padding as usize + RECORD_HEADER_LEN + RECORD_HEADER_LEN + 2*mem::size_of::<u32>();
        dst.reserve(capacity);

        self.encode_header(header, dst);
        dst.put(data);
        dst.put(&PADDING[..header.padding as usize]);

        self.end_stdout(dst);
        self.end_request(dst);

        Ok(()) 
    }
}

impl Default for FastcgiCodec {
    fn default() -> Self {
        Self::new()
    }
}

// ===== impl FastcgiCodecError =====

impl fmt::Debug for FastcgiCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FastcgiCodecError").finish()
    }
}

impl fmt::Display for FastcgiCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("frame size too big")
    }
}

impl StdError for FastcgiCodecError {}


fn utf8(buf: &[u8]) -> Result<&str, io::Error> {
    str::from_utf8(buf)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Unable to decode input as UTF8"))
}
