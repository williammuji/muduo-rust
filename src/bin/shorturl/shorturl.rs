use std::collections::HashMap;
use tracing_subscriber;
use tracing::info;
use chrono::{DateTime, Utc};
use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Method, Request, Response, Server, StatusCode};
use muduo_rust::{Error, Result};

static MOVED_PERMANENTLY: &[u8] = b"Moved Permanently";
static NOT_FOUND: &[u8] = b"Not Found";

const FAVICON: &'static [u8] = &[ 
  b'\x89', b'P', b'N', b'G', b'\x0D', b'\x0A', b'\x1A', b'\x0A',
  b'\x00', b'\x00', b'\x00', b'\x0D', b'I', b'H', b'D', b'R',
  b'\x00', b'\x00', b'\x00', b'\x10', b'\x00', b'\x00', b'\x00', b'\x10',
  b'\x08', b'\x06', b'\x00', b'\x00', b'\x00', b'\x1F', b'\xF3', b'\xFF',
  b'a', b'\x00', b'\x00', b'\x00', b'\x19', b't', b'E', b'X',
  b't', b'S', b'o', b'f', b't', b'w', b'a', b'r',
  b'e', b'\x00', b'A', b'd', b'o', b'b', b'e', b'\x20',
  b'I', b'm', b'a', b'g', b'e', b'R', b'e', b'a',
  b'd', b'y', b'q', b'\xC9', b'e', b'\x3C', b'\x00', b'\x00',
  b'\x01', b'\xCD', b'I', b'D', b'A', b'T', b'x', b'\xDA',
  b'\x94', b'\x93', b'9', b'H', b'\x03', b'A', b'\x14', b'\x86',
  b'\xFF', b'\x5D', b'b', b'\xA7', b'\x04', b'R', b'\xC4', b'm',
  b'\x22', b'\x1E', b'\xA0', b'F', b'\x24', b'\x08', b'\x16', b'\x16',
  b'v', b'\x0A', b'6', b'\xBA', b'J', b'\x9A', b'\x80', b'\x08',
  b'A', b'\xB4', b'q', b'\x85', b'X', b'\x89', b'G', b'\xB0',
  b'I', b'\xA9', b'Q', b'\x24', b'\xCD', b'\xA6', b'\x08', b'\xA4',
  b'H', b'c', b'\x91', b'B', b'\x0B', b'\xAF', b'V', b'\xC1',
  b'F', b'\xB4', b'\x15', b'\xCF', b'\x22', b'X', b'\x98', b'\x0B',
  b'T', b'H', b'\x8A', b'd', b'\x93', b'\x8D', b'\xFB', b'F',
  b'g', b'\xC9', b'\x1A', b'\x14', b'\x7D', b'\xF0', b'f', b'v',
  b'f', b'\xDF', b'\x7C', b'\xEF', b'\xE7', b'g', b'F', b'\xA8',
  b'\xD5', b'j', b'H', b'\x24', b'\x12', b'\x2A', b'\x00', b'\x05',
  b'\xBF', b'G', b'\xD4', b'\xEF', b'\xF7', b'\x2F', b'6', b'\xEC',
  b'\x12', b'\x20', b'\x1E', b'\x8F', b'\xD7', b'\xAA', b'\xD5', b'\xEA',
  b'\xAF', b'I', b'5', b'F', b'\xAA', b'T', b'\x5F', b'\x9F',
  b'\x22', b'A', b'\x2A', b'\x95', b'\x0A', b'\x83', b'\xE5', b'r',
  b'9', b'd', b'\xB3', b'Y', b'\x96', b'\x99', b'L', b'\x06',
  b'\xE9', b't', b'\x9A', b'\x25', b'\x85', b'\x2C', b'\xCB', b'T',
  b'\xA7', b'\xC4', b'b', b'1', b'\xB5', b'\x5E', b'\x00', b'\x03',
  b'h', b'\x9A', b'\xC6', b'\x16', b'\x82', b'\x20', b'X', b'R',
  b'\x14', b'E', b'6', b'S', b'\x94', b'\xCB', b'e', b'x',
  b'\xBD', b'\x5E', b'\xAA', b'U', b'T', b'\x23', b'L', b'\xC0',
  b'\xE0', b'\xE2', b'\xC1', b'\x8F', b'\x00', b'\x9E', b'\xBC', b'\x09',
  b'A', b'\x7C', b'\x3E', b'\x1F', b'\x83', b'D', b'\x22', b'\x11',
  b'\xD5', b'T', b'\x40', b'\x3F', b'8', b'\x80', b'w', b'\xE5',
  b'3', b'\x07', b'\xB8', b'\x5C', b'\x2E', b'H', b'\x92', b'\x04',
  b'\x87', b'\xC3', b'\x81', b'\x40', b'\x20', b'\x40', b'g', b'\x98',
  b'\xE9', b'6', b'\x1A', b'\xA6', b'g', b'\x15', b'\x04', b'\xE3',
  b'\xD7', b'\xC8', b'\xBD', b'\x15', b'\xE1', b'i', b'\xB7', b'C',
  b'\xAB', b'\xEA', b'x', b'\x2F', b'j', b'X', b'\x92', b'\xBB',
  b'\x18', b'\x20', b'\x9F', b'\xCF', b'3', b'\xC3', b'\xB8', b'\xE9',
  b'N', b'\xA7', b'\xD3', b'l', b'J', b'\x00', b'i', b'6',
  b'\x7C', b'\x8E', b'\xE1', b'\xFE', b'V', b'\x84', b'\xE7', b'\x3C',
  b'\x9F', b'r', b'\x2B', b'\x3A', b'B', b'\x7B', b'7', b'f',
  b'w', b'\xAE', b'\x8E', b'\x0E', b'\xF3', b'\xBD', b'R', b'\xA9',
  b'd', b'\x02', b'B', b'\xAF', b'\x85', b'2', b'f', b'F',
  b'\xBA', b'\x0C', b'\xD9', b'\x9F', b'\x1D', b'\x9A', b'l', b'\x22',
  b'\xE6', b'\xC7', b'\x3A', b'\x2C', b'\x80', b'\xEF', b'\xC1', b'\x15',
  b'\x90', b'\x07', b'\x93', b'\xA2', b'\x28', b'\xA0', b'S', b'j',
  b'\xB1', b'\xB8', b'\xDF', b'\x29', b'5', b'C', b'\x0E', b'\x3F',
  b'X', b'\xFC', b'\x98', b'\xDA', b'y', b'j', b'P', b'\x40',
  b'\x00', b'\x87', b'\xAE', b'\x1B', b'\x17', b'B', b'\xB4', b'\x3A',
  b'\x3F', b'\xBE', b'y', b'\xC7', b'\x0A', b'\x26', b'\xB6', b'\xEE',
  b'\xD9', b'\x9A', b'\x60', b'\x14', b'\x93', b'\xDB', b'\x8F', b'\x0D',
  b'\x0A', b'\x2E', b'\xE9', b'\x23', b'\x95', b'\x29', b'X', b'\x00',
  b'\x27', b'\xEB', b'n', b'V', b'p', b'\xBC', b'\xD6', b'\xCB',
  b'\xD6', b'G', b'\xAB', b'\x3D', b'l', b'\x7D', b'\xB8', b'\xD2',
  b'\xDD', b'\xA0', b'\x60', b'\x83', b'\xBA', b'\xEF', b'\x5F', b'\xA4',
  b'\xEA', b'\xCC', b'\x02', b'N', b'\xAE', b'\x5E', b'p', b'\x1A',
  b'\xEC', b'\xB3', b'\x40', b'9', b'\xAC', b'\xFE', b'\xF2', b'\x91',
  b'\x89', b'g', b'\x91', b'\x85', b'\x21', b'\xA8', b'\x87', b'\xB7',
  b'X', b'\x7E', b'\x7E', b'\x85', b'\xBB', b'\xCD', b'N', b'N',
  b'b', b't', b'\x40', b'\xFA', b'\x93', b'\x89', b'\xEC', b'\x1E',
  b'\xEC', b'\x86', b'\x02', b'H', b'\x26', b'\x93', b'\xD0', b'u',
  b'\x1D', b'\x7F', b'\x09', b'2', b'\x95', b'\xBF', b'\x1F', b'\xDB',
  b'\xD7', b'c', b'\x8A', b'\x1A', b'\xF7', b'\x5C', b'\xC1', b'\xFF',
  b'\x22', b'J', b'\xC3', b'\x87', b'\x00', b'\x03', b'\x00', b'K',
  b'\xBB', b'\xF8', b'\xD6', b'\x2A', b'v', b'\x98', b'I', b'\x00',
  b'\x00', b'\x00', b'\x00', b'I', b'E', b'N', b'D', b'\xAE',
  b'B', b'\x60', b'\x82'
];

async fn on_request(
    req: Request<Body>,
    redirections: HashMap<String, String>,
) -> Result<Response<Body>> {
    info!("method:{} path:{}", req.method(), req.uri().path());
    
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/1") | (&Method::GET, "/2") => {
            let response = Response::builder()
                .status(StatusCode::MOVED_PERMANENTLY)
                .header("Location", redirections.get(req.uri().path()).unwrap())
                .body(MOVED_PERMANENTLY.into())?;
            Ok(response)
        },
        (&Method::GET, "/") => {
            let mut resp = String::new();
            resp.push_str("<html><head><title>My tiny short url service</title></head>");
            resp.push_str("<body><h1>Known redirections</h1>");
            for (path, url) in redirections {
                resp.push_str(&format!("<ul>{} =&gt; {}</ul>", path, url));
            }
            let now: DateTime<Utc> = Utc::now();
            resp.push_str(&format!("Now is {}</body></html>", now.to_rfc2822()));

            let response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/html")
                .body(resp.into())?;
            Ok(response)
        },
        (&Method::GET, "/favicon.ico") => {
            let response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "image/png")
                .body(FAVICON.into())?;
            Ok(response)
        },
        _ => {
            // Return 404 not found response.
            let response = Response::builder()
               .status(StatusCode::NOT_FOUND)
               .body(NOT_FOUND.into())?;
            Ok(response)
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:3000".parse().unwrap();

    let mut redirections = HashMap::new();
    redirections.insert("/1".to_string(), "http://chenshuo.com".to_string());
    redirections.insert("/2".to_string(), "http://blog.csdn.net/Solstice".to_string());

    let new_service = make_service_fn(move |_| {
        let redirections = redirections.clone();

        async {
            Ok::<_, Error>(service_fn(move |req| {
                on_request(req, redirections.to_owned())
            }))
        }
    });

    let server = Server::bind(&addr).serve(new_service);

    println!("Listening on http://{}", addr);

    server.await?;

    Ok(())
}
