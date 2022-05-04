mod fastcgi_codec;

use muduo_rust::{Result, accept_backoff};
use crate::fastcgi_codec::FastcgiCodec;
use tokio_util::codec::Framed;

use tracing_subscriber;
use tracing::{error, info};
use clap::Parser;
use bytes::{Buf, BufMut, BytesMut};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use futures::SinkExt;
use sudoku::Sudoku;
use std::str;

const PATH: &str = "/sudoku/";
const PATH_LEN: usize = PATH.len();
const CELLS: usize = 81;

async fn run(options: Options) -> Result<()> {
    let listener = TcpListener::bind(&options.addr).await.unwrap();
    info!("Sudoku FastCGI listens on {}", options.addr);

    let mut id: i64 = 0;
    loop {
        let socket = accept_backoff(&listener).await?;
        let _ = socket.set_nodelay(true)?;
        let mut fastcgi_codec = Framed::new(socket, FastcgiCodec::new());
        id += 1;
        
        tokio::spawn(async move {
            info!("id:{} {} -> {} is UP", id, fastcgi_codec.get_ref().local_addr().unwrap(), fastcgi_codec.get_ref().peer_addr().unwrap());

            while let Some(message) = fastcgi_codec.next().await {
                match message {
                    Ok(data) => {
                        let uri = data.params.get("REQUEST_URI");
                        if uri.is_none() {
                            info!("get REQUEST_URI none");
                            break;
                        }
                        let uri = uri.unwrap();
                        info!("id:{} uri:{}", id, uri);
                       
                        for (key, value) in &data.params {
                           info!("{} = {}", key, value); 
                        }

                        if data.stdin.len() > 0 {
                            info!("stdin: {}", str::from_utf8(data.stdin.chunk()).unwrap());
                        }

                        let mut response = BytesMut::new();
                        response.put_slice("Context-Type: text/plain\r\n\r\n".as_bytes());
                        if uri.len() == CELLS + PATH_LEN && uri.contains(PATH) {
                            let sudoku = Sudoku::from_str_line(&uri.as_str()[PATH_LEN..]).unwrap();
                            if let Some(solution) = sudoku.solve_one() { 
                                info!("Solution:{}", solution.to_str_line());
                                response.put_slice(&solution.to_str_line().as_bytes());
                            } else {
                                error!("NoSolution uri:{} uri.len:{}", uri, uri.len());
                                response.put_slice("NoSolution".as_bytes());
                            }
                        } else {
                            error!("bad request uri:{} uri.len:{}", uri, uri.len());
                            response.put_slice("bad request".as_bytes());
                        }

                        let _ = fastcgi_codec.send(response.freeze()).await;
                    }
                    Err(e) => {
                        error!("an error occurred while processing messages error = {:?}", e);
                        break;
                    }
                }
            }

            info!("id:{} {} -> {} is DOWN", id, fastcgi_codec.get_ref().local_addr().unwrap(), fastcgi_codec.get_ref().peer_addr().unwrap());
        });
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    /// Sudoku address 
    #[clap(short, long)]
    #[clap(default_value_t = String::from("127.0.0.1:19981"))]
    addr: String,
}


#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let _ = run(Options::parse()).await?;
    Ok(())
}
