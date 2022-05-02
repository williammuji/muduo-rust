use muduo_rust::{CountdownLatch, Result};
use clap::Parser;
use tracing_subscriber;
use tracing::{info, error};
use tokio::net::TcpStream;
use std::time::Instant;
use rand::Rng;
use md5::{Md5, Digest};
use tokio_util::codec::{Framed, BytesCodec};
use tokio_stream::StreamExt;

const MB: usize = 1024 * 1024;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    /// Host Addr
    #[clap(short, long, default_value_t = String::from("127.0.0.1:2021"))]
    addr: String,

    /// Clients 
    #[clap(short, long, default_value_t = 500)]
    clients: usize,
    
    /// Min Length 
    #[clap(long, default_value_t = 1)]
    min_length: usize,
    
    /// Max Length 
    #[clap(long, default_value_t = 6)]
    max_length: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let options = Options::parse();
    assert!(options.min_length <= options.max_length);

    let start = Instant::now();
    
    let latch = CountdownLatch::new(options.clients as i32);

    let mut rng = rand::thread_rng();
    let variance = rng.gen_range(0..(options.max_length*MB - options.min_length*MB+ 1));
    let max_length = options.min_length + variance;

    for id in 0..options.clients {
        let address = options.addr.clone();
        let latch = latch.clone();
     
        tokio::spawn(async move {
            let stream = TcpStream::connect(address).await.unwrap();
            let _ = stream.set_nodelay(true).unwrap();
            let mut bytes_codec = Framed::new(stream, BytesCodec::new());
            info!("id:{} {} -> {} is UP", id, bytes_codec.get_ref().local_addr().unwrap(), bytes_codec.get_ref().peer_addr().unwrap());
            info!("id:{} variance:{} max_length:{}", id, variance, max_length); 

            let mut hasher = Md5::new();
            let mut received: usize = 0;

            while let Some(message) = bytes_codec.next().await {
                match message {
                    Ok(msg) => {
                        received += msg.len();
                        hasher.update(&msg);

                        // info!("id:{} msg_len:{} received:{}", id, msg.len(), received);

                        if received > max_length {
                            info!("messageReceived id:{} got {}", id, received);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("an error occurred while processing messages error = {:?}", e);
                        break;
                    }
                }
            }

            let hash = hasher.finalize();
            info!("Disconnected id:{} got received:{} {}", id, received, hex::encode(&hash));

            info!("id:{} {} -> {} is DOWN", id, bytes_codec.get_ref().local_addr().unwrap(), bytes_codec.get_ref().peer_addr().unwrap());
            latch.countdown();
        });
    }

    latch.wait();
    info!("All done. {}ms", start.elapsed().as_millis());

    Ok(())
}
