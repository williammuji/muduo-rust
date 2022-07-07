use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use futures::SinkExt;
use bytes::Bytes;
use tracing_subscriber;
use tracing::{info, error};
use muduo_rust::Result;
use std::io::{self, BufRead};

type Rx = mpsc::UnboundedReceiver<String>;

async fn run(mut frame: Framed<TcpStream, LengthDelimitedCodec>, mut rx: Rx) -> Result<()> {
    loop {
        tokio::select! {
            Some(msg) = rx.recv() => {
                frame.send(Bytes::from(msg)).await?;
            }
            result = frame.next() => match result {
                Some(Ok(msg)) => {
                    info!("<<< {:?}", msg);
                }
                Some(Err(e)) => {
                    error!(
                        "an error occurred while processing messages error = {:?}",
                        e
                        );
                }
                None => break
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();

    let stream = TcpStream::connect("127.0.0.1:6142").await?;
    let frame = Framed::new(stream, LengthDelimitedCodec::new());

    let (tx, rx) = mpsc::unbounded_channel();
    
    tokio::spawn(async move {
        if let Err(e) = run(frame, rx).await {
            info!("an error occurred; error = {:?}", e);
        }
    });

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let _ = tx.send(line.unwrap());
    }

    Ok(())
}

