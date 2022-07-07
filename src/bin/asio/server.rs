use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use futures::SinkExt;
use std::collections::HashMap;
use std::env;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use bytes::Bytes;    
use tracing_subscriber;
use tracing::{info, error};
use muduo_rust::Result;

type Tx = mpsc::UnboundedSender<String>;
type Rx = mpsc::UnboundedReceiver<String>;

struct Shared {
    peers: HashMap<SocketAddr, Tx>,
}

struct Peer {
    buffers: Framed<TcpStream, LengthDelimitedCodec>,

    rx: Rx,
}

impl Shared {
    fn new() -> Self {
        Shared {
            peers: HashMap::new(),
        }
    }

    async fn broadcast(&mut self, sender: SocketAddr, message: &str) {
        for peer in self.peers.iter_mut() {
            if *peer.0 != sender {
                let _ = peer.1.send(message.into());
            }
        }
    }
}

impl Peer {
    async fn new(
        state: Arc<Mutex<Shared>>,
        buffers: Framed<TcpStream, LengthDelimitedCodec>,
    ) -> io::Result<Peer> {
        let addr = buffers.get_ref().peer_addr()?;

        let (tx, rx) = mpsc::unbounded_channel();

        state.lock().await.peers.insert(addr, tx);

        Ok(Peer { buffers, rx })
    }
}

async fn process(
    state: Arc<Mutex<Shared>>,
    stream: TcpStream,
    addr: SocketAddr,
) -> Result<()> {
    let buffers = Framed::new(stream, LengthDelimitedCodec::new());

    let peer_addr = buffers.get_ref().peer_addr();
    let mut peer = Peer::new(state.clone(), buffers).await?;

    {
        let mut state = state.lock().await;
        let msg = format!("{:?} has joined the chat", peer_addr);
        info!("{}", msg);
        state.broadcast(addr, &msg).await;
    }

    loop {
        tokio::select! {
            Some(msg) = peer.rx.recv() => {
                peer.buffers.send(Bytes::from(msg)).await?;
            }
            result = peer.buffers.next() => match result {
                Some(Ok(msg)) => {
                    let mut state = state.lock().await;
                    let msg = format!("{:?}: {:?}", peer_addr, msg);

                    state.broadcast(addr, &msg).await;
                }
                Some(Err(e)) => {
                    error!(
                        "an error occurred while processing messages for {:?}; error = {:?}",
                        peer_addr, 
                        e
                    );
                }
                None => break,
            },
        }
    }

    {
        let mut state = state.lock().await;
        state.peers.remove(&addr);

        let msg = format!("{:?} has left the chat", peer_addr);
        info!("{}", msg);
        state.broadcast(addr, &msg).await;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();

    let state = Arc::new(Mutex::new(Shared::new()));

    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:6142".to_string());

    let listener = TcpListener::bind(&addr).await?;

    info!("server running on {}", addr);

    loop {
        let (stream, addr) = listener.accept().await?;

        let state = Arc::clone(&state);

        tokio::spawn(async move {
            info!("accepted connection");
            if let Err(e) = process(state, stream, addr).await {
                info!("an error occurred; error = {:?}", e);
            }
        });
    }
}
