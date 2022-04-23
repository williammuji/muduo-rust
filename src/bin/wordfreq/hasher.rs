use tracing_subscriber;
use tracing::{info, error};
use tokio::net::TcpStream;
use std::io;
use tokio_util::codec::{Framed, LinesCodec};
use tokio::sync::{mpsc, Mutex};
use futures::SinkExt;

use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::fs::File;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::sync::Arc;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const MAX_HASH_SIZE: usize = 10_000_000;

#[tokio::main]
async fn main() {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    if env::args().len() < 3 {
        error!("Usage: {} addresses_of_receivers input_file1 [input_file2]*", env::args().nth(0).unwrap());
        error!("Example: {} 'ip1:port1,ip2:port2,ip3:port3,ip4:port4' input_file1 input_file2", env::args().nth(0).unwrap());
        return;
    }

    // Create the shared state. This is how all the connections communicate.
    //
    // The server task will hold a handle to this. For every new connection, the
    // `state` handle is cloned and passed into the task that processes the
    // connection.
    let state = Arc::new(Mutex::new(Shared::new()));

    let (connected_tx, mut connected_rx) = mpsc::channel(1);
    let (processed_tx, mut processed_rx) = mpsc::channel(1);
    let (disconnected_tx, mut disconnected_rx) = mpsc::channel(1);

    let addrs = env::args().nth(1).unwrap();
    let res: Vec<&str> = addrs.split(',').collect();
    for addr in res.iter() {
        // Clone a handle to the `Shared` state for the new connection.
        let state = Arc::clone(&state);
        let addr = addr.to_string();
        let connected_tx = connected_tx.clone();
        let disconnected_tx = disconnected_tx.clone();
        // Spawn our handler to be run asynchronously.
        tokio::spawn(async move {
            if let Err(e) = process_connection(state, addr, connected_tx, disconnected_tx).await {
                tracing::info!("an error occurred when process_connection; error = {:?}", e);
            }
        });
    }

    drop(connected_tx);
    // all connected
    let _ = connected_rx.recv().await;
    info!("All connected");

    for i in 2..env::args().len() {
        // Clone a handle to the `Shared` state for the new connection.
        let state = Arc::clone(&state);
        let file_name = env::args().nth(i).unwrap();
        let processed_tx = processed_tx.clone();
        // Spawn our handler to be run asynchronously.
        tokio::spawn(async move {
            if let Err(e) = process_file(state, file_name, processed_tx).await {
                tracing::info!("an error occurred when process_file; error = {:?}", e);
            }
        });
    }

    drop(processed_tx);
    // all files processed 
    let _ = processed_rx.recv().await;
    info!("All files processed");

    {
        let mut state = state.lock().await;
        state.buckets.clear();
    }

    drop(disconnected_tx);
    // all disconnected
    let _ = disconnected_rx.recv().await;
    info!("All disconnected");
}

/// Shorthand for the transmit half of the message channel.
type Tx = mpsc::UnboundedSender<String>;

/// Shorthand for the receive half of the message channel.
type Rx = mpsc::UnboundedReceiver<String>;

struct Shared {
    buckets: Vec<Tx>,
}

impl Shared {
    /// Create a new, empty, instance of `Shared`.
    fn new() -> Self {
        Shared {
            buckets: Vec::new(),
        }
    }

    /// Send a `LineCodec` encoded message to every connection, except
    /// for the sender.
    async fn send(&mut self, word: String, count: i64) {
        let message = format!("{}\t{}", word, count);

        let mut s = DefaultHasher::new();
        word.hash(&mut s);
        let _ = self.buckets[s.finish() as usize % self.buckets.len()].send(message);
    }
}

struct Connection {
    lines: Framed<TcpStream, LinesCodec>,
    // file datas read to channel in
    rx: Rx,
}

impl Connection {
    async fn new(
        state: Arc<Mutex<Shared>>,
        lines: Framed<TcpStream, LinesCodec>
    ) -> io::Result<Connection> {
        // Create a channel for this connection
        let (tx, rx) = mpsc::unbounded_channel();

        // Add an entry for this `Peer` in the shared state map.
        state.lock().await.buckets.push(tx);

        Ok(Connection { lines, rx})
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        info!("{} -> {} is DOWN", self.lines.get_ref().local_addr().unwrap(), self.lines.get_ref().peer_addr().unwrap());
    }
}

/// Process an individual connection 
async fn process_connection(
    state: Arc<Mutex<Shared>>,
    addr: String,
    connected_tx: mpsc::Sender<()>,
    disconnected_tx: mpsc::Sender<()>
) -> Result<(), Box<dyn Error>> {
    let socket = TcpStream::connect(addr).await?;
    let _ = socket.set_nodelay(true)?;
    info!("{} -> {} is UP", socket.local_addr()?, socket.peer_addr()?);
    
    let lines = Framed::new(socket, LinesCodec::new());
    // Register our connection with state which internally sets up some channels.
    let mut connection = Connection::new(state.clone(), lines).await?;

    drop(connected_tx);

    // A message was received from a connection. Send it to the current user.
    while let Some(msg) = connection.rx.recv().await {
        connection.lines.send(&msg).await?;
    }

    drop(disconnected_tx);

    Ok(())
}

/// Process an individual file 
async fn process_file(
    state: Arc<Mutex<Shared>>,
    file_name: String,
    processed_tx: mpsc::Sender<()>
) -> Result<(), Box<dyn Error>> {
    let file = File::open(file_name).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut word_counts = HashMap::new();

    while let Some(line) = lines.next_line().await? {
        *word_counts.entry(line).or_insert(0) += 1;

        if word_counts.len() >= MAX_HASH_SIZE {
            for (word, counts) in word_counts.iter() {
                let mut state = state.lock().await;
                state.send(word.to_string(), *counts).await;
            }
            word_counts.clear();
        }
    }
    
    for (word, counts) in word_counts.iter() {
        let mut state = state.lock().await;
        state.send(word.to_string(), *counts).await;
    }

    drop(processed_tx);

    Ok(())
}
