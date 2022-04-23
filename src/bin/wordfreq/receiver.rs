use tokio::net::{TcpListener, TcpStream};
use tracing_subscriber;
use tokio::sync::{mpsc, Semaphore};
use tracing::{error, info};
use tokio::time::{self, Duration};
use std::env;
use std::io::{self, Write};
use tokio_util::codec::{Framed, LinesCodec};
use tokio_stream::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::fs::OpenOptions;
use muduo_rust::Result;

type WordCounts = Arc<Mutex<HashMap<String, i64>>>;

struct Listener {
    addr: String, 
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    session_notify_shutdown_tx: mpsc::Sender<()>,
    word_counts: WordCounts, 
    senders: Arc<AtomicUsize>, 
}

impl Listener {
    async fn run(&mut self) -> Result<()> {
        info!("accepting inbound connections");

        let mut id: i64 = 0;
        loop {
            self.limit_connections.acquire().await.unwrap().forget();

            let socket = self.accept().await?;
            socket.set_nodelay(true).expect("set_nodelay failed");

            id += 1;
            let mut session = Session {
                name: format!("connection-{}", id),
                addr: self.addr.clone(),
                lines: Framed::new(socket, LinesCodec::new()),
                limit_connections: self.limit_connections.clone(),
                session_notify_shutdown_tx: self.session_notify_shutdown_tx.clone(),
                word_counts: self.word_counts.clone(),
                senders: self.senders.clone(),
            };
            info!("{} {} -> {} is UP", session.name, session.lines.get_ref().local_addr()?, session.lines.get_ref().peer_addr()?);
           
            tokio::spawn(async move {
                if let Err(err) = session.run().await {
                    error!(cause = ?err, "{} error", session.name);
                }
            });
        }
    }

    async fn accept(&mut self) -> io::Result<TcpStream> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        return Err(err.into()); 
                    }
                }
            }

            time::sleep(Duration::from_secs(backoff)).await;

            backoff *= 2;
        }
    }

}
    
struct Session {
    name: String,
    addr: String,
    lines: Framed<TcpStream, LinesCodec>,
    limit_connections: Arc<Semaphore>,
    session_notify_shutdown_tx: mpsc::Sender<()>,
    word_counts: WordCounts, 
    senders: Arc<AtomicUsize>, 
}

impl Session {
    async fn run(self: &mut Session) -> io::Result<()> {
        loop {
            tokio::select! {
                line = self.lines.next() => match line {
                    Some(Ok(msg)) => {
                        let res: Vec<&str> = msg.split('\t').collect();
                        if res.len() != 2 {
                            tracing::error!("wrong format, no tab found");
                        } else {
                           let mut word_counts = self.word_counts.lock().unwrap();
                           *word_counts.entry(res[0].to_string()).or_insert(0) += res[1].parse::<i64>().unwrap();
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!(
                            "an error occurred while processing messages error = {:?}",
                            e
                            );
                    }
                    None => break,
                },
            }
        }
        Ok(())
    }

    fn output(self: &mut Session) -> std::io::Result<()> {
        info!("Writing shard");

        let mut file = OpenOptions::new().write(true)
            .create(true)
            .open(format!("/tmp/shard_{}", self.addr)).unwrap();
        let word_counts = self.word_counts.lock().unwrap();
        for (word, counts) in word_counts.iter() {
            file.write_all(format!("{}\t{}\n", word, counts).as_bytes())?;
        }
        file.flush()?;
        Ok(())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        info!("{} {} -> {} is DOWN", self.name, self.lines.get_ref().local_addr().unwrap(), self.lines.get_ref().peer_addr().unwrap());
        self.limit_connections.add_permits(1);
        if self.senders.fetch_sub(1, Ordering::SeqCst) == 1 {
            let _ = self.output();
            let session_notify_shutdown_tx = self.session_notify_shutdown_tx.clone();
            tokio::spawn(async move {
                let _ = session_notify_shutdown_tx.send(()).await;
            });
        }
    }
}

pub async fn run(addr: &str, num_of_senders: usize) {

    let (session_notify_shutdown_tx, mut session_notify_shutdown_rx) = mpsc::channel(1);

    let listener = TcpListener::bind(addr).await.unwrap();
    info!("start listening.. addr:{} num_of_senders:{}", addr, num_of_senders);
   
    let mut server = Listener {
        addr: addr.to_string(),
        listener,
        limit_connections: Arc::new(Semaphore::new(num_of_senders)),
        session_notify_shutdown_tx,
        word_counts: Arc::new(Mutex::new(HashMap::new())),
        senders: Arc::new(AtomicUsize::new(num_of_senders)),
    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = session_notify_shutdown_rx.recv() => {
            info!("shard write completed");
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
   
    if env::args().len() < 3 {
        error!("Usage: {} addr num_of_senders", env::args().nth(0).unwrap());
        return;
    }

    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:6142".to_string());

    let num_of_senders : usize = match env::args().nth(2).unwrap().parse() {
        Ok(n) => n,
        Err(_) => {
            return;
        }
    };

    run(&addr, num_of_senders).await;
}
