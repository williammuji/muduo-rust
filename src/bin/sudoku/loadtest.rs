use tokio::net::TcpStream;
use tracing::{info, error};
use tracing_subscriber;
use std::env;
use std::sync::{Arc, Mutex};
use tokio::time::{self, Duration, Instant};
use tokio::sync::{broadcast, mpsc};
use std::error::Error;
use std::io::{self, BufReader, BufRead};
use tokio_util::codec::{Framed, LinesCodec};
use std::collections::HashMap;
use std::fs::File;
use tokio_stream::StreamExt;
use futures::SinkExt;

mod percentile;

const HZ: i64 = 100;

//type Tx = mpsc::UnboundedSender<i32>;
enum SessionCmd {
    SendCmd(i64),
    ReportCmd,
}

type Rx = mpsc::UnboundedReceiver<SessionCmd>;


struct Shared {
    file_lines: Vec<String>,
    latencies: Vec<i32>,
    infly: i32,
    remain: i32,
    count: i32,
    ticks: i64,
    sofar: i64,
}

impl Shared {
    fn new(file_lines: Vec<String>) -> Self {
        Shared {
            file_lines,
            latencies: Vec::new(),
            infly: 0,
            remain: 0,        
            count: 0,
            ticks: 0,
            sofar: 0,
        }
    }
}
            
async fn process_session(id: String, 
                         addr: String, 
                         shutdown: broadcast::Receiver<()>, 
                         shutdown_complete_tx: mpsc::Sender<()>, 
                         rx: Rx,
                         shared: Arc<Mutex<Shared>>) -> Result<(), Box<dyn Error>> {
    let socket = TcpStream::connect(addr).await?;
    let _ = socket.set_nodelay(true)?;

    let mut session = Session {
        id,
        shutdown,
        _shutdown_complete_tx: shutdown_complete_tx,
        count: 0,
        send_times: HashMap::new(),
        latencies: Vec::new(),
        rx,
        shared,
        lines: Framed::new(socket, LinesCodec::new()),
    };

    info!("{} -> {} is UP", session.lines.get_ref().local_addr()?, session.lines.get_ref().peer_addr()?);

    loop {
        tokio::select! {
            line = session.lines.next() => match line {
                Some(Ok(msg)) => {
                    if msg.len() > 100 {
                        error!("Line is too long!");
                        () 
                    }

                    session.verify(&msg, Instant::now());
                }
                Some(Err(e)) => {
                    tracing::error!(
                        "an error occurred while processing messages error = {:?}",
                        e
                        );
                }
                None => break,
            },
            Some(cmd) = session.rx.recv() => {
                match cmd {
                    SessionCmd::SendCmd(reqs) => {
                        session.send(reqs as i32).await?;
                    }
                    SessionCmd::ReportCmd => {
                        session.report();
                    }
                }
            }
            _ = session.shutdown.recv() => break
        }
    }

    Ok(())
}

struct Session {
    id: String,
    shutdown: broadcast::Receiver<()>,
    _shutdown_complete_tx: mpsc::Sender<()>,
    count: i32,
    send_times: HashMap<i32, Instant>,
    latencies: Vec<i32>,
    rx: Rx,
    shared: Arc<Mutex<Shared>>,
    lines: Framed<TcpStream, LinesCodec>,
}

impl Drop for Session {
    fn drop(&mut self) {
        info!("{} -> {} is DOWN", self.lines.get_ref().local_addr().unwrap(), self.lines.get_ref().peer_addr().unwrap());
    }
}

impl Session {
    async fn send(&mut self, n: i32) -> Result<(), Box<dyn Error>> { 
        let mut msg = String::new();
        let now = Instant::now();
        {
            let shared = self.shared.lock().unwrap();
            for i in 0 .. n {
                let line = &shared.file_lines[self.count as usize%shared.file_lines.len()];
                if i > 0 {
                    msg.push_str("\r\n");
                }
                msg.push_str(&format!("c{}-{:08}:{}", self.id, self.count, line));
                self.send_times.insert(self.count, now);
                self.count += 1;
            }
        }
        self.lines.send(msg).await?;
        Ok(())
    }

    fn verify(&mut self, response: &String, recv_time: Instant) {
        if let Some(colon) = response.find(':') {
            if let Some(dash) = response.find('-') {
                if dash < colon {
                    let id = &response[dash+1..colon];
                    let id: i32 = id.parse().unwrap();
                    if let Some(send_time) = self.send_times.get(&id) {
                        let latency = recv_time.duration_since(*send_time).as_micros() as i32;
                        self.latencies.push(latency);
                        let _ = self.send_times.remove(&id);
                    } else {
                        error!("Unknown id {} of {}", id, self.id);
                    }
                }
            }
        }
    }

    fn report(&mut self) {
        let mut shared = self.shared.lock().unwrap();
        shared.latencies.extend_from_slice(&self.latencies);
        self.latencies.clear();
        shared.infly += self.send_times.len() as i32;
        shared.remain -= 1;
               
        if shared.remain == 0 {
            let infly = shared.infly;
            let mut percentile = percentile::Percentile::new(&mut shared.latencies, infly);
            info!("{}", percentile.stat);
            if let Err(e) = percentile.save(&shared.latencies, &format!("r{:04}", shared.count)) {
                tracing::info!("an error occurred when process_session; error = {:?}", e);
            }
            shared.count += 1;
        }
    }
}

fn read_file_to_lines(file_name: &str) -> io::Result<Vec<String>> {
    let file = File::open(file_name)?;
    let reader = BufReader::new(file);

    let mut lines: Vec<String> = Vec::new();
    for line in reader.lines() {
        lines.push(line.unwrap());
    }
    info!("{} requests from {}", lines.len(), file_name);

    Ok::<_, io::Error>(lines)
}

#[tokio::main]
async fn main() -> io::Result<()> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    if env::args().len() < 5 {
        error!("Usage: {} input server_addr requests_per_second connections", env::args().nth(0).unwrap());
        return Ok(());
    }
    
    let input = env::args().nth(1).unwrap();
    let server_addr = env::args().nth(2).unwrap();
    let rps: i64  = env::args().nth(3).unwrap().parse().unwrap();
    let connections: i32 = env::args().nth(4).unwrap().parse().unwrap();

    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    let shared = Arc::new(Mutex::new(Shared::new(read_file_to_lines(&input)?)));
    let mut session_txs = Vec::with_capacity(connections as usize);

    for id in 0 .. connections {
        let (tx, rx) = mpsc::unbounded_channel();

        session_txs.push(tx);

        let addr = server_addr.clone();
        let notify_shutdown_sub = notify_shutdown.subscribe();
        let shutdown_complete_tx_clone = shutdown_complete_tx.clone();
        let shared_clone = shared.clone();
        tokio::spawn(async move {
            if let Err(e) = process_session((id+1).to_string(), addr, notify_shutdown_sub, shutdown_complete_tx_clone, rx, shared_clone).await {
                tracing::info!("an error occurred when process_session; error = {:?}", e);
            }
        });
    }

    let mut tick_interval = time::interval(Duration::from_millis(1000/HZ as u64));
    let mut tock_interval = time::interval(Duration::from_secs(1));
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tick_interval.tick() => {
                    let mut shared = shared.lock().unwrap();
                    shared.ticks += 1;

                    let reqs: i64 = rps * shared.ticks / HZ - shared.sofar;
                    shared.sofar += reqs;

                    if reqs > 0 {
                        for tx in session_txs.iter() {
                            let _ = tx.send(SessionCmd::SendCmd(reqs)); 
                        }
                    }
                }
                _ = tock_interval.tick() => {
                    {
                        let mut shared = shared.lock().unwrap();
                        shared.remain = connections;
                    }
                    for tx in session_txs.iter() {
                        let _ = tx.send(SessionCmd::ReportCmd); 
                    }
                }
            }
        }
    });
            
    let shutdown = tokio::signal::ctrl_c();
    tokio::select! {
        _ = shutdown => {
            info!("shutting down");
        }
    }

    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}
