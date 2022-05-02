use muduo_rust::{CountdownLatch, Result};
use clap::Parser;
use tracing_subscriber;
use tracing::info;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LinesCodec};
use futures::SinkExt;
use tokio_stream::StreamExt;
use std::time::Instant;
use tokio::sync::broadcast;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    /// Host IP
    #[clap(short, long, default_value_t = String::from("127.0.0.1"))]
    host_ip: String,

    /// TCP port
    #[clap(short, long, default_value_t = 11211)]
    tcp_port: u16,

    /// Number of worker threads
    #[clap(long, default_value_t = 4)]
    threads: i32,
    
    /// Number of concurrent clients
    #[clap(short, long, default_value_t = 100)]
    clients: i32,
    
    /// Number of requests per client
    #[clap(short, long, default_value_t = 2)]
    requests: i32,
    
    /// Number of keys per client 
    #[clap(short, long, default_value_t = 10000)]
    keys: i32,
    
    /// Set or Get 
    #[clap(short, long, default_value_t = 1)]
    set: u16,
}

#[derive(Debug, PartialEq, Copy, Clone)]
enum Operation {
    Get,
    Set,
}

struct Client {
    name: String,
    notify_send: broadcast::Receiver<()>,
    op: Operation,
    sent: i32,
    acked: i32,
    requests: i32,
    keys: i32,
    value_len: i32,
    value: String,
    connected: CountdownLatch,
    finished: CountdownLatch,
}

impl Client {
    fn new(n: i32, notify_send: broadcast::Receiver<()>, op: Operation, requests: i32, keys: i32, value_len: i32, connected: CountdownLatch, finished: CountdownLatch) -> Client {
        let mut value: String = String::with_capacity(value_len as usize);
        for _ in 0..value_len {
            value.push('a');
        } 

        Client {
            name: format!("conn-{}", n),
            notify_send,
            op,
            sent: 0,
            acked: 0,
            requests,
            keys,
            value_len,
            value,
            connected,
            finished,
        }
    }

    async fn run(&mut self, addr: &str) -> Result<()> {
        let stream = TcpStream::connect(addr).await?;
        let _ = stream.set_nodelay(true)?;
        let mut lines = Framed::new(stream, LinesCodec::new());
        
        info!("{} {} -> {} is UP", self.name, lines.get_ref().local_addr().unwrap(), lines.get_ref().peer_addr().unwrap());
        self.connected.countdown();

        loop {
            tokio::select! {
                line = lines.next() => match line {
                    Some(Ok(request)) => {
                        if self.op == Operation::Set {
                            self.acked += 1;
                            if self.sent < self.requests {
                                self.send(&mut lines).await?;
                            }
                        } else {
                            if request.ends_with("END") {
                                self.acked += 1;
                                if self.sent < self.requests {
                                    self.send(&mut lines).await?;
                                }
                            }
                        }

                        if self.acked == self.requests {
                            break;
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
                _ = self.notify_send.recv() => {
                    self.send(&mut lines).await?;
                }
            }
        }

        self.finished.countdown();
        info!("{} {} -> {} is DOWN", self.name, lines.get_ref().local_addr().unwrap(), lines.get_ref().peer_addr().unwrap());
        Ok(()) 
    }

    async fn send(&mut self, lines: &mut Framed<TcpStream, LinesCodec>) -> Result<()> {
        if self.op == Operation::Set {
            let key_str = format!("set {}{} 42 0 {}", self.name, self.sent%self.keys, self.value_len);
            lines.send(&key_str).await?;
            self.sent += 1;
            lines.send(&self.value).await?;
        } else {
            let key_str = format!("get {}{}", self.name, self.sent%self.keys);
            lines.send(&key_str).await?;
            self.sent += 1;
        }
        Ok(())
    }
}

async fn run(options: Options) -> Result<()> {
    info!("Connecting {}:{}", options.host_ip, options.tcp_port);

    let mut op = Operation::Set;
    if options.set == 0 {
        op = Operation::Get;
    }

    let value_len = 100;
    let memory_mib = 1.0 * options.clients as f64 * options.keys as f64 * (32+80+value_len+8) as f64 / 1024.0 / 1024.0 as f64;
    info!("estimated memcached-debug memory usage {} MiB", memory_mib as i32);

    let connected = CountdownLatch::new(options.clients);
    let finished = CountdownLatch::new(options.clients);
    let (notify_send, _) = broadcast::channel(1);

    let addr = format!("{}:{}", options.host_ip, options.tcp_port);

    for i in 0..options.clients {
        let mut client = Box::new(Client::new(
                    i,
                    notify_send.subscribe(),
                    op,
                    options.requests,
                    options.keys,
                    value_len,
                    connected.clone(),
                    finished.clone()));

        let address = addr.clone();
        tokio::spawn(async move {
            if let Err(e) = client.run(&address).await {
                tracing::info!("an error occurred when process_connection; error = {:?}", e);
            }
        });
    }

    connected.wait();
    info!("{} clients all connected", options.clients);

    let now = Instant::now();
    drop(notify_send);
    finished.wait();
    info!("All finished");

    let seconds = now.elapsed().as_secs() as i32;
    info!("{} sec", seconds);
    if seconds > 0 {
        info!("{} QPS", 1.0 * (options.clients * options.requests / seconds) as f32);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let _ = run(Options::parse()).await?;
    Ok(())
}
