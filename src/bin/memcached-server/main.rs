mod item;
pub use item::{UpdatePolicy, Item};

mod session;
pub use session::{State, Protocol, Session};

mod server;
pub use server::{Shared, MemcacheServer};

use tracing_subscriber;
use tracing::{error, info};
use clap::Parser;
use tokio::sync::{broadcast, mpsc};
use std::sync::Arc;
use tokio::net::TcpListener;


#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Options {
    /// TCP port
    #[clap(short, long)]
    #[clap(default_value_t = 11211)]
    pub port: u16,

    /// UDP port
    #[clap(short, long)]
    #[clap(default_value_t = 11212)]
    udp_port: u16,

    /// Number of worker threads
    #[clap(short, long)]
    #[clap(default_value_t = 4)]
    threads: u16,
}

pub async fn run(options: Options) {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", options.port)).await.unwrap();
    info!("start listening.. port:{} threads:{}", options.port, options.threads);

    let (notify_shutdown, _) = broadcast::channel(1);
    let (server_shutdown_tx, mut server_shutdown_rx) = mpsc::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    let mut server = MemcacheServer {
        listener,
        notify_shutdown,
        server_shutdown_tx,
        shutdown_complete_tx,
        shutdown_complete_rx,
        shared: Arc::new(Shared::new()),
    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = server_shutdown_rx.recv() => {
            info!("server shutdown");   
        } 
    }

    let MemcacheServer {
        notify_shutdown,
        shutdown_complete_tx,
        mut shutdown_complete_rx,
        ..
    } = server;

    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    run(Options::parse()).await;
}
