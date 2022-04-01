use muduo_rust::Result;

use tracing_subscriber;
use tracing::{error, info};
use clap::Parser;
use tokio::sync::oneshot;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};


async fn relay(server_stream: TcpStream, connect_addr: String) -> Result<()> {
    info!("relay: listen on {}, new client from {}", server_stream.local_addr().unwrap(), server_stream.peer_addr().unwrap());

    let client_stream = TcpStream::connect(connect_addr).await?;
    info!("relay: connected to {}, from {}", client_stream.peer_addr().unwrap(), client_stream.local_addr().unwrap());

    let (mut server_rd, mut server_wr) = server_stream.into_split();
    let (mut client_rd, mut client_wr) = client_stream.into_split();

    let (done_tx, done_rx) = oneshot::channel();
    tokio::spawn(async move {
        if io::copy(&mut server_rd, &mut client_wr).await.is_err() {
            info!("relay: failed to copy from client to server");
        } else {
            info!("relay: done copying serverconn to clientconn");
        }
        drop(client_wr);
        let _ = done_tx.send(()); 
    });

    if io::copy(&mut client_rd, &mut server_wr).await.is_err() {
        info!("relay: failed to copy from server to client");
    } else {
        info!("relay: done copying clientconn to serverconn");
    }
    drop(server_wr);
    let _ = done_rx.await;
    
    Ok(()) 
}

async fn run(options: Options) {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", options.port)).await.unwrap();
    info!("start listening.. port:{}", options.port);
   
    loop {
        let socket = listener.accept().await;

        let socket = match socket {
            Ok(stream) => stream,
            Err(err) => {
                info!("listener.accept error:{}", err);
                continue; 
            }
        };

        let connect_addr = options.server_addr.clone();
        tokio::spawn(async move {
            if let Err(err) = relay(socket.0, connect_addr).await {
                error!(cause = ?err, "relay error");
            }
        });
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    /// listen port
    #[clap(short, long)]
    #[clap(default_value_t = 2000)]
    port: u16,
    
    /// connect server addr 
    #[clap(short, long)]
    #[clap(default_value_t = String::from("localhost:3000"))]
    server_addr: String,
}


#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    run(Options::parse()).await;
}
