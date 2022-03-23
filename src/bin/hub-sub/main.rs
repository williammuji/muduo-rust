use muduo_rust::Result;

use tracing_subscriber;
use tracing::info;
use clap::Parser;
use tokio::sync::mpsc;
use tokio_util::codec::{Framed, LinesCodec};
use tokio_stream::StreamExt;
use futures::SinkExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use sysinfo::{UserExt, System, SystemExt, get_current_pid};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    /// Hub address 
    #[clap(short, long, default_value_t = String::from("127.0.0.1:11211"))]
    addr: String,
    
    /// Topic 
    #[clap(short, long)]
    topic: String,
}

async fn run(options: Options) -> Result<()> {
    let address = options.addr;
    let (done_tx, done_rx) = oneshot::channel();
    let (sub_tx, mut sub_rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let system = System::new_all();
        let current_pid = get_current_pid().unwrap();
        let process = system.process(current_pid).unwrap();
        let mut user_name = String::new();
        for user in system.users() {
            if *(user.uid()) == process.uid {
                user_name = user.name().to_string();
            }
        }
        let name = format!("{}@{}:{:?}", user_name, system.host_name().unwrap(), current_pid);  

        let stream = TcpStream::connect(address).await.unwrap();
        let _ = stream.set_nodelay(true).unwrap();
        let mut lines = Framed::new(stream, LinesCodec::new());
        info!("{} {} -> {} is UP", name, lines.get_ref().local_addr().unwrap(), lines.get_ref().peer_addr().unwrap());

        loop {
            tokio::select! {
                line = lines.next() => match line {
                    Some(Ok(msg)) => {
						info!("{}", msg);
        			}
                    Some(Err(e)) => {
                        tracing::error!(
                            "an error occurred while processing messages error = {:?}",
                            e
                            );
                    }
                    None => break,
                },
                sub_msg = sub_rx.recv() => {
                    let _ = lines.send(sub_msg.unwrap()).await.unwrap();
                }
            }
        }

        if let Err(_) = done_tx.send(()) {
            info!("oneshot receiver dropped");
        }

        info!("{} {} -> {} is DOWN", name, lines.get_ref().local_addr().unwrap(), lines.get_ref().peer_addr().unwrap());
    });


    let topics: Vec<&str> = options.topic.split(',').collect();
    for topic in &topics {
        let _ = sub_tx.send(format!("sub {}", topic))?;
    }
        
    let _ = done_rx.await.unwrap();

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let _ = run(Options::parse()).await?;
    Ok(())
}
