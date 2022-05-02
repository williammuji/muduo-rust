use tracing_subscriber;
use tracing::info;
use clap::Parser;
use tokio::fs::File;
use tokio::io;
use tokio::net::TcpListener;
use muduo_rust::accept_backoff;
use muduo_rust::Result;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    /// listen port
    #[clap(short, long)]
    #[clap(default_value_t = 2021)]
    port: u16,
    /// file_for_downloading 
    #[clap(short, long)]
    #[clap(default_value_t = String::from("file_for_downloading"))]
    file: String,
}

async fn run(options: Options) -> Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", options.port)).await?;
    info!("listening on port:{}", options.port);
    
    loop {
        let mut socket = accept_backoff(&listener).await?;

        let file_name = options.file.clone();
        tokio::spawn(async move {
            let _ = socket.set_nodelay(true).unwrap();
            info!("FileServer - {} -> {} is UP", socket.local_addr().unwrap(), socket.peer_addr().unwrap());
            info!("FileServer - Sending file {} to {}", file_name, socket.peer_addr().unwrap());
        
            let file = File::open(file_name).await;
            match file {
                Err(e) => {
                    info!("FileServer - no such file {}", e);
                }
                Ok(mut file) => {
                    let res = io::copy(&mut file, &mut socket).await;
                    if let Err(e) = res {
                        info!("FileServer - io::copy failed {}", e);
                    }
                }
            }
        });
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
   
    let _ = run(Options::parse()).await?;

    Ok(())
}
