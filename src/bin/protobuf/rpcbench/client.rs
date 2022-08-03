use echom::echo_service_client::EchoServiceClient;
use echom::EchoRequest;
use muduo_rust::{CountdownLatch, Result};
use tracing_subscriber;
use tracing::{info, error};
use clap::Parser;
use std::time::Instant;
use tokio::sync::broadcast;

pub mod echom {
    tonic::include_proto!("echo");
}

const REQUESTS: usize = 50_000;

async fn process_bench_client(id: i32, mut request_rx: broadcast::Receiver<EchoRequest>, all_connected: CountdownLatch, all_finished: CountdownLatch) -> Result<()> { 
    let mut client = EchoServiceClient::connect("http://[::1]:8888").await?;
    all_connected.countdown();
    let mut count: usize = 0;

    let request = request_rx.recv().await;
    match request {
        Ok(req) => {
            loop { 
                let echo_request = tonic::Request::new(req.clone());
                match client.echo(echo_request).await {
                    Ok(_) => {
                        count += 1;
                        if count < REQUESTS {
                            if count % 1000 == 0 {
                                info!("Client-{} count:{} done", id, count);
                            }
                            continue;
                        } else {
                            info!("Client-{} finished", id);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Client-{} echo error:{}", id, e);
                        break;
                    }
                }
            }
        }
        Err(e) => {
            error!("Client-{} request_rx.recv error:{}", id, e);
        }
    }

    all_finished.countdown();

    Ok(())
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    #[clap(short, long)]
    #[clap(default_value_t = 100)]
    clients: i32,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init(); 

    let options = Options::parse();
    
    let all_connected = CountdownLatch::new(options.clients);
    let all_finished = CountdownLatch::new(options.clients);
    let (request_tx, _) = broadcast::channel(1);

    for id in 0 .. options.clients {
        let request_rx_clone = request_tx.subscribe(); 
        let all_connected_clone = all_connected.clone();
        let all_finished_clone = all_finished.clone();
        tokio::spawn(async move {
            if let Err(e) = process_bench_client(id, request_rx_clone, all_connected_clone, all_finished_clone).await {
                info!("an error occurred; error = {:?}", e);
            }
        }); 
    }

    all_connected.wait();
    let start = Instant::now();
    info!("all connected");

    let request = EchoRequest {
        payload: "001010".into(),
    };
    request_tx.send(request).unwrap();
    
    all_finished.wait();
    info!("all finished");
    let seconds = start.elapsed().as_secs_f32();
    info!("{} seconds", seconds);
    info!("{:.1} calls per second", (options.clients as usize * REQUESTS) as f32 / seconds);

    Ok(())
}
