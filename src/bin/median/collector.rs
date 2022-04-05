use median::sorter_client::SorterClient;
use median::{Empty, QueryResponse, SearchRequest};

use muduo_rust::{Result, CountdownLatch};
use tracing_subscriber;
use tracing::{info, error};
use clap::Parser;
use tokio::sync::{broadcast, mpsc};
use async_trait::async_trait;
use crate::kth::KthSearch;

mod kth;

pub mod median {
    tonic::include_proto!("median");
}

async fn run(addr: String,
             connected: CountdownLatch, 
             mut notify_query: broadcast::Receiver<()>, 
             query_result_tx: mpsc::Sender<QueryResponse>, 
             mut notify_search: broadcast::Receiver<i64>,
             search_result_tx: mpsc::Sender<(i64, i64)>,
             _shutdown_complete_tx: mpsc::Sender<()>) -> Result<()> {
    info!("sorter addr:{} connecting", addr);
    let mut client = SorterClient::connect(addr).await?;
    connected.countdown();

    loop {
        tokio::select! {
            _ = notify_query.recv() => {
                let request = tonic::Request::new(Empty {});

                let response = client.query(request).await;
                match response {
                    Ok(res) => {
                        info!("client.query response:{:?}", res.get_ref());
                        let _ = query_result_tx.send(res.into_inner()).await?;
                    },
                    Err(e) => {
                        info!("client.query error:{:?}", e);
                        return Err(e.into()); 
                    }
                }
            }
            guess = notify_search.recv() => {
                if guess.is_err() {
                    return Ok(());
                }

                let search_request = SearchRequest {
                    guess: guess.unwrap(), 
                };

                let request = tonic::Request::new(search_request);
                let response = client.search(request).await;
                match response {
                    Ok(res) => {
                        info!("client.search response:{:?}", res.get_ref());
                        let resp = res.into_inner();
                        let _ = search_result_tx.send((resp.smaller, resp.same)).await?;
                    },
                    Err(e) => {
                        info!("client.search error:{:?}", e);
                        return Err(e.into()); 
                    }
                }
            }
        }
    }
}

struct Collector {
    count: usize,
    notify_search_tx: broadcast::Sender<i64>,
    search_result_rx: mpsc::Receiver<(i64, i64)>,
}

impl Collector {
    fn new(count: usize, notify_search_tx: broadcast::Sender<i64>, search_result_rx: mpsc::Receiver<(i64, i64)>) -> Collector {
        Collector {
            count,
            notify_search_tx,
            search_result_rx,
        }
    }
}

#[async_trait]
impl KthSearch for Collector {
    async fn search(&mut self, guess: i64) -> (i64, i64) {
        if let Err(e) = self.notify_search_tx.send(guess) {
            info!("KthSearch.notify_search failed {}", e);
            return (0, 0);
        }
       
        let mut smaller: i64 = 0;
        let mut same: i64 = 0;
        for _ in 0..self.count {
            let response = self.search_result_rx.recv().await;
            if response.is_none() {
                info!("KthSearch.search_result_rx recv none");
                return (0, 0);
            }

            let (res_smaller, res_same) = response.unwrap();
            smaller += res_smaller;
            same += res_same;
        }

        (smaller, same)
    }
}


#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    /// listen port
    #[clap(short, long)]
    #[clap(default_value_t = String::from("127.0.0.1:5555"))]
    sorter_addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let options = Options::parse();
    info!("starting");

    let sorter_addresses: Vec<&str> = options.sorter_addr.split(',').collect();
    let connected = CountdownLatch::new(sorter_addresses.len() as i32);
    let (notify_query, _) = broadcast::channel(1);
    let (query_result_tx, mut query_result_rx) = mpsc::channel(sorter_addresses.len());
    let (notify_search, _) = broadcast::channel(1);
    let (search_result_tx, search_result_rx) = mpsc::channel(sorter_addresses.len());
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    for sorter_addr in sorter_addresses.iter() {
        let addr = format!("http://{}", sorter_addr);
        let connected_clone = connected.clone();
        let notify_query_clone = notify_query.subscribe();
        let query_result_tx_clone = query_result_tx.clone();
        let notify_search_clone = notify_search.subscribe();
        let search_result_tx_clone = search_result_tx.clone();
        let shutdown_complete_tx_clone = shutdown_complete_tx.clone();
        tokio::spawn(async move {
            if let Err(err) = run(addr, connected_clone, notify_query_clone, query_result_tx_clone, notify_search_clone, search_result_tx_clone, shutdown_complete_tx_clone).await {
                error!(cause = %err, "failed to run");
            }
        });
    }
    
    let mut collector = Collector::new(sorter_addresses.len(), notify_search, search_result_rx);

    connected.wait();
    info!("All connected");

    let _ = notify_query.send(());

    let mut stats = QueryResponse {
        min: i64::MAX,
        max: i64::MIN,
        count: 0,
        sum: 0,
    };
    for _ in 0..sorter_addresses.len() {
        let res = query_result_rx.recv().await.unwrap();

        stats.count += res.count;
        stats.sum += res.sum;
        if res.count > 0 {
            if res.min < stats.min {
                stats.min = res.min;
            }
            if res.max > stats.max {
                stats.max = res.max;
            }
        }
    }
    info!("stats:{:?}", stats);

    if stats.count > 0 {
        info!("mean: {}", stats.sum as f64 / stats.count as f64);
    }

    if stats.count <= 0 {
        info!("***** No median");
    } else {
        let k = (stats.count+1)/2;
        let (median_num, succeed) = kth::get_kth(&mut collector, k, stats.min, stats.max).await;

        if succeed {
            info!("***** Median is {}", median_num);
        } else {
            error!("***** Median not found");
        }
    }

    let Collector {
        notify_search_tx,
        ..
    } = collector;
    drop(notify_search_tx);
    
    drop(shutdown_complete_tx);
    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}
