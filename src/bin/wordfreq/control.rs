#![feature(is_sorted)]
use wordfreq::word_frequency_service_client::WordFrequencyServiceClient;
use wordfreq::Empty;
use tracing_subscriber;
use tracing::{info, error};
use clap::Parser;
use muduo_rust::Result;
use tokio::sync::mpsc;


mod wordfreq {
    tonic::include_proto!("wordfreq");
}

struct Worker {
    part: usize,
    got_info: bool,
    ready: bool,
    shard_max_key: i64,
    shard_key_count: i64,
    peers: Vec<String>,
}

impl Worker {
    fn new() -> Worker {
        Worker {
            part: 0,
            got_info: false,
            ready: false,
            shard_max_key: -1,
            shard_key_count: -1,
            peers: Vec::new(),
        }
    }
}

struct Controller {
    header_addr: String,
    workers: Vec<Worker>,
    max_key: i64,
    key_count: i64,
    peers: Vec<String>,
}

impl Controller {
    fn new(header_addr: String) -> Controller {
        Controller {
            header_addr,
            workers: Vec::new(), 
            max_key: 0,
            key_count: 0,
            peers: Vec::new(),
        }
    }

    async fn run(&mut self) {
        if let Err(e) = self.prepare().await {
            error!("prepare error:{}", e);
        } else {
            info!("All ready, num of workers = {}, max key = {}, total count = {}", self.workers.len(), self.max_key, self.key_count);
            // self.find_pivots();
        }
    }

    async fn prepare(&mut self) -> Result<()> {
        assert!(self.workers.is_empty());

        self.peers = self.get_peers().await?;
        for _ in 0..self.peers.len() {
            self.workers.push(Worker::new());
        }
        info!("connecting to {} workers: {:?}", self.peers.len(), self.peers);
  
        self.connect_all().await;
        info!("got info of all");
        assert!(self.peers.len() == self.workers.len());
   
        for worker in self.workers.iter() {
            self.key_count += worker.shard_key_count;
            if worker.shard_max_key > self.max_key {
                self.max_key = worker.shard_max_key; 
            }

            if !worker.ready {
                error!("Worker is not ready"); 
            }

            if worker.peers != self.peers {
                error!("Different peer address list of workers"); 
            }
        }

        Ok(())
    }

    async fn get_peers(&mut self) -> Result<Vec<String>> {
        let mut head = WordFrequencyServiceClient::connect(format!("http://{}", self.header_addr)).await?; 
   
        let request = tonic::Request::new(Empty{});
        let response = head.get_info(request).await;
        match response {
            Ok(res) => {
                let resp = res.get_ref();
                info!("head.get_info response:{:?}", resp);
                return Ok(res.into_inner().peers);
            },
            Err(e) => {
                info!("head.get_infoerror:{:?}", e);
                return Err(e.into());
            }
        }
    }

    async fn connect_all(&mut self) {
        info!("get info of all");
        let (tx, mut rx) = mpsc::channel::<Worker>(self.peers.len());
        for part in 0..self.peers.len() {
            let peer = self.peers[part].clone();
            let tx_clone = tx.clone();
            tokio::spawn(async move {
                let res = get_info(peer.clone(), part).await;
                match res { 
                    Ok(worker) => {
                        let res = tx_clone.send(worker).await;
                        if res.is_err() {
                            error!("tx_clone.send error"); 
                        }
                    }
                    Err(e) => {
                        error!("spawn get_info addr:{} error:{}", peer, e);
                    }
                }
            });

            if let Some(worker) = rx.recv().await {
                let part = worker.part;
                self.workers[part] = worker;
            }
        }
    }
}

async fn get_info(addr: String, part: usize) -> Result<Worker> {
    let mut client = WordFrequencyServiceClient::connect(format!("http://{}", addr)).await?;

    let request = tonic::Request::new(Empty{});
    let response = client.get_info(request).await;
    match response {
        Ok(res) => {
            let resp = res.get_ref();
            info!("client.get_info response:{:?}", resp);

            let mut worker = Worker::new();
            worker.part = part;
            assert!(!worker.got_info);
            worker.got_info = true;
            worker.ready = resp.ready;
            if !worker.ready {
                error!("worker {} is not ready.", part);
            }
            worker.shard_max_key = resp.max_key;
            worker.shard_key_count = resp.key_count;
            worker.peers = res.into_inner().peers;
            return Ok(worker);
        },
        Err(e) => {
            info!("client.get_info error:{:?}", e);
            return Err(e.into());
        }
    }
}


#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    /// header addr 
    #[clap(short, long)]
    #[clap(default_value_t = String::from("127.0.0.1:6000"))]
    addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let options = Options::parse();

    let mut controller = Controller::new(options.addr.clone());
    controller.run().await;

    Ok(())
}
