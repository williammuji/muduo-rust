#![feature(is_sorted)]
use tonic::{transport::Server, Request, Response, Status};

use wordfreq::word_frequency_service_server::{WordFrequencyService, WordFrequencyServiceServer};
use wordfreq::word_frequency_service_client::WordFrequencyServiceClient;
use wordfreq::{Empty, GetInfoResponse, GetHistogramRequest, GetHistogramResponse, ShuffleKeyRequest};
use wordfreq::{ShuffleKeyResponse, ShardKeyRequest, SortKeyResponse};

use tracing_subscriber;
use tracing::{info, error};
use clap::Parser;
use std::time::Instant;
use std::fs::{File, OpenOptions};
use std::io::{Write, BufReader, BufRead};
use tokio::sync::mpsc;
use superslice::Ext;
use std::cmp::Ordering;
use muduo_rust::Error;
use muduo_rust::BlockingQueue;
use std::mem;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

mod wordfreq {
    tonic::include_proto!("wordfreq");
}

const CONCURRENCY: usize = 4;
const BATCH: usize = 8192;

type Tx = mpsc::UnboundedSender<ShardKeyRequest>;

fn is_sorted_ord(vec: &Vec<i64>) -> bool {
    vec.is_sorted_by(|a, b| {
        if a < b {
            Some(Ordering::Less)
        } else if a > b {
            Some(Ordering::Greater)
        } else {
            None
        }
    })
}
    
fn new_shard_key_request() -> ShardKeyRequest {
    ShardKeyRequest {
        partition: -1,
        keys: Vec::new(),
    }
}

#[derive(Clone)]
struct Shared {
    shuffling: bool,
    partition: i32,
    partition_keys: Vec<i64>,
}

impl Shared {
    fn new() -> Shared {
        Shared {
            shuffling: false,
            partition: -1,
            partition_keys: Vec::new(),
        }
    }
}

struct TheWordFrequencyService {
    keys: Vec<i64>,
    max_key: i64,
    quit_tx: mpsc::Sender<()>,
    peers: Option<Peers>,
    concurrent_requests: usize,
    shared: Arc<Mutex<Shared>>,
}

impl TheWordFrequencyService {
    fn new(keys: Vec<i64>, max_key: i64, quit_tx: mpsc::Sender<()>, peers: Option<Peers>) -> TheWordFrequencyService {
        let concurrent_requests: usize;
        if peers.is_some() {
            concurrent_requests = peers.as_ref().unwrap().len() * CONCURRENCY;
        } else {
            concurrent_requests = 0;
        }

        TheWordFrequencyService {
            keys,
            max_key,
            quit_tx,
            peers,
            concurrent_requests,
            shared: Arc::new(Mutex::new(Shared::new())),
        }
    }
    
    async fn shuffle(
        &self,
        request: Request<ShuffleKeyRequest>,
        ) -> std::result::Result<Response<ShuffleKeyResponse>, Error> {
     
        let start = Instant::now();
        let request = request.get_ref();
        let peers = self.peers.as_ref().unwrap();

        let mut shard_requests: Vec<ShardKeyRequest> = Vec::with_capacity((*peers).tx.len());
        for _ in 0..(*peers).tx.len() {
            shard_requests.push(new_shard_key_request());
        }
        assert!(shard_requests.len() == (*peers).tx.len());

        for _ in shard_requests.len() .. self.concurrent_requests {
            (*peers).free_shards.put(());
        }

        for key in self.keys.iter() {
            let idx = request.pivots.lower_bound(key);
            assert!(idx < shard_requests.len());

            let mut shard_req = &mut shard_requests[idx];
            (*shard_req).keys.push(*key);
            if (*shard_req).keys.len() >= BATCH {
                (*shard_req).partition = idx as i32;
                let req = mem::replace(&mut shard_requests[idx], new_shard_key_request());
                let res = (*peers).tx[idx].send(req);
                if let Err(e) = res {
                    error!("WordFrequencyService.peers.tx.send request:{:?} failed {}", request, e);
                    return Err(e.into()); 
                }

                info!("taking");
                let _ = (*peers).free_shards.take();
                info!("taken");
            }
        }

        for idx in 0..shard_requests.len() {
            let mut shard_req = &mut shard_requests[idx];
            if (*shard_req).keys.len() > 0 {
                (*shard_req).partition = idx as i32;
                let req = mem::replace(&mut shard_requests[idx], new_shard_key_request());
                let res = (*peers).tx[idx].send(req);
                if let Err(e) = res {
                    error!("WordFrequencyService.peers.tx.send request:{:?} failed {}", request, e);
                    return Err(e.into()); 
                }
            }
        }

        for _ in 0..self.concurrent_requests {
            info!("taking");
            let _ = peers.free_shards.take(); 
            info!("taken");
        }

        Ok(Response::new(wordfreq::ShuffleKeyResponse{
            error: String::new(),
            elapsed: start.elapsed().as_secs_f64(),
        }))
    }
}

#[tonic::async_trait]
impl WordFrequencyService for TheWordFrequencyService {
    async fn get_info(
        &self,
        request: Request<Empty>,
        ) -> Result<Response<GetInfoResponse>, Status> {
        info!("WordFrequencyService.get_info from {:?} request:{:?}", request.remote_addr(), request.get_ref());

        let mut peers_addr = Vec::new();
        if self.peers.is_some() {
            peers_addr = self.peers.as_ref().unwrap().addrs.clone();
        }

        let response = wordfreq::GetInfoResponse {
            ready: true,
            max_key: self.max_key,
            key_count: self.keys.len() as i64,
            peers: peers_addr,
        };

        Ok(Response::new(response))
    }
    
    async fn quit(
        &self,
        request: Request<Empty>,
        ) -> Result<Response<Empty>, Status> {
        info!("WordFrequencyService.quit from {:?} request:{:?}", request.remote_addr(), request.get_ref());

        let response = wordfreq::Empty{};

        if let Err(e) = self.quit_tx.send(()).await {
            error!("quit_tx receiver dropped {}", e);
        }

        Ok(Response::new(response))
    }
    
    async fn get_histogram(
        &self,
        request: Request<GetHistogramRequest>,
        ) -> Result<Response<GetHistogramResponse>, Status> {
        info!("WordFrequencyService.get_histogram from {:?} request:{:?}", request.remote_addr(), request.get_ref());

        let request = request.get_ref();
        assert!(is_sorted_ord(&(request.pivots)));

        let mut response = wordfreq::GetHistogramResponse{
            counts: Vec::with_capacity(request.pivots.len()+1),
        };
        for i in 0..request.pivots.len()+1 {
            response.counts[i] = 0;
        }

        for i in 0..self.keys.len() {
            let idx = request.pivots.lower_bound(&self.keys[i]);
            assert!(idx < response.counts.len());
            response.counts[idx] += 1;
        }

        if let Err(e) = self.quit_tx.send(()).await {
            error!("quit_tx receiver dropped {}", e);
        }

        Ok(Response::new(response))
    }

    async fn shuffle_key(
        &self,
        request: Request<ShuffleKeyRequest>,
        ) -> Result<Response<ShuffleKeyResponse>, Status> {
        info!("WordFrequencyService.shuffle_key from {:?} request:{:?}", request.remote_addr(), request.get_ref());

        let req = request.get_ref();
        let mut response = Response::new(ShuffleKeyResponse{
            error: "Shuffling".to_string(),
            elapsed: 0.0,
        });

        let shuffling: bool;
        {
            let guard = self.shared.lock().unwrap();
            shuffling = (*guard).shuffling;
        }

        let peers_len: usize;
        if self.peers.is_some() {
            peers_len = self.peers.as_ref().unwrap().addrs.len();
        } else {
            peers_len = 0;
        }

        if !shuffling && self.peers.is_some()
            && req.pivots.len() == peers_len
                && *(req.pivots.last().unwrap()) >= self.max_key
                && is_sorted_ord(&req.pivots) {
                    {
                        let mut guard = self.shared.lock().unwrap();
                        if !(*guard).shuffling {
                            return Ok(response);
                        } else {
                            (*guard).shuffling = true;
                        }
                    }

                    let res = self.shuffle(request).await;
                    match res {
                        Ok(resp) => {
                            return Ok(resp);
                        }
                        Err(_) => {
                            return Ok(response);
                        }
                    }
                } else {
                    info!("ShuffleKey");

                    if !shuffling {
                        response.get_mut().error = "Invalid pivots".to_string();
                    }
                }

        Ok(response)
    }

    async fn shard_key(
        &self,
        request: Request<ShardKeyRequest>,
        ) -> Result<Response<Empty>, Status> {
        info!("WordFrequencyService.shard_key from {:?} request:{:?}", request.remote_addr(), request.get_ref());

        let request = request.get_ref();
        let mut guard = self.shared.lock().unwrap();
        if (*guard).partition == -1 {
            (*guard).partition = request.partition;
            assert!((*guard).partition_keys.is_empty());
        } else if (*guard).partition != request.partition {
            error!("Wrong partition, was {} now {}", (*guard).partition, request.partition);
        }

        (*guard).partition_keys.extend_from_slice(&request.keys);
        Ok(Response::new(wordfreq::Empty{}))
    }

    async fn sort_key(
        &self,
        request: Request<Empty>,
        ) -> Result<Response<SortKeyResponse>, Status> {
        info!("WordFrequencyService.sort_key from {:?} request:{:?}", request.remote_addr(), request.get_ref());

        let shared: Shared;
        {
            let mut guard = self.shared.lock().unwrap();
            (*guard).partition_keys.sort();
            shared = (*guard).clone();
        }

        let f = OpenOptions::new()
            .append(true)
            .create(true)
            .open("/tmp/partition");
        if let Ok(mut file) = f {
            for key in shared.partition_keys.iter() {
                if let Err(e) = write!(&mut file, "{}\n", key) {
                    error!("write file error key:{} error:{}", key, e);
                }
            }
        }

        let mut response = wordfreq::SortKeyResponse{
            partition: shared.partition,
            count: shared.partition_keys.len() as i64,
            min_key: None,
            max_key: None,
        };
        if !shared.partition_keys.is_empty() {
            response.min_key = Some(*(shared.partition_keys.first().unwrap()));
            response.max_key = Some(*(shared.partition_keys.last().unwrap()));
        }
        Ok(Response::new(response))
    }
}


fn read_keys(file_name: &str) -> std::result::Result<(Vec<i64>, i64), Error> {
    let file = File::open(file_name)?;
    let reader = BufReader::new(file);

    let mut keys: Vec<i64> = Vec::new();
    let mut max_key: i64 = 0;
    for line in reader.lines() {
        if let Ok(line) = line {
            let res: Vec<&str> = line.as_str().split('\t').collect();
            if res.len() < 2 {
                return Ok((keys, max_key));
            }

            let key: i64 = res[1].parse()?;
            if key > 0 {
                keys.push(key);
                if key > max_key {
                    max_key = key; 
                }
            }
        }
    }
    
    Ok((keys, max_key))
}


#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    /// listen port
    #[clap(short, long)]
    #[clap(default_value_t = String::from("127.0.0.1:6000"))]
    addr: String,

    /// shard file 
    #[clap(short, long)]
    #[clap(default_value_t = String::from("/tmp/shard"))]
    shard_file: String,
    
    /// peer list 
    #[clap(short, long)]
    #[clap(default_value_t = String::from("127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003,127.0.0.1:6004"))]
    peer_list: String,
}

struct Peers {
    tx: Vec<Tx>,
    addrs: Vec<String>,
    free_shards: BlockingQueue<()>, 
}

impl Peers {
    fn new() -> Peers {
        Peers {
            tx: Vec::new(),
            addrs: Vec::new(),
            free_shards: BlockingQueue::new(),
        }
    }

    fn len(&self) -> usize {
       self.addrs.len() 
    }
}

async fn create_peers(peer_list: String, quit_tx: mpsc::Sender<()>) -> std::result::Result<Peers, Error> {
    let peer_list: Vec<&str> = peer_list.split(',').collect();
    let (connected_tx, mut connected_rx) = mpsc::channel::<()>(peer_list.len());

    let mut peers = Peers::new();

    for addr in peer_list.iter() {
        peers.addrs.push(addr.to_string());

        let (tx, mut rx) = mpsc::unbounded_channel();
        peers.tx.push(tx);

        let quit_tx_clone = quit_tx.clone();
        let connected_tx_clone = connected_tx.clone();
        let free_shards_clone = peers.free_shards.clone();
        let addr = format!("http://{}", addr);

        tokio::spawn(async move {
            loop {
                let res = WordFrequencyServiceClient::connect(addr.clone()).await;
                match res {
                    Err(e) => {
                        error!("WordFrequencyServiceClient addr:{} error:{}", addr, e);
                        sleep(Duration::from_millis(500)).await;
                    }
                    Ok(mut client) => {
                        info!("WordFrequencyServiceClient addr:{}", addr);
                        drop(connected_tx_clone);

                        loop {
                            tokio::select! {
                                request = rx.recv() => {
                                    if let Some(req) = request {
                                        // Empty
                                        let response = client.shard_key(tonic::Request::new(req)).await;
                                        free_shards_clone.put(());

                                        match response {
                                            Ok(res) => {
                                                info!("client.shard_key response:{:?}", res.get_ref());
                                            },
                                            Err(e) => {
                                                info!("client.shard_key error:{:?}", e);
                                                break; 
                                            }
                                        }
                                    } else {
                                        error!("rx.recv none request");
                                        break;
                                    }
                                }
                            }
                        }
                        break;
                    }
                }
            }

            let _ = quit_tx_clone.send(());
        });
    }

    drop(connected_tx);
    let _ = connected_rx.recv();

    Ok(peers)
}

#[tokio::main]
async fn main() -> std::result::Result<(), Error> {
    tracing_subscriber::fmt::init();

    let options = Options::parse();
    info!("WordFreq listening on {}", options.addr);

    let (keys, max_key) = read_keys(&(options.shard_file))?;
    let (quit_tx, mut quit_rx) = mpsc::channel(1);
    
    let mut peers: Option<Peers> = None;
    if !options.peer_list.is_empty() {
        let res = create_peers(options.peer_list, quit_tx.clone()).await;
        match res {
            Err(e) => {
                return Err(e.into());
            }
            Ok(p) => {
                peers = Some(p);
            }
        }
    }

    let wordfreq_service = TheWordFrequencyService::new(keys, max_key, quit_tx.clone(), peers);
    let addr = options.addr.parse().unwrap();
    let serve = Server::builder()
        .add_service(WordFrequencyServiceServer::new(wordfreq_service))
        .serve(addr);

    tokio::spawn(async move {
        if let Err(e) = serve.await {
            error!("spawn serve Error = {:?}", e);
        }

        if let Err(e) = quit_tx.send(()).await {
            error!("quit_tx receiver dropped {}", e);
        }
    });

    let _ = quit_rx.recv().await;

    Ok(())
}
