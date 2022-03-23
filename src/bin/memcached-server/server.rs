use crate::item::{UpdatePolicy, Item};
use crate::session::{Session, State, Protocol, LONGEST_KEY_SIZE};
use muduo_rust::Result;

use tracing::{error, info};
use tokio::sync::{broadcast, mpsc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use tokio::time::{self, Duration};
use std::sync::{Arc, Mutex};
use tokio_util::codec::{Framed, LinesCodec};
use tokio::net::{TcpListener, TcpStream};
use std::collections::HashSet;

const SHARDS: usize = 4096;
const LONGEST_KEY: &'static [u8] = &[b'x'; LONGEST_KEY_SIZE];

type ItemMap = Arc<Mutex<HashSet<Arc<Item>>>>;

pub struct Shared {
    // stats not implemented
    start_time: SystemTime,
    shards: Vec<ItemMap>,
    cas: AtomicU64,
}

impl Shared {
    pub fn new() -> Shared {
        let mut shards = Vec::with_capacity(SHARDS);
        for _ in 0..SHARDS {
            shards.push(Arc::new(Mutex::new(HashSet::new())));
        }

        Shared {
            start_time: SystemTime::now(),
            shards,
            cas: AtomicU64::new(0),
        }
    }

    pub fn start_time(&self) -> u64 {
        self.start_time.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as u64 - 1
    }
    
    pub fn store_item(&self, item: &mut Arc<Item>, policy: UpdatePolicy, exists: &mut bool) -> bool {
        assert!(item.needed_bytes() == 0);

        let mut items = self.shards[item.hash() as usize % SHARDS].lock().unwrap();
        let mut orig_item: Option<Arc<Item>> = None;
        *exists = false;
        if let Some(orig_item_res) = items.get(item) {
            orig_item = Some(orig_item_res.clone()); 
            *exists = true;
        }

        if policy == UpdatePolicy::Set {
            Arc::get_mut(item).unwrap().set_cas(self.cas.fetch_add(1, Ordering::SeqCst)+1);
            if *exists {
                items.remove(&orig_item.unwrap());
            }
            items.insert(item.clone());
        } else {
            if policy == UpdatePolicy::Add {
                if *exists {
                    return false;
                } else {
                    Arc::get_mut(item).unwrap().set_cas(self.cas.fetch_add(1, Ordering::SeqCst)+1);
                    items.insert(item.clone());
                } 
            } else if policy == UpdatePolicy::Replace {
                if *exists {
                    Arc::get_mut(item).unwrap().set_cas(self.cas.fetch_add(1, Ordering::SeqCst)+1);
                    items.remove(&orig_item.unwrap());
                    items.insert(item.clone());
                } else {
                    return false;
                }
            } else if policy == UpdatePolicy::Append || policy == UpdatePolicy::Prepend {
                if *exists {
                    let orig_item = orig_item.unwrap();
                    let item = Arc::get_mut(item).unwrap();
                    let new_len = item.value_length() + orig_item.value_length() - 2;
                    let mut new_item = Item::new(&item.data[0..item.key_len()],
                                             orig_item.flags(),
                                             orig_item.rel_exptime(),
                                             new_len,
                                             self.cas.fetch_add(1, Ordering::SeqCst)+1);
                    if policy == UpdatePolicy::Append {
                        Arc::get_mut(&mut new_item).unwrap().append(&orig_item.data[orig_item.key_len()..orig_item.total_len()-2]);
                        Arc::get_mut(&mut new_item).unwrap().append(&item.data[item.key_len()..item.total_len()]);
                    } else {
                        Arc::get_mut(&mut new_item).unwrap().append(&item.data[item.key_len()..item.total_len()-2]);
                        Arc::get_mut(&mut new_item).unwrap().append(&orig_item.data[orig_item.key_len()..orig_item.total_len()]);
                    }

                    assert!(new_item.needed_bytes() == 0);
                    assert!(new_item.ends_with_crlf());

                    items.remove(&orig_item);
                    items.insert(new_item); 
                } else {
                    return false;
                }
            } else if policy == UpdatePolicy::Cas {
                if *exists {
                    let orig_item = orig_item.unwrap();
                    if orig_item.cas() == item.cas() {
                        Arc::get_mut(item).unwrap().set_cas(self.cas.fetch_add(1, Ordering::SeqCst)+1);
                        items.remove(&orig_item);
                        items.insert(item.clone());
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                assert!(false);
            }
        }
        true
    }

    pub fn get_item(&self, key: &Arc<Item>) -> Option<Arc<Item>> {
        let items = self.shards[key.hash() as usize % SHARDS].lock().unwrap();
        if let Some(item) = items.get(key) {
            Some(item.clone())
        } else {
            None
        }
    }

    pub fn delete_item(&self, key: &Arc<Item>) -> bool {
        let mut items = self.shards[key.hash() as usize % SHARDS].lock().unwrap();
        items.remove(key)
    }
}

pub struct MemcacheServer {
    pub listener: TcpListener,
    pub notify_shutdown: broadcast::Sender<()>,
    pub server_shutdown_tx: mpsc::Sender<()>,
    pub shutdown_complete_tx: mpsc::Sender<()>,
    pub shutdown_complete_rx: mpsc::Receiver<()>,
    pub shared: Arc<Shared>,
}

impl MemcacheServer {

    pub async fn run(&mut self) -> Result<()> {
        info!("accepting inbound connections");

        let mut id: i64 = 0;
        loop {
            let socket = self.accept().await?;
            socket.set_nodelay(true).expect("set_nodelay failed");

            id += 1;
            let shared = self.shared.clone();
            let mut session = Session {
                name: format!("connection-{}", id),
                lines: Framed::new(socket, LinesCodec::new()),
                notify_shutdown: self.notify_shutdown.subscribe(),
                server_shutdown_tx: self.server_shutdown_tx.clone(),
                _shutdown_complete_tx: self.shutdown_complete_tx.clone(),
                shared,
                state: State::NewCommand,
                protocol: Protocol::Ascii,
               
                command: String::new(),
                noreply: false,
                policy: UpdatePolicy::Invalid,
                curr_item: None,
                bytes_to_discard: 0,

                needle: Item::new(&LONGEST_KEY, 0, 0, 2, 0),
                output_buf: String::new(),

                bytes_read: 0,
                requests_processed: 0,
            };
            info!("{} {} -> {} is UP", session.name, session.lines.get_ref().local_addr()?, session.lines.get_ref().peer_addr()?);

            tokio::spawn(async move {
                if let Err(err) = session.run().await {
                    error!(cause = ?err, "{} error", session.name);
                }
                info!("requests processed: {} ", session.requests_processed);
            });
        }
    }

    async fn accept(&mut self) -> Result<TcpStream> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        return Err(err.into());
                    }
                }
            }

            time::sleep(Duration::from_secs(backoff)).await;

            backoff *= 2;
        }
    }
}
