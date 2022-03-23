use muduo_rust::Result;

use tracing_subscriber;
use tracing::{error, info};
use clap::Parser;
use tokio::sync::{broadcast, mpsc};
use std::time::{Duration, SystemTime};
use std::collections::{HashSet, HashMap};
use tokio::time;
use chrono::{DateTime, Utc};
use tokio_util::codec::{Framed, LinesCodec};
use tokio_stream::StreamExt;
use futures::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

type Tx = mpsc::UnboundedSender<String>;
type Rx = mpsc::UnboundedReceiver<String>;

struct Topic {
    topic: String,
    content: String,
    last_pub_time: SystemTime,
    audiences: HashMap<String, Tx>,
}

impl Topic {
    fn new(t: &str) -> Topic {
        Topic {
            topic: t.to_string(),
            content: String::new(),
            last_pub_time: SystemTime::UNIX_EPOCH,
            audiences: HashMap::new(),
        }
    }

    // Frame LineCodec will append \r\n
    fn make_message(&self) -> String {
        format!("pub {}\r\n{}", self.topic, self.content)
    }

    fn add(&mut self, session_name: &str, tx: Tx) -> Result<()> {
        self.audiences.insert(session_name.to_string(), tx.clone());
        if self.last_pub_time != SystemTime::UNIX_EPOCH {
            let _ = tx.send(self.make_message()).unwrap();
        }
        Ok(())
    }

    fn remove(&mut self, session_name: &str) {
        self.audiences.remove(&session_name.to_string());
    }

    fn publish(&mut self, content: String, time: SystemTime) -> Result<()> {
        self.content = content;
        self.last_pub_time = time;
        let message = self.make_message();
        for (_, tx) in self.audiences.iter() {
            let _ = tx.send(message.clone()).unwrap();
        }
        Ok(())
    }
}

struct Shared {
    topics: HashMap<String, Topic>,
}

impl Shared {
    fn new() -> Shared {
        Shared {
            topics: HashMap::new(),
        }
    }
    fn time_publish(&mut self) -> Result<()> {
        let topic = "utc_time".to_string();
        let now = SystemTime::now();
        let datetime: DateTime<Utc> = now.into();
        let content = format!("{}", datetime.format("%Y/%m/%d %T"));

        if !self.topics.contains_key(&topic) {
            self.topics.insert(topic.clone(), Topic::new(&topic));
        }

        if let Some(ref mut topic_instance) = self.topics.get_mut(&topic) {
            let _ = (*topic_instance).publish(content, now).unwrap();
        }
        Ok(())
    }

    fn do_unsubscribe(&mut self, session_name: &str, topic: &str) {
        if let Some(ref mut topic_instance) = self.topics.get_mut(&topic.to_string()) {
            (*topic_instance).remove(session_name);
        }
    }

    fn do_subscribe(&mut self, session_name: &str, topic: &str, tx: Tx) -> Result<()> {
        let topic_string = topic.to_string();
        if !self.topics.contains_key(&topic_string) {
            self.topics.insert(topic_string, Topic::new(topic));
        }

        if let Some(ref mut topic_instance) = self.topics.get_mut(&topic.to_string()) {
            let _ = (*topic_instance).add(session_name, tx).unwrap();
        }
        Ok(())
    }

    fn do_publish(&mut self, topic: &str, content: String, time: SystemTime) -> Result<()> {
        let topic_string = topic.to_string();
        if !self.topics.contains_key(&topic_string) {
            self.topics.insert(topic_string, Topic::new(topic));
        }

        if let Some(ref mut topic_instance) = self.topics.get_mut(&topic.to_string()) {
            let _ = (*topic_instance).publish(content, time).unwrap();
        }
        Ok(())
    }
}

struct Session {
    name: String,
    lines: Framed<TcpStream, LinesCodec>,
    notify_shutdown: broadcast::Receiver<()>,
    _shutdown_complete_tx: mpsc::Sender<()>,
    shared: Arc<Mutex<Shared>>,
    subscriptions: HashSet<String>,
    tx: Tx,
    rx: Rx,
}

impl Session {
    fn new(name: String, lines: Framed<TcpStream, LinesCodec>, notify_shutdown: broadcast::Receiver<()>, shutdown_complete_tx: mpsc::Sender<()>, shared: Arc<Mutex<Shared>>, tx: Tx, rx: Rx) -> Session {
        Session {
            name,
            lines,
            notify_shutdown,
            _shutdown_complete_tx: shutdown_complete_tx,
            shared,
            subscriptions: HashSet::new(),
            tx,
            rx,
        }
    }
    
    pub async fn run(&mut self) -> Result<()> {
        info!("{} {} -> {} is UP", self.name, self.lines.get_ref().local_addr().unwrap(), self.lines.get_ref().peer_addr().unwrap());
       
        let mut pub_topic = String::new();
        loop {
            tokio::select! {
                line = self.lines.next() => match line {
                    Some(Ok(request)) => {
                        // pub [topic]\r\n[content]\r\n
                        if !pub_topic.is_empty() {
                            // request as content
                            let _ = self.do_publish(&pub_topic, request, SystemTime::now()).unwrap(); 
                            pub_topic.clear();
                        } else {
                            let req: Vec<&str> = request.split(' ').collect();
                            if req.len() < 2 {
                                return Ok(());  // shutdown
                            }

                            if req[0] == "pub" {
                                pub_topic = req[1].to_string();
                            } else if req[0] == "sub" {
                                let _ = self.do_subscribe(req[1]).unwrap();
                            } else if req[0] == "unsub" {
                                self.do_unsubscribe(req[1]);
                            } else {
                                return Ok(());  // shutdown
                            }
                        }
                    }
                    Some(Err(e)) => {
                        tracing::info!(
                            "{} an error occurred while session processing messages error = {:?}",
                            self.name, e);
                    }
                    None => break,
                },
              content = self.rx.recv() => {
                let _ = self.lines.send(content.unwrap()).await?;     
              },
              _ = self.notify_shutdown.recv() => {
                return Ok(());
              }
            }
        }

        Ok(())
    }

    fn do_unsubscribe(&mut self, topic: &str) {
        info!("{} unscribes {}", self.name, topic);
        self.subscriptions.remove(&topic.to_string());
   
        let mut guard = self.shared.lock().unwrap();
        (*guard).do_unsubscribe(&self.name, topic);
    }

    fn do_subscribe(&mut self, topic: &str) -> Result<()> {
        info!("{} scribes {}", self.name, topic);
        self.subscriptions.insert(topic.to_string());

        let mut guard = self.shared.lock().unwrap();
        (*guard).do_subscribe(&self.name, topic, self.tx.clone())
    }

    fn do_publish(&mut self, topic: &str, content: String, time: SystemTime) -> Result<()> {
        let mut guard = self.shared.lock().unwrap();
        (*guard).do_publish(topic, content, time)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let mut guard = self.shared.lock().unwrap();
        for topic in &self.subscriptions {
            (*guard).do_unsubscribe(&self.name, &topic);
        }
        info!("{} {} -> {} is DOWN", self.name, self.lines.get_ref().local_addr().unwrap(), self.lines.get_ref().peer_addr().unwrap());
    }
}

struct PubSubServer {
    listener: TcpListener,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    shared: Arc<Mutex<Shared>>,
}

impl PubSubServer {
    pub async fn run(&mut self) -> Result<()> {
        info!("accepting inbound connections");

        let shared = self.shared.clone();
        tokio::spawn(async move {
            let shared_clone = shared.clone();
            let mut interval = time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let mut guard = shared_clone.lock().unwrap();
                        let _ = (*guard).time_publish().unwrap();
                    }
                }
            }
        });

        let mut id: i64 = 0;
        loop {
            let socket = self.accept().await?;
            socket.set_nodelay(true).expect("set_nodelay failed");

            id += 1;
            let (tx, rx) = mpsc::unbounded_channel();
            let mut session = Session::new(
                format!("session-{}", id),
                Framed::new(socket, LinesCodec::new()),
                self.notify_shutdown.subscribe(),
                self.shutdown_complete_tx.clone(),
                self.shared.clone(),
                tx,
                rx);

            tokio::spawn(async move {
                if let Err(err) = session.run().await {
                    error!(cause = ?err, "{} error", session.name);
                }
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

pub async fn run(options: Options) {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", options.port)).await.unwrap();
    info!("start listening.. port:{}", options.port);

    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    let mut server = PubSubServer {
        listener,
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
        shared: Arc::new(Mutex::new(Shared::new())),
    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
    }

    let PubSubServer {
        notify_shutdown,
        shutdown_complete_tx,
        mut shutdown_complete_rx,
        ..
    } = server;

    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Options {
    /// TCP port
    #[clap(short, long)]
    #[clap(default_value_t = 11211)]
    pub port: u16,
}


#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    run(Options::parse()).await;
}
