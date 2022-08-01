use muduo_rust::{Result, accept_backoff};
use tracing_subscriber;
use tracing::{error, info};
use std::process;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec, BytesCodec};
use bytes::{Bytes, BytesMut, Buf, BufMut};
use futures::SinkExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::collections::VecDeque;

const MAX_CONNS: i32 = 10;
const MAX_PACKET_LEN: usize = 255;

const CLIENT_PORT: u16 = 3333;
const BACKEND_PORT: u16 = 9999;

type Tx = mpsc::UnboundedSender<Bytes>;
type ShutdownTx = mpsc::Sender<()>;

struct ClientConn {
    client_conn_tx: Tx,
    client_shutdown_tx: ShutdownTx,
}

impl ClientConn {
    fn new(client_conn_tx: Tx, client_shutdown_tx: ShutdownTx) -> Self {
        ClientConn {
            client_conn_tx,
            client_shutdown_tx,
        }
    }
}

struct Shared {
    backend_conn_tx: Option<Tx>,
    client_conns: HashMap<i32, ClientConn>,
    avail_ids: VecDeque<i32>,
    transferred: usize,
    received_messages: usize,
}

impl Shared {
    fn new(tx: Tx) -> Self {
        Shared {
            backend_conn_tx: Some(tx),
            client_conns: HashMap::new(),
            avail_ids: VecDeque::new(),
            transferred: 0,
            received_messages: 0,
        }
    }

    async fn send_to_client(&mut self, conn_id: i32, message: Bytes) {
        for client_conn in self.client_conns.iter_mut() {
            if *client_conn.0 == conn_id {
                let _ = client_conn.1.client_conn_tx.send(message);
                break;
            }
        }
    }
    
    async fn send_to_backend(&mut self, message: Bytes) {
        if let Some(ref backend_conn_tx) = self.backend_conn_tx {
            let _ = backend_conn_tx.send(message);
        }
    }

    async fn shutdown(&mut self) {
        self.backend_conn_tx = None;
        for client_conn in self.client_conns.iter_mut() {
            if let Err(_) = client_conn.1.client_shutdown_tx.send(()).await {
                error!("{} client_shutdown_rx receiver dropped.", client_conn.0);
            }
        }
        self.client_conns.clear();
        while !self.avail_ids.is_empty() {
            self.avail_ids.pop_front();
        }
    }
}

async fn send_backend_packet(state: Arc<Mutex<Shared>>, conn_id: i32, buf: BytesMut) {
    assert!(buf.len() <= MAX_PACKET_LEN);

    let mut packet = BytesMut::with_capacity(buf.len() + 2);
    packet.put_u8((conn_id & 0xFF) as u8);
    packet.put_u8(((conn_id & 0xFF00) >> 8) as u8);
    packet.extend(buf);
    {
        let mut state = state.lock().await;
        state.send_to_backend(packet.freeze()).await;
    }
}

async fn process_client_connection(stream: TcpStream, state: Arc<Mutex<Shared>>, limit_connections: Arc<Semaphore>) -> Result<()> {
    let mut frame = Framed::new(stream, BytesCodec::new());
    let (client_conn_tx, mut client_conn_rx) = mpsc::unbounded_channel();
    let (shutdown_client_tx, mut shutdown_client_rx) = mpsc::channel(1);

    let peer_addr: String = frame.get_ref().peer_addr().unwrap().to_string();
    info!("Client {} -> {} is UP", peer_addr, frame.get_ref().local_addr()?);
    let mut conn_id = -1 as i32;
    {
        let mut state = state.lock().await;
        if !state.avail_ids.is_empty() {
            if let Some(front) = state.avail_ids.pop_front() {
                conn_id = front; 
                state.client_conns.insert(conn_id, ClientConn::new(client_conn_tx, shutdown_client_tx));
            } else {
                return Ok(());
            }
        }
    }

    let mut msg = BytesMut::with_capacity(256);
    let msg_str = format!("CONN {} FROM {} IS UP\r\n", conn_id, frame.get_ref().peer_addr()?);
    msg.put(msg_str.as_bytes());
    send_backend_packet(state.clone(), 0, msg).await;

    let state_clone = state.clone();
    loop {
        tokio::select! {
            _ = shutdown_client_rx.recv() => break, 
            Some(msg) = client_conn_rx.recv() => {
                frame.send(msg).await?;
            },
            result = frame.next() => match result {
                Some(Ok(mut msg)) => {
                    {
                        let mut state = state.lock().await;
                        state.transferred += msg.len();
                        state.received_messages += 1;
                    }

                    while msg.len() > MAX_PACKET_LEN {
                        send_backend_packet(state_clone.clone(), conn_id, msg.split_to(MAX_PACKET_LEN)).await;
                    }
                    if msg.len() > 0 {
                        send_backend_packet(state_clone.clone(), conn_id, msg).await;
                    }
                }
                Some(Err(e)) => {
                    error!(
                        "an error occurred while process_client_message error = {:?}",
                        e
                        );
                }
                // The stream has been exhausted.
                None => break,
            }
        }
    }
    
    info!("Client {} -> {} is DOWN", peer_addr, frame.get_ref().local_addr()?);

    let mut client_connected = false;
    {
        let mut state = state.lock().await;
        if state.client_conns.get(&conn_id).is_some() {
            client_connected = true; 
        }

        if state.backend_conn_tx.is_some() {
            state.avail_ids.push_back(conn_id);
            state.client_conns.remove(&conn_id);
        } else {
            assert!(state.avail_ids.is_empty()); 
            assert!(state.client_conns.is_empty()); 
        }
    }

    if client_connected {
        let mut msg = BytesMut::with_capacity(256);
        msg.put(format!("CONN {} FROM {} IS DOWN\r\n", conn_id, peer_addr).as_bytes());
        send_backend_packet(state.clone(), 0, msg).await;
    }
    limit_connections.add_permits(1);

    Ok(())
}

async fn accept_loop(state: Arc<Mutex<Shared>>) -> Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", CLIENT_PORT)).await?;
    info!("start listening.. port:{}", CLIENT_PORT);

    let max_conns: usize;
    {
        let state = state.lock().await;
        max_conns = state.avail_ids.len();
    }

    let limit_connections = Arc::new(Semaphore::new(max_conns));

    loop {
        limit_connections.acquire().await.unwrap().forget();

        let socket = accept_backoff(&listener).await?;
        let _ = socket.set_nodelay(true)?;

        let state_clone = state.clone();
        let limit_connections_clone = limit_connections.clone();
        tokio::spawn(async move {
            if let Err(err) = process_client_connection(socket, state_clone, limit_connections_clone).await {
                error!(cause = ?err, "process_client_connection error");
            }
        });
    }
}

async fn send_to_client(state: Arc<Mutex<Shared>>, mut msg: BytesMut) {
    let high = msg.get_u8() as i32;
    let low = msg.get_u8() as i32;
    let conn_id = high + (low << 8);

    {
        let mut state = state.lock().await;
        state.transferred += msg.len() + 2;
        state.received_messages += 1;
        state.send_to_client(conn_id, msg.freeze()).await; 
    }
}

async fn process_backend_connection() -> Result<()> {
    // backend
    let stream = TcpStream::connect(format!("127.0.0.1:{}", BACKEND_PORT)).await?;
    let mut frame = Framed::new(stream, LengthDelimitedCodec::builder()
                                .length_field_offset(0)
                                .length_field_length(1)
                                .length_adjustment(2)
                                .new_codec());
    let (backend_conn_tx, mut backend_conn_rx) = mpsc::unbounded_channel();

    info!("Backend {} -> {} is UP", frame.get_ref().local_addr()?, frame.get_ref().peer_addr()?);
    let state = Arc::new(Mutex::new(Shared::new(backend_conn_tx)));
    {
        let mut state = state.lock().await;
        for i in 1 .. MAX_CONNS {
            state.avail_ids.push_back(i);
        }
    }
    let state_clone = state.clone();

    tokio::spawn(async move {
        if let Err(err) = accept_loop(state_clone).await {
            error!(cause = ?err, "accept_loop error");
        }
    });

    loop {
        tokio::select! {
            Some(msg) = backend_conn_rx.recv() => {
                frame.send(msg).await?;
            },
            bytes = frame.next() => match bytes {
                Some(Ok(msg)) => {
                    send_to_client(state.clone(), msg).await;
                },
                Some(Err(e)) => {
                    error!("an error occurred while process_backend_connection  error = {:?}", e);
                },
                None => {
                    break;
                }
            }
        }
    }

    info!("Backend {} -> {} is DOWN", frame.get_ref().local_addr()?, frame.get_ref().peer_addr()?);
    {                     
        let mut state = state.lock().await;
        state.shutdown().await; 
    } 

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("pid = {}", process::id());

    let _ = process_backend_connection().await?;

    Ok(())
}
