use muduo_rust::{Result, accept_backoff};
use tracing_subscriber;
use tracing::{error, info};
use std::process;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec, BytesCodec};
use bytes::{Bytes, BytesMut, Buf, BufMut};
use futures::SinkExt;
use std::collections::HashMap;
use std::sync::Arc;

const MAX_PACKET_LEN: usize = 255;

const LISTEN_PORT: u16 = 9999;
const SOCKS_PORT: u16 = 7777;
const CONN: &'static str = "CONN ";

type Tx = mpsc::UnboundedSender<Bytes>;
type Rx = mpsc::UnboundedReceiver<Bytes>;
type ShutdownTx = mpsc::Sender<()>;
type ShutdownRx = mpsc::Receiver<()>;


struct SocksConn {
    socks_conn_tx: Tx,
    socks_shutdown_tx: ShutdownTx, 
}

impl SocksConn {
    fn new(socks_conn_tx: Tx, socks_shutdown_tx: ShutdownTx) -> Self {
        SocksConn {
            socks_conn_tx,
            socks_shutdown_tx,
        }
    }
}

struct Shared {
    server_conn_tx: Option<Tx>,
    socks_conns: HashMap<i32, SocksConn>,
}

impl Shared {
    fn new() -> Self {
        Shared {
            server_conn_tx: None,
            socks_conns: HashMap::new(),
        }
    }

    async fn send_to_sock(&mut self, conn_id: i32, message: Bytes) {
        for socks_conn in self.socks_conns.iter_mut() {
            if *socks_conn.0 == conn_id {
                let _ = socks_conn.1.socks_conn_tx.send(message);
                break;
            }
        }
    }
    
    async fn send_to_server(&mut self, message: Bytes) {
        if let Some(ref server_conn_tx) = self.server_conn_tx {
            let _ = server_conn_tx.send(message);
        }
    }

    async fn clear(&mut self) {
        self.server_conn_tx = None;
        for socks_conn in self.socks_conns.iter_mut() {
            if let Err(_) = socks_conn.1.socks_shutdown_tx.send(()).await {
                error!("socks_shutdown_tx receiver dropped.");
            }
        }
    }
}

async fn process_server_connection(stream: TcpStream, state: Arc<Mutex<Shared>>) -> Result<()> {
    let mut frame = Framed::new(stream, LengthDelimitedCodec::builder()
                                .length_field_offset(0)
                                .length_field_length(1)
                                .length_adjustment(2)
                                .new_codec());
    let (server_conn_tx, mut server_conn_rx) = mpsc::unbounded_channel();
    {                     
        let mut state = state.lock().await;
        state.server_conn_tx = Some(server_conn_tx); 
    } 

    loop {
        tokio::select! {
            Some(msg) = server_conn_rx.recv() => {
                frame.send(msg).await?;
            },
            bytes = frame.next() => match bytes {
                Some(Ok(mut msg)) => {
                    let high = msg.get_u8() as i32;
                    let low = msg.get_u8() as i32;
                    let conn_id = high + (low << 8);

                    if conn_id != 0 {
                        let mut state = state.lock().await;
                        assert!(state.socks_conns.contains_key(&conn_id) == true); 
                        state.send_to_sock(conn_id, msg.freeze()).await;
                    } else {
                        do_command(state.clone(), String::from_utf8(msg.chunk().to_vec()).unwrap()).await;
                    }
                },
                Some(Err(e)) => {
                    error!("an error occurred while process_server_connection  error = {:?}", e);
                },
                None => {
                    break;
                }
            }
        }
    }

    {                     
        let mut state = state.lock().await;
        state.clear().await;
    } 
    info!("on server connection reset server_conn");

    Ok(())
}

async fn send_server_packet(state: Arc<Mutex<Shared>>, conn_id: i32, buf: Bytes) {
   info!("conn_id:{} send_server_packet buf.len:{}", conn_id, buf.len());
   assert!(buf.len() <= MAX_PACKET_LEN);

   let mut packet = BytesMut::with_capacity(2 + buf.len());
   packet.put_u8((conn_id & 0xFF) as u8);
   packet.put_u8(((conn_id & 0xFF00) >> 8) as u8);
   packet.extend(buf);
   {
       let mut state = state.lock().await;
       state.send_to_server(packet.freeze()).await;
   }
}

async fn process_socks_message(state: Arc<Mutex<Shared>>, mut frame: Framed<TcpStream, BytesCodec>, conn_id: i32, 
                               mut socks_conn_rx: Rx, mut shutdown_socks_rx: ShutdownRx) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown_socks_rx.recv() => break, 
            Some(msg) = socks_conn_rx.recv() => {
                frame.send(msg).await?;
            },
            result = frame.next() => match result {
                Some(Ok(msg)) => {
                    let mut msg = msg.freeze();
                    while msg.len() > MAX_PACKET_LEN {
                        send_server_packet(state.clone(), conn_id, msg.split_to(MAX_PACKET_LEN)).await;
                    }
                    if msg.len() > 0 {
                        send_server_packet(state.clone(), conn_id, msg).await; 
                    }
                }
                Some(Err(e)) => {
                    error!(
                        "an error occurred while process_socks_message error = {:?}",
                        e
                        );
                }
                // The stream has been exhausted.
                None => break,
            }
        }
    }

    let state_clone = state.clone();
    {
        let mut state = state.lock().await;
        if state.server_conn_tx.is_some() {
            let mut msg = BytesMut::with_capacity(256);
            msg.put(format!("DISCONNECT {}\r\n", conn_id).as_bytes());
            send_server_packet(state_clone, 0, msg.freeze()).await; 
        } else {
            state.socks_conns.remove(&conn_id);
        }
    }

    Ok(())
}

async fn do_command(state: Arc<Mutex<Shared>>, command: String) {
    let conn_id: i32 = command[CONN.len()..].parse().unwrap();
    let is_up = command.contains(" IS UP");
    info!("do_command conn_id:{} is_up:{}", conn_id, is_up);
        
    if is_up {
        if let Ok(stream) = TcpStream::connect(format!("127.0.0.1:{}", SOCKS_PORT)).await {
            let frame = Framed::new(stream, BytesCodec::new());
            let (socks_conn_tx, socks_conn_rx) = mpsc::unbounded_channel();
            let (shutdown_socks_tx, shutdown_socks_rx) = mpsc::channel(1);

            {
                let mut state = state.lock().await;
                assert!(state.socks_conns.contains_key(&conn_id) == false);
                state.socks_conns.insert(conn_id, SocksConn::new(socks_conn_tx, shutdown_socks_tx));
            }

            tokio::spawn(async move {
                if let Err(err) = process_socks_message(state.clone(), frame, conn_id, socks_conn_rx, shutdown_socks_rx).await {
                    error!(cause = ?err, "process_socks_message error");
                }
            });
        }
    } else {
        let state = state.lock().await;
        assert!(state.socks_conns.contains_key(&conn_id) == true);
        if let Some(socks_conn) = state.socks_conns.get(&conn_id) {
            let _ = socks_conn.socks_shutdown_tx.send(()).await;
        }
    }
}

pub async fn run() -> Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", LISTEN_PORT)).await?;
    info!("start listening.. port:{}", LISTEN_PORT);

    let state = Arc::new(Mutex::new(Shared::new()));

    loop {
        let socket = accept_backoff(&listener).await?;
        let _ = socket.set_nodelay(true)?;

        let server_connected: bool;
        {
            let state = state.lock().await;
            server_connected = state.server_conn_tx.is_some();
        }
        if server_connected == false {
            info!("on server connection set server_conn");

            let state_clone = state.clone();
            tokio::spawn(async move {
                if let Err(err) = process_server_connection(socket, state_clone).await {
                    error!(cause = ?err, "process_server_connection error");
                }
            });
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("pid = {}", process::id());

    let _ = run().await?;

    Ok(())
}
