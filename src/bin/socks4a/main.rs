use muduo_rust::Result;
use muduo_rust::accept_backoff;

use std::process;
use tracing_subscriber;
use clap::Parser;
use tracing::{error, info};
use tokio::sync::{broadcast, mpsc};
use tokio_util::codec::{Framed, BytesCodec};
use tokio_stream::StreamExt;
use futures::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use bytes::{Buf, BytesMut, BufMut};
use std::net::{SocketAddrV4, Ipv4Addr, IpAddr};
use tokio::io::{self, AsyncWriteExt};
use resolve::resolve_host;

struct Session {
    name: String,
    bytes: Option<Framed<TcpStream, BytesCodec>>,
    notify_shutdown: broadcast::Receiver<()>,
    _shutdown_complete_tx: mpsc::Sender<()>,
}

impl Session {
    fn new(name: String, bytes: Framed<TcpStream, BytesCodec>, notify_shutdown: broadcast::Receiver<()>, shutdown_complete_tx: mpsc::Sender<()>) -> Session {
        Session {
            name,
            bytes: Some(bytes),
            notify_shutdown,
            _shutdown_complete_tx: shutdown_complete_tx,
        }
    }

    async fn run(&mut self) -> Result<()> {
        let mut server_bytes = self.bytes.take().unwrap();
        info!("{} serverside: {} -> {} is UP", self.name, server_bytes.get_ref().peer_addr().unwrap(), server_bytes.get_ref().local_addr().unwrap());

        let mut message = BytesMut::with_capacity(128);
        let mut message_end: usize;
        let mut server_addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3000);
        let mut okay = false;
        let ver: u8;
        let cmd: u8;
        let port: u16;
        let ip: [u8; 4];

        loop {
            tokio::select! {
                bytes = server_bytes.next() => match bytes {
                    Some(Ok(msg)) => {
                        let receive_len = msg.len();
                        message.put(msg);
                        info!("{} receive_len:{} message_len:{}", self.name, receive_len, message.len());

                        if message.len() > 128 {
                            return Ok(());
                        } else if message.len() > 8 {
                            if message.iter().nth(8) != Some(&b'\0') {
                                return Ok(());
                            } else {
                                message_end = 8;
                                let message_slice = message.as_ref();
                                ver = message_slice[0];
                                cmd = message_slice[1];
                               
                                let port_array: [u8; 2] = [message_slice[2], message_slice[3]];
                                port = u16::from_be_bytes(port_array);
                                ip = [message_slice[4], message_slice[5], message_slice[6], message_slice[7]];


                                // socks4a
                                if ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] != 0 {
                                    if let Some(hostname_end_pos) = message_slice[9..].iter().position(|&x| x == b'\0') {
                                        message_end = hostname_end_pos;
                                        let hostname = &message_slice[9..9+hostname_end_pos];
                                        let res = resolve_host(&String::from_utf8_lossy(&hostname));
                                        if res.is_err() {
                                            break;  // loop 
                                        }

                                        for addr in res.unwrap() {
                                            if let IpAddr::V4(ip4) = addr {
                                                server_addr = SocketAddrV4::new(ip4, port);
                                                okay = true;
                                                break;  // for
                                            }
                                        }
                                        break;  // loop
                                    } else {
                                        return Ok(());
                                    }
                                } else {
                                    server_addr = SocketAddrV4::new(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]), port);
                                    okay = true;
                                    break;   // loop
                                }
                            }   // else
                        }   // if message.len() > 8
                    }
                    Some(Err(e)) => {
                        info!("{} an error occurred while session processing messages error = {:?}", self.name, e);
                    }
                    None => {
                        return Ok(());
                    }
                },
                _ = self.notify_shutdown.recv() => {
                    return Ok(());
                }
            }   // select
        }   // loop

        info!("server_addr:{:?}", server_addr);

        let _ = message.split_to(message_end+1);
        let mut msg = BytesMut::new();
        msg.put_u8(0);
        msg.put_u8(90);
        if ver == 4 && cmd == 1 && okay {
            let mut client_stream = TcpStream::connect(server_addr).await?;
            let _ = client_stream.set_nodelay(true)?;
            info!("{} clientside: {} -> {} is UP", self.name, client_stream.local_addr().unwrap(), client_stream.peer_addr().unwrap());

            msg.put_u16(port.to_be());
            msg.put_slice(&ip);
            server_bytes.send(msg.freeze()).await?;

            client_stream.write_all(message.chunk()).await?;

            self.relay(server_bytes.into_inner(), client_stream).await?;
        } else {
            msg.put_slice(b"UVWXYZ");
            server_bytes.send(msg.freeze()).await?;
            return Ok(())
        }

        Ok(())
    }

    async fn relay(&mut self, server_stream: TcpStream, client_stream: TcpStream) -> Result<()> {
        let (done_tx, mut done_rx) = mpsc::channel::<i32>(1);

        let client_peer_addr = client_stream.peer_addr().unwrap();
        let client_local_addr = client_stream.local_addr().unwrap();
        let server_peer_addr = server_stream.peer_addr().unwrap();
        let server_local_addr = server_stream.local_addr().unwrap();

        let (mut server_rd, mut server_wr) = server_stream.into_split();
        let (mut client_rd, mut client_wr) = client_stream.into_split();

        let name1 = self.name.clone();
        let done_tx1 = done_tx.clone();
        tokio::spawn(async move {
            if io::copy(&mut server_rd, &mut client_wr).await.is_err() {
                info!("{} relay: failed to copy from client to server", name1);
            } else {
                info!("{} relay: done copying serverconn to clientconn", name1);
            }
            drop(client_wr);
            drop(done_tx1);
            info!("{} clientside: {} -> {} is DOWN", name1, client_local_addr, client_peer_addr);
        });

        let name2 = self.name.clone();
        let done_tx2 = done_tx.clone();
        tokio::spawn(async move {
            if io::copy(&mut client_rd, &mut server_wr).await.is_err() {
                info!("{} relay: failed to copy from server to client", name2);
            } else {
                info!("{} relay: done copying clientconn to serverconn", name2);
            }
            drop(server_wr);
            drop(done_tx2);
            info!("{} serverside: {} -> {} is DOWN", name2, server_peer_addr, server_local_addr);
        });

        drop(done_tx);

        loop {
            tokio::select! {
                _ = self.notify_shutdown.recv() => break,
                _ = done_rx.recv() => break,
            }
        }

        Ok(())
    }
}

struct Socks4aServer {
    listener: TcpListener,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
}

impl Socks4aServer {
    async fn run(&mut self) -> Result<()> {
        info!("accepting inbound connections");

        let mut id: i64 = 0;
        loop {
            let socket = accept_backoff(&self.listener).await?;
            let _ = socket.set_nodelay(true)?;

            id += 1;
            let mut session = Session::new(
                format!("tunnel-{}", id),
                Framed::new(socket, BytesCodec::new()),
                self.notify_shutdown.subscribe(),
                self.shutdown_complete_tx.clone());

            tokio::spawn(async move {
                if let Err(err) = session.run().await {
                    error!(cause = ?err, "{} session.run error", session.name);
                }
            });
        }
    }
}

async fn run(options: Options) -> Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", options.port)).await?;
    info!("start listening. port:{} pid:{}", options.port, process::id());

    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    let mut server = Socks4aServer {
        listener,
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = %err, "server.run error");
            }
        }
    }

    let Socks4aServer {
        notify_shutdown,
        shutdown_complete_tx,
        mut shutdown_complete_rx,
        ..
    } = server;

    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    /// TCP port
    #[clap(short, long)]
    #[clap(default_value_t = 2000)]
    port: u16,
}


#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let _ = run(Options::parse()).await?;

    Ok(())
}
