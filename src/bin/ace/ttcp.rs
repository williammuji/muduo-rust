use tracing_subscriber;
use tracing::{info, error};
use clap::Parser;
use std::time::Instant;
use tokio::sync::mpsc;
use muduo_rust::Error;
use std::mem;
use tokio_util::codec::{Framed, BytesCodec};
use tokio::sync::broadcast;
use tokio::net::{TcpListener, TcpStream};
use muduo_rust::{Result, accept_backoff};
use futures::{SinkExt, StreamExt};
use bytes::{Bytes, Buf, BytesMut, BufMut};
use std::convert::TryInto;

static NUMBERS: &str = "0123456789ABCDEF";

struct Listener {
    listener: TcpListener,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
}

impl Listener {
    async fn run(&mut self, nodelay: i32) -> Result<()> {
        let mut id: i64 = 0;
        loop {
			let socket = accept_backoff(&self.listener).await?;
			if nodelay == 1{
				let _ = socket.set_nodelay(true);
			}

            id += 1;
            let mut session = Session {
                name: format!("receive-{}", id),
                buffers: Framed::new(socket, BytesCodec::new()),
                listener_shutdown: self.notify_shutdown.subscribe(),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
                receive_buffer: BytesMut::new(),
            };
            info!("{} {} -> {} is UP", session.name, session.buffers.get_ref().peer_addr()?, session.buffers.get_ref().local_addr()?);

            tokio::spawn(async move {
                if let Err(err) = session.run().await {
                    error!(cause = ?err, "{} error", session.name);
                }
            });
        }
    }
}

struct Session {
    name: String,
    buffers: Framed<TcpStream, BytesCodec>,
    listener_shutdown: broadcast::Receiver<()>,
    _shutdown_complete: mpsc::Sender<()>,
    receive_buffer: BytesMut,
}

impl Session {
    async fn run(self: &mut Session) -> Result<()> {
        let mut number: i32 = 0;
        let mut length: i32 = 0;
        let mut total_len: usize;
        let mut receive_number: i32 = 0;
        let header_len = mem::size_of::<i32>();
        let mut ack = BytesMut::with_capacity(header_len);
        loop {
            tokio::select! {
                bytes = self.buffers.next() => match bytes {
                    Some(Ok(mut msg)) => {
                        info!("{} receive msg.len:{}", self.name, msg.len());
                        while msg.len() >= header_len {
                            if number == 0 && length == 0 {
                                if msg.len() >= 2*header_len {
                                    number = i32::from_be_bytes(msg.chunk()[0..header_len].try_into().unwrap());
                                    msg.advance(header_len);
                                    length = i32::from_be_bytes(msg.chunk()[0..header_len].try_into().unwrap());
                                    msg.advance(header_len);
                                    ack.put_i32(length);
                                    info!("{} receive number:{} length:{}", self.name, number, length);
                                } else {
                                    break;
                                }
                            } else {
                                self.receive_buffer.extend_from_slice(msg.chunk());
                                let payload_len = i32::from_be_bytes(self.receive_buffer.chunk()[0..header_len].try_into().unwrap());
                                if payload_len == length {
                                    total_len = length as usize + header_len;
                                    if self.receive_buffer.len() >= total_len {
                                        self.receive_buffer.advance(total_len);
                                        let ack_data = Bytes::copy_from_slice(ack.chunk());
                                        let _ = self.buffers.send(ack_data).await?;
                                        receive_number += 1;
                                        info!("{} receive payload_len:{} receive_number:{} total_len:{} receive_buffer.len:{}", self.name, payload_len, receive_number, total_len, self.receive_buffer.len());
                                        if receive_number >= number {
                                            return Ok(());
                                        }
                                    } else {
                                        break;
                                    }
                                } else {
                                    error!("{} wrong payload_len:{}", self.name, payload_len);
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        error!(
                            "an error occurred while processing messages error = {:?}",
                            e
                            );
                    }
                    None => break,
                },
                _ = self.listener_shutdown.recv() => {
                    info!("{} receive listener shutdown", self.name);
                    return Ok(());
                },
            }
        }
        Ok(())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if self.buffers.get_ref().peer_addr().is_ok() && self.buffers.get_ref().local_addr().is_ok() {
            info!("{} {} -> {} is DOWN", self.name, self.buffers.get_ref().peer_addr().unwrap(), self.buffers.get_ref().local_addr().unwrap());
        }
    }
}

async fn transmit(opt: &Options) -> Result<()> {
    info!("connecting to {}:{}", opt.host, opt.port); 

    let socket = TcpStream::connect(&format!("{}:{}", opt.host, opt.port)).await?;
	if opt.nodelay == 1 {
		let _ = socket.set_nodelay(true)?;
	}
    
    let mut buffers = Framed::new(socket, BytesCodec::new());
    info!("{} -> {} is UP", buffers.get_ref().local_addr()?, buffers.get_ref().peer_addr()?);
    
    let total_mb: f32 = 1.0 * opt.length as f32 * opt.number as f32 / 1024.0 / 1024.0;
    info!("{:.3} MiB in total", total_mb);

    let start = Instant::now();
    let mut header = BytesMut::with_capacity(2*mem::size_of::<i32>());
    header.put_i32(opt.number);
    header.put_i32(opt.length);
    
    let _ = buffers.send(header.freeze()).await?;
    info!("transmit header number:{} length:{}", opt.number, opt.length);

    let total_len = mem::size_of::<i32>() + opt.length as usize;
    let mut payload = BytesMut::with_capacity(total_len as usize);
    payload.put_i32(opt.length);
    for i in 0..opt.length as usize {
        payload.put_u8(NUMBERS.as_bytes()[i%16]);
    }

    let mut receive_buffer = BytesMut::new();

    let header_len = mem::size_of::<i32>();
    let mut receive_number: i32 = 0;
    let mut send_number: i32 = 1;
    let data = Bytes::copy_from_slice(payload.chunk());
    let res = buffers.send(data).await;
    if res.is_err() {
        error!("transmit payload failed err:{:?}, receive_number:{}", res, receive_number);
        return Ok(());
    }
    info!("transmit payload send_number:{} payload.len:{}", send_number, payload.len());

    loop {
        tokio::select! {
            buffer = buffers.next() => match buffer {
                Some(Ok(msg)) => {
                    info!("transmit receive msg.len:{}", msg.len());
                    receive_buffer.extend_from_slice(msg.chunk());
                    while receive_buffer.len() >= header_len {
                        let payload_len = i32::from_be_bytes(receive_buffer.chunk()[0..header_len].try_into().unwrap());
                        receive_buffer.advance(header_len); 
                        if payload_len == opt.length {
                            receive_number += 1;
                            if receive_number < opt.number {
                                let data = Bytes::copy_from_slice(payload.chunk());
                                let _ = buffers.send(data).await;
                                send_number += 1;
                                info!("transmit payload receive_number:{} send_number:{} payload.len:{}", receive_number, send_number, payload.len());
                            } else {
                                let elapsed = start.elapsed().as_secs_f32();
                                info!("{:.3} seconds {:.3} MiB/s", elapsed, total_mb/elapsed);

                                return Ok(());
                            }
                        } else {
                            error!("transmit payload wrong length:{}", payload_len);
                            return Ok(());
                        }
                    }
                }
                Some(Err(e)) => {
                    error!(
                        "an error occurred while processing messages error = {:?}",
                        e
                        );
                }
                None => break,
            }
        }
    }

    Ok(())
}

async fn receive(opt: &Options) ->Result<()> {
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", opt.port)).await?;
    info!("start listening on port:{}", opt.port);

    let shutdown = tokio::signal::ctrl_c();
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    let mut server = Listener {
        listener,
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
    };

    tokio::select! {
        res = server.run(opt.nodelay) => {
            if let Err(err) = res {
                error!(cause = %err, "server.run failed.");
            }
        }
        _ = shutdown => {
            info!("shutting down.");
        }
    }

    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
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
    /// listen port
    #[clap(short, long)]
    #[clap(default_value_t = 8848)]
    port: u16,
    
    /// length 
    #[clap(short, long)]
    #[clap(default_value_t = 1024)]
    length: i32,

    /// number 
    #[clap(short, long)]
    #[clap(default_value_t = 100)]
    number: i32,

    /// transmit 
    #[clap(short, long)]
    #[clap(default_value_t = 0)]
    transmit: i32,

    /// receive 
    #[clap(short, long)]
    #[clap(default_value_t = 1)]
    receive: i32,

    /// nodelay 
    #[clap(long)]
    #[clap(default_value_t = 1)]
    nodelay: i32,
    
    /// host 
    #[clap(short, long)]
    #[clap(default_value_t = String::from("127.0.0.1"))]
    host: String,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Error> {
    tracing_subscriber::fmt::init();

    let options = Options::parse();

	if options.transmit == 1 {
		let _ = transmit(&options).await?;
	} else if options.receive == 1{
		let _ = receive(&options).await?;
	}

    Ok(())
}
