use std::future::Future;
use tokio::net::{TcpListener, TcpStream};
use tracing_subscriber;
use tokio::sync::{broadcast, mpsc};
use std::sync::{Arc, Mutex};
use tracing::{error, info};
use tokio::time::{self, Duration};
use std::env;
use std::io;
use tokio_util::codec::{Framed, LinesCodec};
use tokio_stream::StreamExt;
use futures::SinkExt;
use sudoku::Sudoku;
use std::str;
use std::time::SystemTime;
use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod stat;

const CELLS: usize = 81;

struct Listener {
    listener: TcpListener,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    stat: Arc<Mutex<stat::SudokuStat>>,
}

impl Listener {
    async fn run(&mut self) -> io::Result<()> {
        info!("accepting inbound connections");

        let mut id: i64 = 0;
        loop {
            let socket = self.accept().await?;
            socket.set_nodelay(true).expect("set_nodelay failed");

            id += 1;
            let mut session = Session {
                name: format!("connection-{}", id),
                buffers: Framed::new(socket, LinesCodec::new()),
                listener_shutdown: self.notify_shutdown.subscribe(),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
                stat: self.stat.clone(),
            };
            info!("{} {} -> {} is UP", session.name, session.buffers.get_ref().local_addr()?, session.buffers.get_ref().peer_addr()?);
           
            tokio::spawn(async move {
                if let Err(err) = session.run().await {
                    error!(cause = ?err, "{} error", session.name);
                }
            });
        }
    }

    async fn accept(&mut self) -> io::Result<TcpStream> {
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

struct Request {
    puzzle: String,
    receive: Duration,
}

struct Session {
    name: String,
    buffers: Framed<TcpStream, LinesCodec>,
    listener_shutdown: broadcast::Receiver<()>,
    _shutdown_complete: mpsc::Sender<()>,
    stat: Arc<Mutex<stat::SudokuStat>>,
}

impl Session {
    async fn run(self: &mut Session) -> io::Result<()> {
        let (compute_tx, mut compute_rx) = mpsc::unbounded_channel::<Request>();
        let (compute_resp_tx, mut compute_resp_rx) = mpsc::unbounded_channel::<String>();

        let stat_cpu_clone = self.stat.clone();
        // cpu bound 
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(msg) = compute_rx.recv() => {
                        let mut puzzle = msg.puzzle.as_str();

                        let mut resp = String::new(); 
                        if let Some(index) = msg.puzzle.find(":") {
                            puzzle = &msg.puzzle[index+1 ..]; 
                            resp.push_str(&msg.puzzle[..index+1]);
                        }

                        if let Some(solution) = Sudoku::from_str_line(puzzle).unwrap().solve_one() {
                            resp.push_str(&solution.to_str_line());
                            let _ = compute_resp_tx.send(resp);
                            let mut stat = stat_cpu_clone.lock().unwrap();
                            stat.record_response(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap(), msg.receive, true);
                        } else {
                            resp.push_str("NoSolution");
                            let _ = compute_resp_tx.send(resp);
                            let mut stat = stat_cpu_clone.lock().unwrap();
                            stat.record_response(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap(), msg.receive, false);
                        }
                    }
                    else => break,
                }
            }
        });

        let stat_io_clone = self.stat.clone();
        // io bound
        loop {
            tokio::select! {
                buffer = self.buffers.next() => match buffer {
                    Some(Ok(msg)) => {
                        { 
                            let mut stat = stat_io_clone.lock().unwrap();
                            stat.record_request();
                        }
                        let mut parse_success = true;
                        let puzzle = &msg;
                        if let Some(index) = msg.find(":") {
                            let puzzle = &msg[index+1..];
                            if puzzle.len() != CELLS {
                                parse_success = false;
                            }
                        } else {
                            if puzzle.len() != CELLS {
                                parse_success = false;
                            }
                        }

                        if !parse_success {
                            let _ = self.buffers.send("Bad request!").await.unwrap();
                            let mut stat = stat_io_clone.lock().unwrap();
                            stat.record_bad_request();
                        } else {
                            let _ = compute_tx.send(Request{
                                puzzle: msg,
                                receive: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap()});
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!(
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
                Some(response) = compute_resp_rx.recv() => {
                    let _ = self.buffers.send(response).await.unwrap();
                }
            }
        }
        Ok(())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        info!("{} {} -> {} is DOWN", self.name, self.buffers.get_ref().local_addr().unwrap(), self.buffers.get_ref().peer_addr().unwrap());
    }
}

async fn handle_connection(mut stream: TcpStream, stat: Arc<Mutex<stat::SudokuStat>>) -> Result<(), Box<dyn Error>> {
    let mut buffer = [0; 1024];
    stream.read(&mut buffer).await?;

    let stats_header = b"GET /sudoku/stats HTTP/1.1\r\n";
    let reset_header = b"GET /sudoku/reset HTTP/1.1\r\n";

    let (status_line, contents) = if buffer.starts_with(stats_header) {
        ("HTTP/1.1 200 OK", stat.lock().unwrap().report()) 
    } else if buffer.starts_with(reset_header) {
        ("HTTP/1.1 200 OK", stat.lock().unwrap().reset())
    } else {
        ("HTTP/1.1 404 NOT FOUND", "<!DOCTYPE html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\">
    <title>Hello!</title>
  </head>
  <body>
    <h1>Oops!</h1>
    <p>Sorry, I don't know what you're asking for.</p>
  </body>
</html>".to_string())
    };

    let response = format!("{}\r\nConetnt-Length: {}\r\n\r\n{}", status_line, contents.len(), contents);
    stream.write(response.as_bytes()).await?;
    stream.flush().await?;

    Ok(())
}

async fn inspect(inspector_addr: String, stat: Arc<Mutex<stat::SudokuStat>>) -> Result<(), Box<dyn Error>> {
    info!("start listening.. inspector_addr:{}", inspector_addr);
    let listener = TcpListener::bind(inspector_addr).await.unwrap();

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, stat.clone()).await?;
    }
}

pub async fn run(addr: &str, inspector_addr: String, shutdown: impl Future) {

    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    let stat = Arc::new(Mutex::new(stat::SudokuStat::new()));

    let stat_clone = stat.clone();
    tokio::spawn(async {
        if let Err(e) = inspect(inspector_addr, stat_clone).await {
            tracing::info!("an error occurred when inspect; error = {:?}", e);
        }
    });

    let listener = TcpListener::bind(addr).await.unwrap();
    info!("start listening.. addr:{}", addr);
   
    let mut server = Listener {
        listener,
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
        stat,
    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("shutting down");
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
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
   
    let addr = env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1:9981".to_string());

    let inspector_addr = env::args()
        .nth(3)
        .unwrap_or_else(|| "127.0.0.1:9982".to_string());

    run(&addr, inspector_addr, tokio::signal::ctrl_c()).await;
}
