use crate::item::{UpdatePolicy, Item};
use crate::server::Shared;
use muduo_rust::Result;

use tokio_util::codec::{Framed, LinesCodec};
use tokio_stream::StreamExt;
use tokio::sync::{mpsc, broadcast};
use tokio::net::TcpStream;
use std::sync::Arc;
use tracing::info;
use futures::SinkExt;
use std::io::ErrorKind;

pub const LONGEST_KEY_SIZE: usize = 250;
pub const NOREPLY: &str = " noreply";


#[derive(Debug, PartialEq)]
pub enum State {
    NewCommand,
    ReceiveValue,
    DiscardValue,
}

#[derive(Debug, PartialEq)]
pub enum Protocol {
    Ascii,
    Binary,
    Auto,
}

pub struct Session {
    pub name: String,
    pub lines: Framed<TcpStream, LinesCodec>,
    pub notify_shutdown: broadcast::Receiver<()>,
    pub server_shutdown_tx: mpsc::Sender<()>,
    pub _shutdown_complete_tx: mpsc::Sender<()>,
    pub shared: Arc<Shared>,
    pub state: State,
    pub protocol: Protocol,

    // current request
    pub command: String,
    pub noreply: bool,
    pub policy: UpdatePolicy,
    pub curr_item: Option<Arc<Item>>,
    pub bytes_to_discard: usize,

    // cached
    pub needle: Arc<Item>,
    pub output_buf: String,

    // per session stats
    pub bytes_read: usize,
    pub requests_processed: i64,
}

impl Session {
    pub async fn run(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                line = self.lines.next() => match line {
                    // illegal length without crlf would shutdown
                    // eg. bytesMut buffer size over 1024
                    Some(Ok(mut request)) => {
                        if self.state == State::NewCommand {
                            if self.process_request(&mut request).await? {
                                self.reset_request();
                            }
                        } else if self.state == State::ReceiveValue {
                            self.receive_value(&request).await?;
                        } else if self.state == State::DiscardValue {
                            self.discard_value(&request);
                        } else {
                            assert!(false);
                        }
                        self.bytes_read += request.len();
                    }
                    Some(Err(e)) => {
                        tracing::info!(
                            "{} an error occurred while session processing messages error = {:?}",
                            self.name, e);
                    }
                    None => break,
                },
              _ = self.notify_shutdown.recv() => {
                return Ok(());
              }
            }
        }

        if self.lines.get_ref().local_addr().is_ok() && self.lines.get_ref().peer_addr().is_ok() {
            info!("{} {} -> {} is DOWN", self.name, self.lines.get_ref().local_addr().unwrap(), self.lines.get_ref().peer_addr().unwrap());
        }

        Ok(())
    }

    async fn receive_value(&mut self, request: &str) -> Result<()> {
        assert!(self.curr_item.is_some());
        assert!(self.state == State::ReceiveValue);

        if let Some(ref mut curr_item) = self.curr_item {
            // FIXME request.len() == curr_item.needed_bytes() on Ascii protocol
            // let avail = cmp::min(request.len(), curr_item.needed_bytes()-2);
             assert!(Arc::strong_count(&curr_item) == 1);

            Arc::get_mut(curr_item).unwrap().append(request.as_bytes());
            if Arc::get_mut(curr_item).unwrap().needed_bytes() == 2 {
                Arc::get_mut(curr_item).unwrap().append(b"\r\n");
            }

            // \r\n
            if curr_item.needed_bytes() == 0 {
                let mut exists = false;
                let success = self.shared.store_item(curr_item, self.policy, &mut exists);

                if success {
                    self.reply("STORED").await?;
                } else {
                    if self.policy == UpdatePolicy::Cas {
                        if exists {
                            self.reply("EXISTS").await?;
                        } else {
                            self.reply("NOT_FOUND").await?;
                        }
                    } else {
                        self.reply("NOT_STORED").await?;
                    }
                }

                self.reset_request();
                self.state = State::NewCommand;
            }
        }

        Ok(())
    }

    fn discard_value(&mut self, request: &str) {
        assert!(self.curr_item.is_none());
        assert!(self.state == State::DiscardValue);
        
        if request.len() < self.bytes_to_discard {
            self.bytes_to_discard -= request.len();
        } else {
            self.bytes_to_discard = 0;
            self.reset_request();
            self.state = State::NewCommand;
        }
    }

    async fn process_request(&mut self, request: &mut str) -> Result<bool> {
        assert!(self.command.is_empty());
        assert!(!self.noreply);
        assert!(self.policy == UpdatePolicy::Invalid);
        assert!(self.curr_item.is_none());
        assert!(self.bytes_to_discard == 0);

        self.requests_processed += 1;

        if request.len() >= 8 && request.ends_with(NOREPLY) {
            self.noreply = true;
            request.strip_suffix(NOREPLY).unwrap();
        }

        let req: Vec<&str> = request.split(' ').collect();
        if req.is_empty() {
            self.reply("ERROR").await?;
            return Ok(true);
        }

        self.command = req[0].to_string();
        if req[0] == "set" || req[0] == "add" || req[0] == "replace"
            || req[0] == "append" || req[0] == "prepend" || req[0] == "cas" {
               return Ok(self.do_update(&req).await?); 
            } else if req[0] == "get" || req[0] == "gets" {
                self.do_get(&req).await?;
            } else if req[0] == "delete" {
                self.do_delete(&req).await?;
            } else if req[0] == "version" {
                self.reply("VERSION 0.01 muduo-rust").await?;
            } else if req[0] == "quit" {
                let err = Box::new(std::io::Error::new(ErrorKind::ConnectionReset, "connection reset by quit"));
                return Err(err.into());
            } else if req[0] == "shutdown" {
                self.server_shutdown_tx.send(()).await?;
                let err = Box::new(std::io::Error::new(ErrorKind::ConnectionReset, "connection reset by shutdown"));
                return Err(err.into());
            } else {
                self.reply("ERROR").await?;
                info!("Unknown command: {}", self.command);
            }

        Ok(true) 
    }

    fn reset_request(&mut self) {
        self.command.clear();
        self.noreply = false;
        self.policy = UpdatePolicy::Invalid;
        self.curr_item = None;
        self.bytes_to_discard = 0;
    }

    async fn reply(&mut self, msg: &str) -> Result<()> {
        if !self.noreply {
            self.lines.send(&msg).await?; 
        }

        Ok(())
    }

    async fn do_update(&mut self, req: &Vec<&str>) -> Result<bool> {
        if self.command == "set" {
            self.policy = UpdatePolicy::Set;
        } else if self.command == "add" {
            self.policy = UpdatePolicy::Add;
        } else if self.command == "replace" {
            self.policy = UpdatePolicy::Replace;
        } else if self.command == "append" {
            self.policy = UpdatePolicy::Append;
        } else if self.command == "prepend" {
            self.policy = UpdatePolicy::Prepend;
        } else if self.command == "cas" {
            self.policy = UpdatePolicy::Cas;
        } else {
            assert!(false);
        }

        let mut good = true;
        let mut flags: u32 = 0;
        let mut exptime: u64 = 1;
        let mut bytes: usize = 0;
        let mut cas: u64 = 0;

        if req.len() >= 5 {
            let bytes_res = req[4].parse::<usize>();
            if bytes_res.is_err() {
                good = false;
            } else {
                bytes = bytes_res.unwrap();
            }
        } else if req.len() >= 4 {
            let exptime_res = req[3].parse::<u64>();
            if exptime_res.is_err() {
                good = false;
            } else {
                exptime = exptime_res.unwrap();
            }
        } else if req.len() >= 3 {
            let flags_res = req[2].parse::<u32>();
            if flags_res.is_err() {
                good = false;
            } else {
                flags = flags_res.unwrap();
            }
        } else if req.len() >= 2 {
            if req[1].len() > LONGEST_KEY_SIZE {
                good = false;
            }
        }

        let mut rel_exptime = exptime as i32;
        if exptime > 60*60*24*30 {
            rel_exptime = (exptime - self.shared.start_time()) as i32;
            if rel_exptime < 1 {
                rel_exptime = 1;
            }
        }

        if good && self.policy == UpdatePolicy::Cas {
            if req.len() >= 6 {
                let cas_res = req[5].parse::<u64>();
                if cas_res.is_err() {
                    good = false;
                } else {
                    cas = cas_res.unwrap();
                }
            }
        }

        if !good {
            self.reply("CLIENT_ERROR bad command line format").await?;
            return Ok(true);
        }

        if bytes > 1024*1024 {
            self.reply("SERVER_ERROR object too large for cache").await?;
            if req.len() >= 2 {
                Arc::get_mut(&mut self.needle).unwrap().reset_key(req[1]);
            }
            self.shared.delete_item(&self.needle);
            self.bytes_to_discard = bytes+2;
            self.state = State::DiscardValue;
            return Ok(false);
        } else {
            self.curr_item = Some(Item::new(req[1].as_bytes(), flags, rel_exptime, bytes+2, cas));
            self.state = State::ReceiveValue;
            return Ok(false);
        }
    }
    
    async fn do_get(&mut self, req: &Vec<&str>) -> Result<()> {
        let cas = self.command == "gets";

        if req.len() < 2 {
            self.reply("no key").await?;
            return Ok(());
        }

        for i in 1..req.len() {
            if req[i].len() > LONGEST_KEY_SIZE {
                self.reply("CLIENT_ERROR bad command line format").await?;
                return Ok(());
            }

            (*Arc::get_mut(&mut self.needle).unwrap()).reset_key(req[i]);
            if let Some(ref mut item) = self.shared.get_item(&self.needle) {
                item.output(&mut self.output_buf, cas);
            }
        }
        self.output_buf.push_str("END");
        self.lines.send(&self.output_buf).await?;
        self.output_buf.clear();

        Ok(())
    }

    async fn do_delete(&mut self, req: &Vec<&str>) -> Result<()> {
        assert!(self.command == "delete");

        let mut good = true;
        if req.len() < 2 {
            good = false;
        } else {
            if req[1].len() > LONGEST_KEY_SIZE {
                good = false;
            }
        }

        if !good {
            self.reply("CLIENT_ERROR bad command line format").await?;
        } else if req.len() >= 3 {
            if req[2] != "0" {
                self.reply("CLIENT_ERROR bad command line format.  Usage: delete <key> [noreply]").await?;
            }
        } else {
            (*Arc::get_mut(&mut self.needle).unwrap()).reset_key(req[1]);
            let mut success = false;
            if self.shared.delete_item(&self.needle) {
                success = true;
            }

            if success {
                self.reply("DELETED").await?;
            } else {
                self.reply("NOT_FOUND").await?;
            }
        }

        Ok(())
    }
}
