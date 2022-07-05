use tonic::{transport::Server, Request, Response, Status};

use logrecord::log_record_service_server::{LogRecordService, LogRecordServiceServer};
use logrecord::{LogRecord, Empty};

use tracing_subscriber;
use tracing::info;
use std::sync::{Arc, Mutex};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicI32, Ordering};
use std::io::Write;


pub mod logrecord {
    tonic::include_proto!("logrecord");
}

#[derive(Default)]
pub struct TheLogRecordService {
    count: AtomicI32,
    file_names: Arc<Mutex<HashMap<String, String>>>,
}

impl TheLogRecordService {
    pub fn new() -> Self {
        TheLogRecordService {
            count: AtomicI32::new(0),
            file_names: Arc::new(Mutex::new(HashMap::new())), 
        }
    }
}

#[tonic::async_trait]
impl LogRecordService for TheLogRecordService {
    async fn send_log(
        &self,
        request: Request<LogRecord>,
        ) -> std::result::Result<Response<Empty>, Status> {
       
        let remote_addr_string = format!("{:?}", request.remote_addr().unwrap());
        let log_string = format!("{:?}", request.get_ref());

        let the_file_name: String;
        {
            let mut file_names = self.file_names.lock().unwrap();
            let file_name = file_names.get(&remote_addr_string);
            if file_name.is_none() {
                let mut name = String::new();
                name.push_str(&remote_addr_string);

                let now: DateTime<Utc> = Utc::now();
                name.push_str(&format!("{}", now.format(".%Y%m%d-%H%M%S.")));

                let count = self.count.fetch_add(1, Ordering::SeqCst);
                name.push_str(&format!("{}.log", count+1));

                (*file_names).insert(remote_addr_string.clone(), name.clone());
                info!("Session of {} file {}", remote_addr_string, name);
                the_file_name = name;
            } else {
                the_file_name = file_name.unwrap().to_string();
            }
        }

        let mut file = OpenOptions::new().write(true)
            .create(true)
            .open(format!("/tmp/{}", the_file_name)).unwrap();
        let _ = file.write_all(format!("{:?}==========\n", log_string).as_bytes());
        file.flush()?;

        info!("LogRecordService.write_file from {} request:{:?}", remote_addr_string, request.get_ref());

        let response = logrecord::Empty{};

        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    
    let addr = "127.0.0.1:50000".parse().unwrap();
    let log_record_service = TheLogRecordService::new(); 

    info!("LogRecordService listening on {}", addr);

    Server::builder()
        .add_service(LogRecordServiceServer::new(log_record_service))
        .serve(addr)
        .await?;

    Ok(())
}
