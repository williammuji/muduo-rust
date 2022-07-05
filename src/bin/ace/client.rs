use logrecord::log_record_service_client::LogRecordServiceClient;
use logrecord::LogRecord;

use tracing_subscriber;
use tracing::info;
use std::io::{self, BufRead};
use sysinfo::{PidExt, ProcessExt, UserExt, System, SystemExt, get_current_pid};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::logrecord::log_record::Heartbeat;
use muduo_rust::Result;

mod logrecord {
    tonic::include_proto!("logrecord");
}

async fn send_heartbeat(logger: &mut LogRecordServiceClient<tonic::transport::Channel>) -> Result<()> {
    let system = System::new_all();
    let current_pid = get_current_pid().unwrap();
    let process = system.process(current_pid).unwrap();
    let mut user_name = String::new();
    for user in system.users() { if *(user.uid()) == process.uid {
            user_name = user.name().to_string();
        }
    }
    let request = tonic::Request::new(LogRecord{
        heartbeat: Some(Heartbeat{
            hostname: system.host_name().unwrap(),
            process_name: process.name().to_string(),
            process_id: current_pid.as_u32() as i32,
            process_start_time: process.start_time() as i64,
            username: user_name, 
        }),
        level: 1,
        thread_id: 0,
        timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        message: "Heartbeat".to_string(),
    });

    let _ = logger.send_log(request).await?;

    info!("Type message below:");

    Ok(())
}

async fn send_log(logger: &mut LogRecordServiceClient<tonic::transport::Channel>, line: String) -> Result<()> {
    let request = tonic::Request::new(LogRecord{
        heartbeat: None,
        level: 1,
        thread_id: 0,
        timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        message: line,
    });

    let _ = logger.send_log(request).await?;

    info!("Type message below:");

    Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let mut logger = LogRecordServiceClient::connect("http://127.0.0.1:50000").await?;

    let _ = send_heartbeat(&mut logger).await;

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let _ = send_log(&mut logger, line.unwrap()).await; 
    }

    Ok(())
}
