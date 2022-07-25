use tracing_subscriber;
use tracing::{info, error};
use muduo_rust::{Result, CountdownLatch};
use curl::easy::Easy;
use std::fs::OpenOptions;
use std::io::Write;
use std::env;

const CONCURRENT: i64 = 4;

fn get_header(url: &str) -> String {
    let mut res = String::new();
    let mut easy = Easy::new();
    easy.url(url).unwrap();
    easy.nobody(true).unwrap();
    let mut transfer = easy.transfer();
    transfer.header_function(|data| {
        res.push_str(std::str::from_utf8(&data).unwrap());
        true 
    }).unwrap();
    transfer.perform().unwrap();
    drop(transfer);

    let code = easy.response_code().unwrap();
    let effective_url = easy.effective_url().unwrap().unwrap().to_string();
    let mut redirect_url = String::new();
    if easy.redirect_url().is_ok() {
        if let Some(name) = easy.redirect_url().unwrap() {
            redirect_url = name.to_string();
        }
    }
    info!("done effective_url:{} redirect_url:{} code:{} res:{}", effective_url, redirect_url, code, res); 
    
    res
}

fn single_download(url: &str) {
    let mut file = OpenOptions::new().write(true)
        .create(true)
        .open("/tmp/output").unwrap();
    let mut easy = Easy::new();
    easy.url(url).unwrap();
    let mut transfer = easy.transfer();
    transfer.write_function(|data| {
        let _ = file.write_all(&data);
        let _ = file.flush();
        Ok(data.len())
    }).unwrap();
    transfer.perform().unwrap();
    drop(transfer);
    
    let code = easy.response_code().unwrap();
    let effective_url = easy.effective_url().unwrap().unwrap().to_string();
    let mut redirect_url = String::new();
    if easy.redirect_url().is_ok() {
        if let Some(name) = easy.redirect_url().unwrap() {
            redirect_url = name.to_string();
        }
    }
    info!("single_download done effective_url:{} redirect_url:{} code:{}", effective_url, redirect_url, code); 
}

async fn concurrent_download(url: &str, length: i64) -> Result<()> {
    let piece_len = length / CONCURRENT;
    let latch = CountdownLatch::new(CONCURRENT as i32);
    for i in 0..CONCURRENT {
        let url_string = url.to_string();
        let latch = latch.clone();
        info!("concurrent_download i:{} CONCURRENT:{}", i, CONCURRENT); 
        tokio::spawn(async move {
            let mut file = OpenOptions::new().write(true)
                .create(true)
                .open(format!("/tmp/output-{:05}-of-{:05}", i, CONCURRENT)).unwrap();
        
            let range_str: String;
            if i < CONCURRENT-1 {
                range_str = format!("{}-{}", i*piece_len, (i+1)*piece_len-1);
            } else {
                range_str = format!("{}-{}", i*piece_len, length-1);
            }
            info!("range:{}", range_str);

            let mut easy = Easy::new();
            easy.url(&url_string).unwrap();
            let _ = easy.range(&range_str);
            easy.write_function(move |data| {
                let _ = file.write_all(&data);
                let _ = file.flush();
                Ok(data.len())
            }).unwrap();
            
            easy.perform().unwrap();
            let code = easy.response_code().unwrap();
            let effective_url = easy.effective_url().unwrap().unwrap().to_string();
            let mut redirect_url = String::new();
            if easy.redirect_url().is_ok() {
                if let Some(name) = easy.redirect_url().unwrap() {
                    redirect_url = name.to_string();
                }
            }

            latch.countdown();
            info!("concurrent_download range:[{}] is done effective_url:{} redirect_url:{} code:{}", range_str, effective_url, redirect_url, code); 
        });
    }
    latch.wait();

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();

    let url = env::args()
        .nth(1)
        .unwrap_or_else(|| "https://chenshuo-public.s3.amazonaws.com/pdf/allinone.pdf".to_string());

    let mut found = false;
    let mut accept_ranges = false;
    let mut length: i64 = 0;
    let header = get_header(&url);
    if !header.is_empty() {
        if header.starts_with("HTTP/1.1 200") || header.starts_with("HTTP/1.0 200") {
            found = true; 
        }

        if header.contains("Accept-Ranges: bytes\r\n") {
            accept_ranges = true;
            info!("Accept-Ranges");
        }

        if let Some(length_index) = header.find("Content-Length: ") {
            if let Some(length_end) = header[length_index..].find("\r\n") {
                length = header[length_index+16..length_index+length_end].parse::<i64>().unwrap();
                info!("Content-Length: {}", length);
            }
        }

        if accept_ranges && length >= CONCURRENT*4096 {
            info!("Downloading with {} connections", CONCURRENT);
            concurrent_download(&url, length).await?;
        } else if found {
            info!("Single connection download");
            single_download(&url);
        } else {
            error!("File not found");
        }
    }

    Ok(())
}
