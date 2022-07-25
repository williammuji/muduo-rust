use tracing_subscriber;
use tracing::info;
use muduo_rust::{CountdownLatch, Result};
use curl::easy::Easy;

fn connect(url: &str, connected: CountdownLatch) {
    let mut easy = Easy::new();
    easy.url(url).unwrap();
    easy.write_function(|data| {
        info!("len:{} {}", data.len(), std::str::from_utf8(&data).unwrap());
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
    info!("done effective_url:{} redirect_url:{} code:{}", effective_url, redirect_url, code); 
    connected.countdown();
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();

    let connected = CountdownLatch::new(3);
    let connected_clone = connected.clone();
    let _ = tokio::spawn(async move {
        connect("http://chenshuo.com", connected_clone);
    });
    let connected_clone = connected.clone();
    let _ = tokio::spawn(async move {
        connect("http://github.com", connected_clone);
    });
    let connected_clone = connected.clone();
    let _ = tokio::spawn(async move {
        connect("http://example.com", connected_clone);
    });

    connected.wait();
    Ok(())
}
