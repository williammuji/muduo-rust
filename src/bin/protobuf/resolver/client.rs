use resolverm::resolver_service_client::ResolverServiceClient;
use resolverm::ResolveRequest;
use muduo_rust::{CountdownLatch, Result};
use std::net::Ipv4Addr;
use tracing_subscriber;
use tracing::{info, error};

pub mod resolverm {
    tonic::include_proto!("resolver");
}

async fn resolve_hostname(client: &mut ResolverServiceClient<tonic::transport::Channel>, address: String, counter: CountdownLatch) {
    let address_clone = address.clone(); 
    let request = tonic::Request::new(ResolveRequest {
        address,
    });

    match client.resolve(request).await {
        Ok(response) => {
            let resp = response.into_inner();
            if let Some(resolved) = resp.resolved {
                if resolved {
                    for ip in resp.ip.iter() {
                        let ip_str = Ipv4Addr::from(*ip);
                        info!("resolved {} : {} \n {:?}", address_clone, ip_str, resp); 
                    }
                } else {
                    info!("resolved {} false", address_clone); 
                }
            } else {
                info!("resolved {} failed", address_clone); 
            }

            counter.countdown();
        }
        Err(e) => {
            error!("resolved {} error:{}", address_clone, e);
        } 
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init(); 
    let mut client = ResolverServiceClient::connect("http://[::1]:2053").await?;
    let counter = CountdownLatch::new(4);

    resolve_hostname(&mut client, "www.example.com:80".into(), counter.clone()).await;
    resolve_hostname(&mut client, "www.chenshuo.com:80".into(), counter.clone()).await;
    resolve_hostname(&mut client, "www.google.com:80".into(), counter.clone()).await;
    resolve_hostname(&mut client, "acme.chenshuo.org:80".into(), counter.clone()).await;
    
    counter.wait();
    
    Ok(())
}
