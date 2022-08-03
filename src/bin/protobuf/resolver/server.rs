use tonic::{transport::Server, Request, Response, Status};

use resolverm::resolver_service_server::{ResolverService, ResolverServiceServer};
use resolverm::{ResolveRequest, ResolveResponse};

use tracing_subscriber;
use tracing::info;
use std::net::ToSocketAddrs;
use std::net::IpAddr;

pub mod resolverm {
    tonic::include_proto!("resolver");
}

#[derive(Default)]
pub struct TheResolverService {
}

#[tonic::async_trait]
impl ResolverService for TheResolverService {
    async fn resolve(
        &self,
        request: Request<ResolveRequest>,
        ) -> std::result::Result<Response<ResolveResponse>, Status> {
        
        let mut response = resolverm::ResolveResponse{
            resolved: None,
            ip: Vec::new(),
            port: Vec::new(),
        };
 
        info!("ResolverService::Resolve {}", request.get_ref().address);
        let addrs_iter = request.get_ref().address.to_socket_addrs();
        if let Err(e) = addrs_iter {
            response.resolved = Some(false);
            info!("ResolverService::Resolve {} error:{}", request.get_ref().address, e);
        } else {
            response.resolved = Some(true);
            let addrs_iter = addrs_iter.unwrap(); 
            for addr in addrs_iter {
                if let IpAddr::V4(ip4) = addr.ip() {
                    response.ip.push(ip4.into()); 
                    response.port.push(addr.port() as i32); 
                }
            }
        }

        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    
    let addr = "[::1]:2053".parse().unwrap();
    let resolver_service = TheResolverService::default(); 

    info!("ResolverService listening on {}", addr);

    Server::builder()
        .add_service(ResolverServiceServer::new(resolver_service))
        .serve(addr)
        .await?;

    Ok(())
}
