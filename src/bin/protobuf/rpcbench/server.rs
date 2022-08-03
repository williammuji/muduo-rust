use tonic::{transport::Server, Request, Response, Status};

use echom::echo_service_server::{EchoService, EchoServiceServer};
use echom::{EchoRequest, EchoResponse};

use tracing_subscriber;
use tracing::info;

pub mod echom {
    tonic::include_proto!("echo");
}

#[derive(Default)]
pub struct TheEchoService {
}

#[tonic::async_trait]
impl EchoService for TheEchoService {
    async fn echo(
        &self,
        request: Request<EchoRequest>,
        ) -> std::result::Result<Response<EchoResponse>, Status> {
        
        let response = echom::EchoResponse{
            payload: request.get_ref().payload.clone(),
        };
 
        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    
    let addr = "[::1]:8888".parse().unwrap();
    let echo_service = TheEchoService::default(); 

    info!("EchoService listening on {}", addr);

    Server::builder()
        .add_service(EchoServiceServer::new(echo_service))
        .serve(addr)
        .await?;

    Ok(())
}
