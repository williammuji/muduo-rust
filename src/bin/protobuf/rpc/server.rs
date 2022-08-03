use tonic::{transport::Server, Request, Response, Status};

use sudokum::sudoku_service_server::{SudokuService, SudokuServiceServer};
use sudokum::{SudokuRequest, SudokuResponse};

use tracing_subscriber;
use tracing::info;

pub mod sudokum {
    tonic::include_proto!("sudoku");
}

#[derive(Default)]
pub struct TheSudokuService {
}

#[tonic::async_trait]
impl SudokuService for TheSudokuService {
    async fn solve(
        &self,
        _request: Request<SudokuRequest>,
        ) -> std::result::Result<Response<SudokuResponse>, Status> {
 
        info!("SudokuService::Solve");
        let response = sudokum::SudokuResponse{
            solved: Some(true),
            checkerboard: Some("1234567".into()),
        };

        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    
    let addr = "[::1]:9981".parse().unwrap();
    let sudoku_service = TheSudokuService::default(); 

    info!("SudokuService listening on {}", addr);

    Server::builder()
        .add_service(SudokuServiceServer::new(sudoku_service))
        .serve(addr)
        .await?;

    Ok(())
}
