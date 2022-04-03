use tonic::{transport::Server, Request, Response, Status};

use nqueens::n_queens_service_server::{NQueensService, NQueensServiceServer};
use nqueens::{SubProblemResponse, SubProblemRequest};

use tracing_subscriber;
use tracing::info;
use std::time::Instant;

const MAXQUEENS: usize = 20;

pub mod nqueens {
    tonic::include_proto!("nqueens");
}

#[derive(Default)]
pub struct TheNQueensService {}

#[tonic::async_trait]
impl NQueensService for TheNQueensService {
    async fn solve(
        &self,
        request: Request<SubProblemRequest>,
        ) -> Result<Response<SubProblemResponse>, Status> {
        info!("NQueensService.solve from {:?} request:{:?}", request.remote_addr(), request.get_ref());

        let now = Instant::now();
        let count = backtracking_sub(request.get_ref().nqueens as u32, request.get_ref().first_row, request.get_ref().second_row);

        let response = nqueens::SubProblemResponse {
            count,
            seconds: now.elapsed().as_secs_f64(),
        };

        Ok(Response::new(response))
    }
}

struct BackTracking {
    n: u32,
    count: i64,
    columns: [u32; MAXQUEENS],
    diagnoal: [u32; MAXQUEENS],
    antidiagnoal: [u32; MAXQUEENS],
}

impl BackTracking {
    fn new(nqueens: u32) -> BackTracking {
        assert!(nqueens > 0 && nqueens <= MAXQUEENS as u32);

        BackTracking {
            n: nqueens,
            count: 0,
            columns: [0; MAXQUEENS],
            diagnoal: [0; MAXQUEENS],
            antidiagnoal: [0; MAXQUEENS],
        }
    }

    fn search(&mut self, row: usize) {
        let mut avail: u32 = self.columns[row] | self.diagnoal[row] | self.antidiagnoal[row];
        avail = !avail;

        while avail > 0 {
            let i = avail.trailing_zeros(); // counting trailing zeros
            if i >= self.n {
                break;
            }

            if row as u32 == self.n - 1 {
                self.count += 1;
            } else {
                let mask: u32 = 1 << i;
                self.columns[row+1] = self.columns[row] | mask;
                self.diagnoal[row+1] = (self.diagnoal[row] | mask) >> 1;
                self.antidiagnoal[row+1] = (self.antidiagnoal[row] | mask) << 1;
                self.search(row+1);
            }

            avail &= avail-1;  // turn off last bit
        }
    }
}

fn backtracking_sub(n: u32, first_row: i32, second_row: i32) -> i64 {
    let m0 = 1 << first_row;

    let mut bt = BackTracking::new(n);
    bt.columns[1] = m0;
    bt.diagnoal[1] = m0 >> 1;
    bt.antidiagnoal[1] = m0 << 1;

    if second_row >= 0 {
        let row: usize = 1;
        let m1: u32 = 1 << second_row;
        let mut avail: u32 = bt.columns[row] | bt.diagnoal[row] | bt.antidiagnoal[row];
        avail = !avail;
        if (avail & m1) > 0 {
            bt.columns[row+1] = bt.columns[row] | m1;
            bt.diagnoal[row+1] = (bt.diagnoal[row] | m1) >> 1;
            bt.antidiagnoal[row+1] = (bt.antidiagnoal[row] | m1) << 1;
            bt.search(row+1);
            return bt.count;
        }
    } else {
        bt.search(1);
        return bt.count;
    }

	0
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    
    let addr = "127.0.0.1:9352".parse().unwrap();
    let n_queens_service = TheNQueensService::default();

    info!("NQueensService listening on {}", addr);

    Server::builder()
        .add_service(NQueensServiceServer::new(n_queens_service))
        .serve(addr)
        .await?;

    Ok(())
}
