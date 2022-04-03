use nqueens::n_queens_service_client::NQueensServiceClient;
use nqueens::SubProblemRequest;

use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing_subscriber;
use tracing::info;
use clap::Parser;

const THRESHOLD: i32 = 14;

pub mod nqueens {
    tonic::include_proto!("nqueens");
}

#[derive(Debug)]
struct Shard {
    first_row: i32,
    second_row: i32,
    count: i64,
}

async fn solve(solver: &mut NQueensServiceClient<tonic::transport::Channel>, n: i32, first_row: i32, second_row: i32) -> i64 {
        let request = tonic::Request::new(SubProblemRequest {
            nqueens: n,
            first_row,
            second_row,
        });
    
        let response = solver.solve(request).await;
        match response {
            Ok(res) => {
                info!("solver.solve response:{:?}", res.get_ref());
                return res.get_ref().count;
            },
            Err(e) => {
                info!("solver.solve error:{:?}", e);
                return -1; 
            }
        }
}

async fn mapper(solver: &mut NQueensServiceClient<tonic::transport::Channel>, n: i32) -> Vec<Shard> {
        let mut shards: i32 = 0;
        let (tx, mut rx) = mpsc::unbounded_channel();

        for i in 0..(n+1)/2 {
            if n <= THRESHOLD {
                shards += 1;
                let first_row = i;
                let tx_clone = tx.clone();
                let mut solver_clone = solver.clone();
                tokio::spawn(async move {
                    let count = solve(&mut solver_clone, n, first_row, -1).await;
                    let _ = tx_clone.send(Shard{
                        first_row,
                        second_row: -1,
                        count,
                    });
                });
            } else {
                for j in 0..n {
                    shards += 1;
                    let first_row = i;
                    let second_row = j;
                    let tx_clone = tx.clone();
                    let mut solver_clone = solver.clone();
                    tokio::spawn(async move {
                        let count = solve(&mut solver_clone, n, first_row, second_row).await;
                        let _ = tx_clone.send(Shard{
                            first_row,
                            second_row,
                            count,
                        });
                    });
                }
            }
        }

        let mut res = Vec::new();
        for _ in 0..shards {
            if let Some(shard) = rx.recv().await {
                res.push(shard);
            } 
        }

        res
}

fn verify(n: i32, shards: &[Shard]) {
	let mut results = HashMap::new();
    for shard in shards.iter() {
        let vec = results.entry(shard.first_row).or_insert(Vec::new());
        (*vec).push(shard.second_row);
    }
    info!("verify: n:{} results:{:?}", n, results);
}

fn reducer(n: i32, shards: &[Shard]) -> i64 {
        let mut results = HashMap::new();

        for shard in shards.iter() {
            let count = results.entry(shard.first_row).or_insert(0);
            *count += shard.count;
        }
        info!("reducer results:{:?}", results);

        let mut total: i64 = 0;
        for i in 0..(n+1)/2 {
            if n%2 == 1 && i == n/2 {
                total += results[&i];
            } else {
                total += 2 * results[&i];
            }
        }
        return total;
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    /// listen port
    #[clap(short, long)]
    #[clap(default_value_t = 8)]
    nqueens: i32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let options = Options::parse();
    info!("n = {}", options.nqueens);

    let mut solver = NQueensServiceClient::connect("http://127.0.0.1:9352").await?;

    let shards = mapper(&mut solver, options.nqueens).await;
    info!("shards = {:?}", shards);

    verify(options.nqueens, &shards);

    let solution = reducer(options.nqueens, &shards);
    info!("solution = {:?}", solution);

    Ok(())
}
