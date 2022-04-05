use tonic::{transport::Server, Request, Response, Status};

use median::sorter_server::{Sorter, SorterServer};
use median::{Empty, QueryResponse, SearchRequest, SearchResponse, GenerateRequest};

use tracing_subscriber;
use tracing::info;
use clap::Parser;
use superslice::Ext;

pub mod median {
    tonic::include_proto!("median");
}

#[derive(Default)]
pub struct TheSorter {
    data: Vec<i64>,
}

impl TheSorter {
    fn do_generate(&mut self, count: i64, min: i64, max: i64) {
        for _ in 0..count {
            let range = max - min;
            let mut value = min;
            if range > 1 {
                value += rand::random::<u32>() as i64 % range;
            }
            self.data.push(value);
        }
        self.data.sort();
        info!("{:?}", self.data);
    }
}

#[tonic::async_trait]
impl Sorter for TheSorter {
    async fn query(
        &self,
        request: Request<Empty>,
        ) -> Result<Response<QueryResponse>, Status> {
        info!("Sorter.query from {:?} request:{:?}", request.remote_addr(), request.get_ref());

        let mut response = QueryResponse {
            count: self.data.len() as i64,
            min: 0,
            max: 0,
            sum: 0,
        };

        if !self.data.is_empty() {
            response.min = self.data[0];
            response.max = *(self.data.last().unwrap());
            response.sum = self.data.iter().sum();
        }

        Ok(Response::new(response))
    }
    
    async fn search(
        &self,
        request: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
        info!("Sorter.search from {:?} request:{:?}", request.remote_addr(), request.get_ref());

        let guess = request.get_ref().guess;
      
        let smaller = self.data.lower_bound(&guess);
        let upper = self.data.upper_bound(&guess);

        let response = SearchResponse {
            smaller: smaller as i64,
            same: (upper - smaller) as i64,
        };
        info!("response:{:?}", response);

        Ok(Response::new(response))
    }
    
    async fn generate(
        &self,
        request: Request<GenerateRequest>,
        ) -> Result<Response<Empty>, Status> {
        info!("Sorter.generate from {:?} request:{:?}", request.remote_addr(), request.get_ref());

        // let req = request.get_ref();
        // self.do_generate(req.count, req.min, req.max);
       
        let response = Empty{};

        Ok(Response::new(response))
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    /// listen port
    #[clap(short, long)]
    #[clap(default_value_t = String::from("127.0.0.1:5555"))]
    addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
   
    let options = Options::parse();
    info!("Sorter listening on {}", options.addr);
    
    let mut sorter_service = TheSorter::default();
    sorter_service.do_generate(100, 0, 30);

    Server::builder()
        .add_service(SorterServer::new(sorter_service))
        .serve(options.addr.parse().unwrap())
        .await?;

    Ok(())
}
