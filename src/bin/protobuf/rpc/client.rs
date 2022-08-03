use sudokum::sudoku_service_client::SudokuServiceClient;
use sudokum::SudokuRequest;

pub mod sudokum {
    tonic::include_proto!("sudoku");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = SudokuServiceClient::connect("http://[::1]:9981").await?;

    let request = tonic::Request::new(SudokuRequest {
        checkerboard: "001010".into(),
    });

    let response = client.solve(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
