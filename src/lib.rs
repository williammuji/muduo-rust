pub mod countdown_latch;
pub use countdown_latch::CountdownLatch;

use tokio::net::{TcpListener, TcpStream};
use tokio::time::{self, Duration};

/// Error returned by most functions.
///
/// When writing a real application, one might want to consider a specialized
/// error handling crate or defining an error type as an `enum` of causes.
/// However, for our example, using a boxed `std::error::Error` is sufficient.
///
/// For performance reasons, boxing is avoided in any hot path. For example, in
/// `parse`, a custom error `enum` is defined. This is because the error is hit
/// and handled during normal execution when a partial frame is received on a
/// socket. `std::error::Error` is implemented for `parse::Error` which allows
/// it to be converted to `Box<dyn std::error::Error>`.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A specialized `Result` type for mini-redis operations.
///
/// This is defined as a convenience.
pub type Result<T> = std::result::Result<T, Error>;

pub async fn accept_backoff(listener: &TcpListener) -> Result<TcpStream> {
    let mut backoff = 1;

    loop {
        match listener.accept().await {
            Ok((socket, _)) => return Ok(socket),
            Err(err) => {
                if backoff > 64 {
                    return Err(err.into());
                }
            }
        }

        time::sleep(Duration::from_secs(backoff)).await;

        backoff *= 2;
    }
}
