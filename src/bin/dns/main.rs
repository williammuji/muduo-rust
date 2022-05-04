use dns_lookup::lookup_host;
use tracing_subscriber;
use tracing::info;

fn main() {
    tracing_subscriber::fmt::init();

    {
        let host = "www.chenshuo.com";
        let ips: Vec<std::net::IpAddr> = lookup_host(host).unwrap();
        info!("{}: {:?}", host, ips);
    }
    {
        let host = "www.example.com";
        let ips: Vec<std::net::IpAddr> = lookup_host(host).unwrap();
        info!("{}: {:?}", host, ips);
    }
    {
        let host = "www.google.com";
        let ips: Vec<std::net::IpAddr> = lookup_host(host).unwrap();
        info!("{}: {:?}", host, ips);
    }
}
