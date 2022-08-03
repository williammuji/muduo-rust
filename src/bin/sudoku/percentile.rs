use std::fs::OpenOptions;
use std::io::{BufWriter, Write};

const INTERVAL: i32 = 5;

pub fn get_percentile(latencies: &[i32], percent: usize) -> i32 {
    assert!(!latencies.is_empty());
    let mut idx: usize = 0;
    if percent > 0 {
        idx = (latencies.len() * percent + 99) / 100 - 1;
        assert!(idx < latencies.len());
    }

    latencies[idx]
}

pub struct Percentile {
    pub stat: String,
}

impl Percentile {
    pub fn new(latencies: &mut [i32], infly: i32) -> Percentile {
        let mut stat = String::new();
        stat.push_str(&format!("recv {:06} in-fly {}", latencies.len(), infly));
        if !latencies.is_empty() {
            latencies.sort();
            let min = &latencies[0];
            let max = &latencies[latencies.len()-1];
            let sum: i32 = latencies.iter().sum();
            let mean = sum / latencies.len() as i32;
            let median = get_percentile(latencies, 50);
            let p90 = get_percentile(latencies, 90);
            let p99 = get_percentile(latencies, 99);
            stat.push_str(&format!(" min {} max {} avg {} median {} p90 {} p99 {}", min, max, mean, median, p90, p99));
        }

        Percentile{
            stat,    
        }
    }

    pub fn save(&mut self, latencies: &[i32], name: &str) -> std::io::Result<()> {
        if latencies.is_empty() {
            return Ok(());
        }

        let file = OpenOptions::new().write(true).create(true).open(name).unwrap();
        let mut buf = BufWriter::new(file);
        buf.write_all(b"# ")?;
        buf.write_all(self.stat.as_bytes())?;
        buf.write_all(b"\n")?;

        let mut low: i32 = latencies.first().unwrap() / INTERVAL * INTERVAL;
        let mut count: i32 = 0;
        let mut sum: i32 = 0;
        let total = latencies.len() as i32;
        for latency in latencies.iter() {
            write!(buf, "# {}\n", latency)?; 
        }

        for latency in latencies.iter() {
            if *latency < low + INTERVAL {
                count += 1;
            } else {
                sum += count;
                write!(buf, "{:04} {:05} {:5.2}", low, count, 100.0 * sum as f32 / total as f32)?;
                low = latency / INTERVAL * INTERVAL;
                assert!(*latency < low + INTERVAL);
                count = 1;
            }
        }

        sum += count;
        assert!(sum == total);
        write!(buf, "{:04} {:05} {:5.1}", low, count, 100*sum/total)?;
        buf.flush()?;
    
        Ok(())
    }
}
