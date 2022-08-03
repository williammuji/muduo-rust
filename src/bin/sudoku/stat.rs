use circular_queue::CircularQueue;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

const SECONDS: u64 = 60;

pub struct SudokuStat {
    last_second: u64,
    requests: CircularQueue<u64>,
    latencies: CircularQueue<u64>,
    total_requests: u64,
    total_responses: u64,
    total_solved: u64,
    bad_requests: u64,
    dropped_requests: u64,
    total_latency: u64,
    bad_latency: u64,
}

impl SudokuStat {
    pub fn new() -> SudokuStat {
        SudokuStat {
            last_second: 0,
            requests: CircularQueue::with_capacity(SECONDS as usize),
            latencies: CircularQueue::with_capacity(SECONDS as usize),
            total_requests: 0,
            total_responses: 0,
            total_solved: 0,
            bad_requests: 0,
            dropped_requests: 0,
            total_latency: 0,
            bad_latency: 0,
        }
    }
    pub fn report(&mut self) -> String {
        let mut res = String::new();
        res.push_str(&format!("total_requests {}\ntotal_responses {}\ntotal_solved {}\nbad_requests {}\ndropped_requests {}\nlatency_sum_us {}\n",
                             self.total_requests,
                             self.total_responses,
                             self.total_solved,
                             self.bad_requests,
                             self.dropped_requests,
                             self.total_latency));
        if self.bad_latency > 0 {
            res.push_str(&format!("bad_latency {}\n", self.bad_latency));
        }
        res.push_str(&format!("last_second {}\n", self.last_second));
        res.push_str(&format!("requests_per_second"));
        let mut requests: u64 = 0;
        self.requests.iter().for_each(|e| {
            requests += e;
            res.push_str(&format!(" {}", e));
        });
        res.push_str(&format!("\nrequests_60s {}\n", requests));

        res.push_str(&format!("latency_sum_us_per_second"));
        let mut latency: u64 = 0;
        self.latencies.iter().for_each(|e| {
            latency += e;
            res.push_str(&format!(" {}", e));
        });
        res.push_str(&format!("\nlatency_sum_us_60s {}\n", latency));

        let mut latency_avg_60s: u64 = 0;
        if requests != 0 {
            latency_avg_60s = latency / requests;
        }
        res.push_str(&format!("latency_us_60s {}\n", latency_avg_60s));

        let mut latency_avg: u64 = 0;
        if self.total_responses != 0 {
            latency_avg = self.total_latency / self.total_responses;
        }
        res.push_str(&format!("latency_us_avg {}\n", latency_avg));

        res
    }
    pub fn reset(&mut self) -> String {
        self.last_second = 0;
        self.requests.clear();
        self.latencies.clear();
        self.total_requests = 0;
        self.total_responses = 0;
        self.total_solved = 0;
        self.bad_requests = 0;
        self.dropped_requests = 0;
        self.total_latency = 0;
        self.bad_latency = 0;

        "reset done.".to_string()
    }
    pub fn record_response(&mut self, now: Duration, receive: Duration, solved: bool) {
        if now < receive {
            self.bad_latency += 1;
        }
        let elapsed_us = now.saturating_sub(receive).as_micros() as u64;
        assert!(self.requests.len() == self.latencies.len());

        self.total_responses += 1;
        if solved {
            self.total_solved += 1;
        }

        let second = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let first_second = self.last_second - self.requests.len() as u64 + 1;
        if self.last_second == second {
            self.add_request();
            self.add_latency(elapsed_us);
        } else if self.last_second + 1 == second || self.last_second == 0 {
            self.last_second = second;
            self.requests.push(0);
            self.latencies.push(0);
            self.add_request();
            self.add_latency(elapsed_us);
        } else if second > self.last_second {
            if second < self.last_second + SECONDS {
                while self.last_second < second {
                    self.requests.push(0);
                    self.latencies.push(0);
                    self.last_second += 1;
                }
            } else {
                self.requests.clear();
                self.latencies.clear();
                self.last_second = second;
                self.requests.push(0);
                self.latencies.push(0);
            }
            self.add_request();
            self.add_latency(elapsed_us);
        } else if second > first_second {
            let idx: u64 = second - first_second;
            assert!(idx < self.requests.len() as u64);

            let mut iter = self.requests.asc_iter_mut();
            for _ in 0..idx {
                iter.next();
            }
            *iter.next().unwrap() += 1;

            let mut iter = self.latencies.asc_iter_mut();
            for _ in 0..idx {
                iter.next();
            }
            *iter.next().unwrap() += elapsed_us;
        } else {
            assert!(second < first_second);
        }

        assert!(self.requests.len() == self.latencies.len());
    }
    pub fn record_request(&mut self) {
        self.total_requests += 1;
    }
    pub fn record_bad_request(&mut self) {
        self.bad_requests += 1;
    }

    pub fn _record_dropped_request(&mut self) {
        self.dropped_requests += 1;
    }

    pub fn add_request(&mut self) {
        if let Some(request) = self.requests.iter_mut().next() {
            *request += 1;
        }
    }
    pub fn add_latency(&mut self, elapsed_us: u64) {
        if let Some(latency) = self.latencies.iter_mut().next() {
            *latency += elapsed_us;
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::prelude::*;
    use std::time::Duration;

    use super::SudokuStat;

    #[test]
    fn same_second() {
        let mut stat = SudokuStat::new();
        for i in 0..100 {
            let start = Duration::from_secs(1234567890);
            let recv = start + Duration::from_secs(0);
            let send = start + Duration::from_secs(1);
            stat.record_response(send, recv, i % 3 != 0);
        }
        println!("same second:\n{}\n", stat.report());
    }

    #[test]
    fn next_second() {
        let mut stat = SudokuStat::new();

        let start = Duration::from_secs(1234567890);
        let mut recv = start + Duration::from_secs(0);
        let mut send = start + Duration::from_millis(2);
        for _ in 0..10000 {
            stat.record_response(send, recv, true);
            recv = send + Duration::from_millis(10);
            send = recv + Duration::from_millis(20);
        }
        println!("next second\n{}\n", stat.report());
    }

    #[test]
    fn fuzz() {
        let mut stat = SudokuStat::new();

        let mut start = Duration::from_secs(1234567890);
        for _ in 0..10000 {
            let recv = start + Duration::from_secs(0);
            let send = start + Duration::from_secs(200);
            stat.record_response(send, recv, true);
            let jump: u64 = rand::random::<u64>() % 200;
            if jump >= 100 {
                start = start.saturating_add(Duration::from_secs(jump - 100));
            } else {
                start = start.saturating_sub(Duration::from_secs(100 - jump));
            }
        }
    }

    #[test]
    fn jump_ahead5() {
        let mut stat = SudokuStat::new();

        let start = Duration::from_secs(1234567890);
        let mut recv = start + Duration::from_secs(0);
        let mut send = start + Duration::from_secs(200);
        stat.record_response(send, recv, true);

        recv = recv + Duration::from_secs(4);
        send = send + Duration::from_secs(5);
        stat.record_response(send, recv, true);
        println!("jump ahead 5 seconds\n{}\n", stat.report());
    }

    #[test]
    fn jump_ahead59() {
        let mut stat = SudokuStat::new();

        let start = Duration::from_secs(1234567890);
        let mut recv = start + Duration::from_secs(0);
        let mut send = start + Duration::from_secs(200);
        stat.record_response(send, recv, true);

        recv = recv + Duration::from_secs(55);
        send = send + Duration::from_secs(59);
        stat.record_response(send, recv, true);
        println!("jump ahead 59 seconds\n{}\n", stat.report());
    }

    #[test]
    fn jump_ahead60() {
        let mut stat = SudokuStat::new();

        let start = Duration::from_secs(1234567890);
        let mut recv = start + Duration::from_secs(0);
        let mut send = start + Duration::from_secs(200);
        stat.record_response(send, recv, true);

        recv = recv + Duration::from_secs(58);
        send = send + Duration::from_secs(60);
        stat.record_response(send, recv, true);
        println!("jump ahead 60 seconds\n{}\n", stat.report());
    }

    #[test]
    fn jump_back3() {
        let mut stat = SudokuStat::new();

        let start = Duration::from_secs(1234567890);
        let mut recv = start + Duration::from_secs(0);
        let mut send = start + Duration::from_secs(200);
        stat.record_response(send, recv, true);

        recv = recv + Duration::from_secs(9);
        send = send + Duration::from_secs(10);
        stat.record_response(send, recv, true);

        recv = recv - Duration::from_secs(4);
        send = send - Duration::from_secs(3);
        stat.record_response(send, recv, true);

        println!("jump back 3 seconds\n{}\n", stat.report());
    }
}
