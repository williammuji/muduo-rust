use superslice::Ext;
use tracing::{info};
use async_trait::async_trait;
use crate::kth::KthSearch;

#[path = "../src/bin/median/kth.rs"]
mod kth;

const VALUES: &[i64] = &[i64::MIN, i64::MIN, i64::MIN, -2, -1, 0, 1, 2, i64::MAX-2, i64::MAX-1, i64::MAX];
const N_VALUES: usize = VALUES.len();

struct Collector {
    data: Vec<i64>,
}

#[async_trait]
impl KthSearch for Collector {
    async fn search(&mut self, guess: i64) -> (i64, i64) {
        let smaller = self.data.lower_bound(&guess) as i64;
        let upper = self.data.upper_bound(&guess) as i64;
        (smaller, upper-smaller)
    }
}

impl Collector {
    fn new(arr: &[i64]) -> Collector {
        let mut collector = Collector {
            data: Vec::with_capacity(arr.len()),
        };
        collector.data.extend_from_slice(arr);
        collector.data.sort();
        collector
    }
    
    async fn check(&mut self) -> bool {
        for i in 0..self.data.len() {
            let (guess, succeed) = kth::get_kth(self, (i+1) as i64, *(self.data.first().unwrap()), *(self.data.last().unwrap())).await;
            if !(succeed && guess == self.data[i]) {
                println!("i = {} {:?}", i, self.data);
                return false;
            }
            info!("***** Result is {}", guess);
        }
        return true;
    }
}


#[tokio::test]
async fn one_element() {
    for i in 0..N_VALUES {
        let data = [ VALUES[i] ];
        info!("***** Input is {:?}", data);
        let mut collector = Collector::new(&data);
        assert!(collector.check().await);
    }
}

#[tokio::test]
async fn two_elements() {
    for i in 0..N_VALUES {
        for j in 0..N_VALUES {
            let data = [ VALUES[i], VALUES[j] ];
            let mut collector = Collector::new(&data);
            assert!(collector.check().await);
        }
    }
}

#[tokio::test]
async fn three_elements() {
    let mut indices = [0, 0, 0];

    while indices[0] < N_VALUES {
        let data = [ VALUES[indices[0]], VALUES[indices[1]], VALUES[indices[2]] ];
        let mut collector = Collector::new(&data);
        assert!(collector.check().await);

        indices[2] += 1;
        if indices[2] >= N_VALUES {
            indices[2] = 0;
            indices[1] += 1;
            if indices[1] >= N_VALUES {
                indices[1] = 0;
                indices[0] += 1;
            }
        }
    }
}

#[tokio::test]
async fn four_elements() {
    let mut indices = [0, 0, 0, 0];
    let mut running = true;

    while running {
        let data = [ VALUES[indices[0]], VALUES[indices[1]], VALUES[indices[2]], VALUES[indices[3]] ];
        let mut collector = Collector::new(&data);
        assert!(collector.check().await);

        let mut d: i64 = 3;
        while d >= 0 {
            indices[d as usize] += 1;
            if indices[d as usize] >= N_VALUES {
                if d == 0 {
                    running = false;
                }
        indices[d as usize] = 0;
        d -= 1;
      } else {
        break;
      }
    }
  }
}
