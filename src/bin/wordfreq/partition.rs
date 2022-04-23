use tracing::info;
use superslice::Ext;

pub trait GetHistogram {
    fn get_histogram(&self, keys: &Vec<i64>, pivots: &Vec<i64>) -> Vec<i64>;
}

pub fn partition(max_key: i64, key_count: i64, keys: &Vec<i64>, n_workers: i32, target: &impl GetHistogram) -> Vec<i64> {
    assert!(max_key > 0);
    assert!(key_count > 0);
    assert!(n_workers > 0);

    let mut slots_per_worker: i32 = 32;
    let mut n_pivots: i64;
    let mut pivots: Vec<i64> = Vec::new();
    let mut histogram: Vec<i64> = Vec::new();

    let mut done = false;
    while !done {
        n_pivots = (slots_per_worker * n_workers) as i64;
        let mut increment = max_key / n_pivots;
        if increment == 0 {
            increment = 1;
            n_pivots = max_key;
        }

        pivots.clear();
        for i in 1..n_pivots {
            pivots.push(i * increment);
        }
        pivots.push(max_key);

        assert!(pivots.len() == n_pivots as usize);

        histogram = target.get_histogram(&keys, &pivots);

        let mut retry = false;
        for count in histogram.iter() {
            if *count > 2 * key_count / n_pivots {
                info!("retry");
                retry = true;
                break;
            }
        }

        if retry && slots_per_worker <= 1024 {
            // FIXME: use adaptive method, only insert pivots in dense slot
            slots_per_worker *= 2;
        } else {
            done = true;
        }
    }

    let mut result: Vec<i64> = Vec::new();

    let cum: Vec<i64> = histogram.iter().scan(0, |acc, &x| {
        *acc += x;
        Some(*acc)
    }).collect();
    assert!(*cum.last().unwrap() == key_count);
    let per_worker = key_count / n_workers as i64;
    for i in 1..n_workers as i64 { 
        let idx = cum.lower_bound(&(i * per_worker));
        assert!(idx < cum.len());
        result.push(pivots[idx]);
    }
    result.push(max_key);
    assert!(result.len() == n_workers as usize);
    assert!(*result.last().unwrap() == max_key);

    result
}
