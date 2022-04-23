use superslice::Ext;
use crate::partition::GetHistogram;
use rand::prelude::*;

#[path = "../src/bin/wordfreq/partition.rs"]
mod partition;

struct GetHistogramFromKey {
}

impl GetHistogram for GetHistogramFromKey {
    // REQUIRE: keys and pivots are sorted
    fn get_histogram(&self, keys: &Vec<i64>, pivots: &Vec<i64>) -> Vec<i64> {
        let mut result: Vec<i64> = vec![0; pivots.len()]; 
        let mut j: usize = 0;
        for i in 0..keys.len() {
            while keys[i] > pivots[j] {
                j += 1;
                if j >= pivots.len() {
                    return result;
                }
            }
            result[j] += 1;
        }
        result
    }
}

#[test]
fn test_get_histogram() {
    let mut keys: Vec<i64> = Vec::new();
    let mut pivots: Vec<i64> = Vec::new();
    let mut expected: Vec<i64> = Vec::new();

    keys.extend_from_slice(&[1, 2, 3, 4, 5]);
    pivots.extend_from_slice(&[1, 2, 3, 4, 5]);
    expected.extend_from_slice(&[1, 1, 1, 1, 1]);

    let get_hist = GetHistogramFromKey{};
    let histogram = get_hist.get_histogram(&keys, &pivots);
    assert!(histogram == expected);

    pivots.clear(); expected.clear();
    pivots.push(3);
    expected.push(3);
    let histogram = get_hist.get_histogram(&keys, &pivots);
    assert!(histogram == expected);

    pivots.clear(); expected.clear();
    pivots.push(6);
    expected.push(5);
    let histogram = get_hist.get_histogram(&keys, &pivots);
    assert!(histogram == expected);

    pivots.clear(); expected.clear();
    pivots.extend_from_slice(&[1, 3, 5]);
    expected.extend_from_slice(&[1, 2, 2]);
    let histogram = get_hist.get_histogram(&keys, &pivots);
    assert!(histogram == expected);

    keys.clear();
    keys.extend_from_slice(&[1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 7]);

    pivots.clear(); expected.clear();
    pivots.extend_from_slice(&[1, 3, 5]);
    expected.extend_from_slice(&[2, 4, 4]);
    let histogram = get_hist.get_histogram(&keys, &pivots);
    assert!(histogram == expected);

    pivots.clear(); expected.clear();
    pivots.extend_from_slice(&[1, 3, 5, 6]);
    expected.extend_from_slice(&[2, 4, 4, 0]);
    let histogram = get_hist.get_histogram(&keys, &pivots);
    assert!(histogram == expected);
}

#[test]
fn test1() {
    let mut keys: Vec<i64> = [1, 2, 3, 4, 5].to_vec();

    let get_histogram = GetHistogramFromKey{};
    let pivots = partition::partition(*keys.last().unwrap(), keys.len() as i64, &keys, 1, &get_histogram);

    assert!(pivots.len() == 1);
    assert!(pivots[0] == 5);

    let pivots = partition::partition(*keys.last().unwrap(), keys.len() as i64, &keys, 2, &get_histogram);

    assert!(pivots.len() == 2);
    assert!(pivots[0] == 2);
    assert!(pivots[1] == 5);

    keys.push(6);
    let pivots = partition::partition(*keys.last().unwrap(), keys.len() as i64, &keys, 2, &get_histogram);

    assert!(pivots.len() == 2);
    assert!(pivots[0] == 3);
    assert!(pivots[1] == 6);
}

#[test]
fn test_uniform() {
    let max_key: i32 = 1000 * 1000;
    let count: usize = 10000;
    let mut keys: Vec<i64> = Vec::with_capacity(count); 
    let mut rng = thread_rng(); 
    for _ in 0..count {
        keys.push(rng.gen_range(0..max_key) as i64);
    }
    keys.sort();

    let get_histogram = GetHistogramFromKey{};
    let pivots = partition::partition(*keys.last().unwrap(), keys.len() as i64, &keys, 1, &get_histogram);

    assert!(pivots.len() == 1);
    assert!(pivots[0] == *keys.last().unwrap());

    {
        let pivots = partition::partition(*keys.last().unwrap(), keys.len() as i64, &keys, 2, &get_histogram);
        assert!(pivots.len() == 2);

        let p0 = keys.iter().filter(|&key| *key <= pivots[0]).count();
        let per_worker = (keys.len() / pivots.len()) as f64;
        let skew = (p0 as f64 / per_worker - 1.0).abs();
        assert!(skew < 0.05);
        assert!(pivots[1] == *keys.last().unwrap());
    }

    for n_workers in 1..10 {
        let pivots = partition::partition(*keys.last().unwrap(), keys.len() as i64, &keys, n_workers, &get_histogram); 
        assert!(pivots.len() == n_workers as usize); 

        let per_worker = (keys.len() / pivots.len()) as f64;

        let mut last: i64 = 0;
        println!("workers:{}", n_workers);
        for i in 0..n_workers as usize {
            let less_equal = keys.upper_bound(&pivots[i]) as i64;
            let this_count = less_equal - last;
            last = less_equal;
            let skew = this_count as f64 / per_worker - 1.0;
            println!("i:{:3} skew:{}", i, skew);
            assert!(skew.abs() < 0.05);
        }
    }
}
