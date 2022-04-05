use tracing::{error, info};
use async_trait::async_trait;

#[async_trait]
pub trait KthSearch {
    async fn search(&mut self, _: i64) -> (i64, i64); 
}

pub async fn get_kth(target: &mut impl KthSearch, k: i64, mut min: i64, mut max: i64) -> (i64, bool) { 
    let mut steps: i32 = 0;
    let mut guess: i64 = max;
    let mut succeed = false;

    while min <= max {
        let smaller: i64;
        let same: i64;

        let (res_smaller, res_same) = target.search(guess).await; 
        smaller = res_smaller;
        same = res_same;
        info!("guess = {}, smaller = {}, same = {}, min = {}, max = {}", guess, smaller, same, min, max);

        steps += 1;
        if steps > 100 {
            error!("Algorithm failed, too many steps");
            break;
        }

        if smaller < k && smaller + same >= k {
            succeed = true;
            break;
        }

        if smaller + same < k {
            min = guess + 1;
        } else if smaller >= k {
            max = guess;
        } else {
            error!("Algorithm failed, no improvement");
            break;
        }

        guess = min + ((max as i128 - min as i128)/2) as i64;
        assert!(min <= guess && guess <= max);
    }

    (guess, succeed)
}
