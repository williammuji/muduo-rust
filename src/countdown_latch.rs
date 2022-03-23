use std::sync::{Arc, Mutex, Condvar};

#[derive(Clone)]
pub struct CountdownLatch {
    pair: Arc<(Mutex<i32>, Condvar)>,
}

impl CountdownLatch {
    pub fn new(count: i32) -> CountdownLatch {
        CountdownLatch {
            pair: Arc::new((Mutex::new(count), Condvar::new())),
        }
    }

    pub fn wait(&self) {
        let (lock, cvar) = &*self.pair;
        let mut count = lock.lock().unwrap();
        while *count > 0 {
            count = cvar.wait(count).unwrap();
        }
    }

    pub fn countdown(&self) {
        let (lock, cvar) = &*self.pair;
        let mut count = lock.lock().unwrap();
        *count -= 1;
        cvar.notify_all();
    }
}
