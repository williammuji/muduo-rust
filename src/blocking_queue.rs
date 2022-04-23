use std::sync::{Arc, Mutex, Condvar};
use std::collections::VecDeque;

#[derive(Clone)]
pub struct BlockingQueue<T> {
    pair: Arc<(Mutex<VecDeque::<T>>, Condvar)>,
}

impl<T> BlockingQueue<T> {
    pub fn new() -> Self {
        BlockingQueue {
            pair: Arc::new((Mutex::new(VecDeque::<T>::new()), Condvar::new())),
        }
    }

    pub fn put(&self, t: T) {
        let (lock, cvar) = &*self.pair;
        let mut queue = lock.lock().unwrap();
        queue.push_back(t);
        cvar.notify_all();
    }

    pub fn take(&self) -> T {
        let (lock, cvar) = &*self.pair;
        let mut queue = lock.lock().unwrap();
        while queue.is_empty() {
            queue = cvar.wait(queue).unwrap();
        }
        assert!(!queue.is_empty());
        let front = queue.pop_front();
        front.unwrap()
    }

    pub fn len(&self) -> usize {
        let (lock, _) = &*self.pair;
        let queue = lock.lock().unwrap();
        queue.len() 
    }
}
