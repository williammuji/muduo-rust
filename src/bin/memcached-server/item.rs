use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::str;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum UpdatePolicy {
    Invalid,
    Set,
    Add,
    Replace,
    Append,
    Prepend,
    Cas,
}

#[derive(Debug)]
pub struct Item {
   key_len: usize,
   flags: u32,
   rel_exptime: i32,
   value_len: usize,
   received_bytes: usize,
   cas: u64,
   hash: u64,
   pub data: Vec<u8>,
}

impl Item {
    pub fn new(key: &[u8], flags: u32, exptime: i32, value_len: usize, cas: u64) -> Arc<Item> {
        assert!(value_len >= 2);
            
        let mut hasher = DefaultHasher::new();
        hasher.write(&key[0..key.len()]);

        let mut item = Arc::new(Item{
            key_len: key.len(),
            flags,
            rel_exptime: exptime,
            value_len,
            received_bytes: 0,
            cas,
            hash: hasher.finish(),
            data: Vec::with_capacity(key.len() + value_len), 
        });
        assert!(item.received_bytes < item.total_len());

        (*Arc::get_mut(&mut item).unwrap()).append(key);
        
        item
    }

    pub fn key_len(&self) -> usize {
        self.key_len
    }

    pub fn total_len(&self) -> usize {
        self.key_len + self.value_len
    }

    pub fn ends_with_crlf(&self) -> bool {
        self.received_bytes == self.total_len()
            && self.data[self.total_len()-2] == b'\r'
            && self.data[self.total_len()-1] == b'\n'
    }

    pub fn needed_bytes(&self) -> usize {
        self.total_len() - self.received_bytes
    }

    pub fn set_cas(&mut self, cas: u64) {
        self.cas = cas; 
    }

    pub fn hash(&self) -> u64 {
        self.hash 
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn value_length(&self) -> usize {
        self.value_len 
    }

    pub fn rel_exptime(&self) -> i32 {
        self.rel_exptime
    }

    pub fn flags(&self) -> u32 {
        self.flags
    }

    pub fn append(&mut self, data: &[u8]) {
        assert!(data.len() <= self.needed_bytes());
        self.data.extend_from_slice(data);
        self.received_bytes += data.len();
        assert!(self.received_bytes <= self.total_len());
    }

    pub fn output(&self, out: &mut String, need_cas: bool) {
        out.push_str("VALUE ");
        out.push_str(str::from_utf8(&self.data[0..self.key_len]).unwrap());
        
        out.push_str(&format!(" {} {}", self.flags, self.value_len-2));
        if need_cas {
            out.push_str(&format!(" {}", self.cas));
        }
        out.push_str("\r\n");
       
        out.push_str(str::from_utf8(&self.data[self.key_len..self.total_len()]).unwrap());
    }

    pub fn reset_key(&mut self, k: &str) {
        let k = k.as_bytes();
        
        assert!(k.len() <= 250);
        self.key_len = k.len();
        self.received_bytes = 0;
        
        for i in 0..k.len() {
            self.data[self.received_bytes+i] = k[i];
        }
        self.received_bytes += k.len();

        let mut hasher = DefaultHasher::new();
        hasher.write(&self.data[0..k.len()]);
        self.hash = hasher.finish();
    }
}

impl Hash for Item {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.data[0..self.key_len]);
    }
}

impl PartialEq for Item {
    fn eq(&self, other: &Self) -> bool {
        self.data[0..self.key_len] == other.data[0..other.key_len]
    }
}

impl Eq for Item {}
