use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub fn custom_partition(instrument: &str, partition_count: i32) -> i32 {
    let mut hasher = DefaultHasher::new();
    instrument.hash(&mut hasher);
    let hash = hasher.finish();
    (hash % partition_count as u64) as i32
}
