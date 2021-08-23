//! A fast key-value store with ACID guarentees written in pure Rust.


use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use tokio::io::{BufWriter, BufReader};
use tokio::sync::{oneshot, mpsc};
use tokio::task::spawn;

use std::io::SeekFrom;
use std::io::{Error, ErrorKind};
use std::path::{PathBuf, Path};
use std::convert::TryInto;
use std::sync::Arc;
use std::time::{Instant, Duration};
use std::cmp::Ordering;
use std::collections::BTreeMap;



/// Contains the parts responsible for detecting commit conflicts.
pub mod oracle;
use oracle::*;

/// Contains the parts responsible for maintaining key-value-pairs on disk.
pub mod vlog;
use vlog::*;

/// Contains the parts responsible for maintaining the most recently added keys index inside memory.
pub mod store;
use store::*;

/// Contains the parts responsible for maintaining the older keys index on disk using a LSM-tree.
pub mod lsm;
use lsm::*;



/// The main object containing the connection to the database.
pub struct DB {
    /// The folder where the VLog files are saved.
    pub vlog_path: PathBuf,

    /// The internal parts of the database. Get shared with all transactions for quick access.
    inner: Arc<DBInner>,
}

struct DBInner {
    /// The key/value store.
    kv_store: KVStoreHandle,
    /// The oracle checking commit conflicts.
    oracle: OracleHandle,
    /// The VLog file. TODO Add multiple of them which are written at once.
    vlog: VLogHandle,
}

impl DB {
    pub async fn open(vlog_path: impl AsRef<Path>) -> std::io::Result<Self> {
        let vlog_path = vlog_path.as_ref().to_path_buf();

        let vlog = match VLog::new(0, &vlog_path).await {
            Ok(vlog) => vlog,
            Err(_) => VLog::open(0, &vlog_path).await?,
        };
        let vlog = vlog.run();

        let kv_store = KVStore::new();
        let kv_store = kv_store.run();

        let oracle = Oracle::new(3_000_000, 100_000);
        let oracle = oracle.run();

        let inner = Arc::new(DBInner {
            kv_store,
            oracle,
            vlog,
        });

        Ok(Self {
            vlog_path,
            inner,
        })
    }

    pub async fn new_txn(&self) -> Txn {
        let read_ts = self.inner.oracle.get_read_ts().await;
        // let read_ts = 0;
        let inner = Arc::clone(&self.inner);

        Txn {
            inner,
            read_ts,
            keys_read: Some(Vec::with_capacity(128)),
            keys_set: Some(Vec::with_capacity(128)),
            finished: false,
        }
    }

    pub async fn new_read_only_txn(&self) -> Txn {
        let read_ts = self.inner.oracle.get_read_ts().await;
        // let read_ts = 0;
        let inner = Arc::clone(&self.inner);

        Txn {
            inner,
            read_ts,
            keys_read: None,
            keys_set: None,
            finished: false,
        }
    }
}



/// A database transaction with ACID guarentees.
pub struct Txn {
    /// The internal parts of the database.
    inner: Arc<DBInner>,

    /// The read timestamp for this transaction.
    read_ts: u128,

    /// The keys which we have read so far, including those which were not present.
    keys_read: Option<Vec<u128>>,
    /// The keys which we have updated.
    keys_set: Option<Vec<(u128, Vec<u8>)>>,

    /// Indicates that this transactions got already finished and may no longer be used.
    finished: bool,
}

impl Txn {
    pub async fn get(&mut self, key: u128) -> std::io::Result<Option<Vec<u8>>> {
        self.check_not_finished()?;

        if let Some(keys_read) = &mut self.keys_read {
            keys_read.push(key);
        }

        if self.keys_set.is_some() {
            if let Some(value) = self.get_cached_value_inner(key) {
                return Ok(Some(value));
            }
        }

        let pair = match self.inner.kv_store.get(key, self.read_ts).await? {
            Some(pair) => pair,
            None => return Ok(None),
        };

        // if let Some(value) = pair.value {
        //     return Ok(Some(value));
        // }

        let value = match self.inner.vlog.read_value(
            key,
            // pair.version,
            pair.vlog_offset,
        ).await {
            Ok(value) => value,
            Err(err) => {
                self.finished = true;

                return Err(err);
            }
        };

        Ok(Some(value))
    }

    pub fn set(&mut self, key: u128, value: Vec<u8>) -> std::io::Result<()> {
        self.check_not_finished()?;
        self.check_not_read_only()?;

        self.set_key_inner(key, value);

        Ok(())
    }

    pub async fn commit(&mut self) -> std::io::Result<()> {
        self.check_not_finished()?;
        self.check_not_read_only()?;

        self.finished = true;

        let mut keys_read = self.keys_read.take().unwrap();
        let keys_set = self.keys_set.take().unwrap();
        let mut keys_set_mini: Vec<u128> = keys_set.iter().map(|&(key, _)| key).collect();

        keys_read.sort();
        keys_set_mini.sort();

        log::info!("Commit - Asking the oracle...");
        let now = Instant::now();

        // Ask the oracle if we can commit.
        let commit_ts = match self.inner.oracle.commit(keys_read, keys_set_mini, self.read_ts).await {
            Some(commit_ts) => commit_ts,
            None => return Err(Error::new(ErrorKind::Other, "Conflict detected.")),
        };

        let elapsed = now.elapsed();
        log::info!("... done. Took {:?}.", elapsed);
        // let commit_ts = {
        //     let mut oracle = self.inner.oracle.lock().unwrap();
        //     *oracle = *oracle+1;
        //     *oracle
        // };


        log::info!("Commit - Writing to disk...");
        let now = Instant::now();

        // Write the new values to the value log.
        let mut pairs = match self.inner.vlog.write_txn(keys_set, commit_ts).await {
            Ok(pairs) => pairs,
            Err(err) => {
                // Inform the oracle of the failed commit.
                self.inner.oracle.abort_commit(commit_ts).await;

                return Err(err);
            }
        };

        let elapsed = now.elapsed();
        log::info!("... done. Took {:?}.", elapsed);


        log::info!("Commit - Presorting the pairs...");
        let now = Instant::now();

        // Presort for better performance.
        pairs.sort();

        let elapsed = now.elapsed();
        log::info!("... done. Took {:?}.", elapsed);


        log::info!("Commit - Adding to the store...");
        let now = Instant::now();

        self.inner.kv_store.insert(pairs).await.unwrap(); // TODO add error handling (requires LSM file error handling...)

        let elapsed = now.elapsed();
        log::info!("... done. Took {:?}.", elapsed);


        log::info!("Commit - Telling the oracle the commit has finished...");
        let now = Instant::now();

        // Inform the oracle of the finished commit.
        self.inner.oracle.finish_commit(commit_ts).await;

        let elapsed = now.elapsed();
        log::info!("... done. Took {:?}.", elapsed);

        Ok(())
    }


    fn set_key_inner(&mut self, key: u128, value: Vec<u8>) {
        let keys_set = self.keys_set.as_mut().unwrap();

        keys_set.push((key, value)); // TODO first search for the key, and update it if already present.
    }

    fn get_cached_value_inner(&self, key: u128) -> Option<Vec<u8>> {
        let keys_set = self.keys_set.as_ref().unwrap();

        match keys_set.binary_search_by(|probe| probe.0.cmp(&key)) {
            Ok(idx) => return Some(keys_set[idx].1.clone()),
            Err(_) => return None,
        }
    }

    fn check_not_finished(&self) -> std::io::Result<()> {
        if self.finished {
            return Err(Error::new(ErrorKind::Other, "Txn is already finished."));
        }

        Ok(())
    }

    fn check_not_read_only(&self) -> std::io::Result<()> {
        if self.keys_read.is_none() {
            return Err(Error::new(ErrorKind::Other, "Txn is read only."));
        }

        Ok(())
    }
}



pub fn merge_sorted_with_indexes<T>(
    old: &mut Vec<T>,
    mut new: Vec<T>,
    new_indexes: Vec<usize>,
) -> Vec<T> {
    assert_eq!(new.len(), new_indexes.len());

    let mut merged = Vec::with_capacity(old.len()+new.len());
    let mut old = old.drain(..);
    let mut new = new.drain(..);

    for idx in new_indexes {
        while idx>merged.len() {
            merged.push(old.next().unwrap());
        }

        merged.push(new.next().unwrap());
    }

    assert!(new.next().is_none());

    while let Some(entry) = old.next() {
        merged.push(entry);
    }

    merged
}

// pub fn merge_multi<T, R, W>(
//     mut old: Vec<R>,
//     new: &mut W,
// ) -> Vec<T>
// where
//     T: Ord,
//     R: AsyncReadExt + AsyncSeekExt + Send + Sync + Unpin,
//     W: AsyncWriteExt + Send + Sync + Unpin
// {
//     let mut old: Vec<(R, Option<KVPair>)> = old.drain(..).map(|reader| (reader, None)).collect();
//
//     // Initialize - load the first pair from each file.
//     for element in old.iter_mut() {
//         let pair = self.read_nth(m).await?;
//     }
//
//     loop {
//
//     }
//
//     let mut total = old.len();
//     for list in new.iter() {
//         total = total + list.len();
//     }
//
//     let mut merged: Vec<_> = Vec::with_capacity(total);
//
//     let mut all_iters = Vec::with_capacity(new.len()+1);
//     all_iters.push(old.drain(..));
//     for list in new.iter_mut() {
//         all_iters.push(list.drain(..));
//     }
//
//     loop {
//         all_iters.retain()
//     }
//
//
//     merged
// }



#[cfg(test)]
mod test {
    #[test]
    fn merge_sorted_with_indexes() {
        let old: Vec<u64> = vec![0, 2, 3, 4, 6, 8, 10];
        let new: Vec<u64> = vec![5, 7];
        let new_indexes: Vec<usize> = vec![4, 6];

        let merged = super::merge_sorted_with_indexes(old, new, new_indexes);

        assert_eq!(merged, vec![0, 2, 3, 4, 5, 6, 7, 8, 10]);
    }

    #[test]
    fn merge_in_place() {
        let mut store: Vec<u64> = vec![0, 2, 3, 4, 6, 8, 10];
        let mut new_pairs: Vec<u64> = vec![5, 7];
        let new_pairs_indexes: Vec<usize> = vec![4, 6];

        store.append(&mut new_pairs);

        assert_eq!(store, vec![0, 2, 3, 4, 6, 8, 10, 5, 7]);

        let mut i = new_pairs_indexes.len()-1;
        let mut last_index = store.len();
        loop {
            let next_index: usize = new_pairs_indexes[i];
            let nr_elements_left = i+1;

            let begin = next_index+1-nr_elements_left;
            let end = last_index;

            store[begin..end].rotate_right(nr_elements_left);

            last_index = next_index;

            if i==0 {
                break;
            }
            i = i-1;
        }

        assert_eq!(store, vec![0, 2, 3, 4, 5, 6, 7, 8, 10]);
    }
}
