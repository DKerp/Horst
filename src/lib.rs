//! A fast key-value store with ACID guarentees written in pure Rust.

// #![feature(is_sorted)]

use std::io::SeekFrom;
use std::io::Cursor;
use std::io::{Error, ErrorKind};
use std::path::{PathBuf, Path};
use std::convert::TryInto;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Instant, Duration};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::pin::Pin;
// use std::task::{Poll, Context};
use std::future::Future;
use std::ops::Deref;

use tokio::fs;
// use tokio::io::{AsyncRead, AsyncSeek};
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use tokio::io::{BufWriter, BufReader, /*ReadBuf*/};
use tokio::sync::{oneshot, mpsc};
use tokio::task::spawn;

use memmap::Mmap;

use async_trait::async_trait;



/// Contains the parts responsible for detecting commit conflicts.
pub mod oracle;
use oracle::*;

/// Contains the parts responsible for maintaining key-value-pairs on disk.
pub mod vlog;
use vlog::*;

/// Contains the parts responsible for maintaining the keys index on disk using a LSM-tree.
pub mod lsm;
use lsm::*;

pub mod header;
use header::*;

// /// Contains the implementation of a userland implementation of a mmap file reader.
// pub mod mmap;
// use mmap::*;



/// Provides the current version of header definition.
pub trait Version {
    const VERSION: u16;
}

/// Provides the objects size on disk.
pub trait DiskSize {
    /// The static part of the objects size.
    const STATIC_SIZE: u64;

    /// The size of this object as saved on disk.
    fn disk_size(&self) -> u64 {
        Self::STATIC_SIZE
    }
}

#[async_trait]
pub trait SaveToDisk {
    async fn save_to_disk<W: AsyncWriteExt + Send + Sync + Unpin>(
        &self,
        disk: &mut W,
    ) -> std::io::Result<()>;
}

#[async_trait]
pub trait LoadFromDisk: Sized {
    async fn load_from_disk<R: AsyncReadExt + Send + Sync + Unpin>(
        disk: &mut R,
    ) -> std::io::Result<Self>;

    async fn load_from_disk_with_buffer<R: AsyncReadExt + Send + Sync + Unpin>(
        disk: &mut R,
        buffer: &mut Vec<u8>,
    ) -> std::io::Result<Self>;
}



#[derive(Debug, Default, Clone, Copy)]
pub struct BenchCommit {
    /// The time it took to write the key-value-pairs into the transaction object.
    pub txn_write: Duration,

    /// The time it took to receive the commit_ts from the oracle.
    pub oracle_commit: Duration,

    /// The total time it took to write the transaction to the value log.
    pub vlog_write_txn: Duration,
    /// The time the sole writing took (transactions get buffered before being written).
    pub vlog_write_txn_inner: Duration,

    pub lsm_add_total: Duration,

    pub lsm_level_0_add: Duration,
    pub lsm_level_0_merge: Option<Duration>,

    pub lsm_write: Option<Duration>,
    pub lsm_merge: Option<Duration>,
}

#[derive(Default, Clone, Copy)]
pub struct BenchCommitAverage {
    pub txn_write_avg: Duration,
    pub txn_write_max: Duration,

    pub oracle_commit_avg: Duration,
    pub oracle_commit_max: Duration,

    pub vlog_write_txn_avg: Duration,
    pub vlog_write_txn_max: Duration,
    // pub vlog_write_txn_inner_avg: Duration,
    // pub vlog_write_txn_inner_max: Duration,

    pub lsm_add_total_avg: Duration,
    pub lsm_add_total_max: Duration,

    pub lsm_level_0_add_avg: Duration,
    pub lsm_level_0_add_max: Duration,
    pub lsm_level_0_merge_avg: Duration,
    pub lsm_level_0_merge_max: Duration,
    pub lsm_level_0_merge_count: usize,

    pub lsm_write_avg: Duration,
    pub lsm_write_max: Duration,
    pub lsm_write_count: usize,
    pub lsm_merge_avg: Duration,
    pub lsm_merge_max: Duration,
    pub lsm_merge_count: usize,
}

impl std::fmt::Debug for BenchCommitAverage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BenchmarkCommitAverage (avg, max, [count])")
            .field("Writing to transaction:", &(self.txn_write_avg, self.txn_write_max))
            .field("Asking oracle to commit:", &(self.oracle_commit_avg, self.oracle_commit_max))
            .field("Writing to VLog:", &(self.vlog_write_txn_avg, self.vlog_write_txn_max))
            .field("Adding to LSM total:", &(self.lsm_add_total_avg, self.lsm_add_total_max))
            // .field("Adding to LSM level 0:", &(self.lsm_level_0_add_avg, self.lsm_level_0_add_max))
            // .field("Merging LSM level 0:", &(self.lsm_level_0_merge_avg, self.lsm_level_0_merge_max, self.lsm_level_0_merge_count))
            // .field("Writing to disk LSM:", &(self.lsm_write_avg, self.lsm_write_max, self.lsm_write_count))
            // .field("Merging LSM level:", &(self.lsm_merge_avg, self.lsm_merge_max, self.lsm_merge_count))
            .finish()
    }
}

impl BenchCommitAverage {
    pub fn from_slice<T>(slice: &T) -> Self
    where
        T: Deref<Target = Vec<BenchCommit>>,
    {
        let slice = Deref::deref(slice);

        let amount: u32 = slice.len().try_into().unwrap();

        let txn_write_avg = slice.iter().map(|b| b.txn_write).sum::<Duration>()/amount;
        let txn_write_max = slice.iter().map(|b| b.txn_write).max().unwrap();

        let oracle_commit_avg = slice.iter().map(|b| b.oracle_commit).sum::<Duration>()/amount;
        let oracle_commit_max = slice.iter().map(|b| b.oracle_commit).max().unwrap();

        let vlog_write_txn_avg = slice.iter().map(|b| b.vlog_write_txn).sum::<Duration>()/amount;
        let vlog_write_txn_max = slice.iter().map(|b| b.vlog_write_txn).max().unwrap();
        // let vlog_write_txn_inner_avg = slice.iter().map(|b| b.vlog_write_txn).sum::<Duration>()/amount;
        // let vlog_write_txn_inner_max = slice.iter().map(|b| b.vlog_write_txn).max().unwrap();

        let lsm_add_total_avg = slice.iter().map(|b| b.lsm_add_total).sum::<Duration>()/amount;
        let lsm_add_total_max = slice.iter().map(|b| b.lsm_add_total).max().unwrap();

        let lsm_level_0_add_avg = slice.iter().map(|b| b.lsm_level_0_add).sum::<Duration>()/amount;
        let lsm_level_0_add_max = slice.iter().map(|b| b.lsm_level_0_add).max().unwrap();
        let lsm_level_0_merge_avg = slice.iter().filter_map(|b| b.lsm_level_0_merge).sum::<Duration>()/amount;
        let lsm_level_0_merge_max = slice.iter().filter_map(|b| b.lsm_level_0_merge).max().unwrap_or(Duration::new(0, 0));
        let lsm_level_0_merge_count = slice.iter().filter_map(|b| b.lsm_level_0_merge).count();

        let lsm_write_avg = slice.iter().filter_map(|b| b.lsm_write).sum::<Duration>()/amount;
        let lsm_write_max = slice.iter().filter_map(|b| b.lsm_write).max().unwrap_or(Duration::new(0, 0));
        let lsm_write_count = slice.iter().filter_map(|b| b.lsm_write).count();
        let lsm_merge_avg = slice.iter().filter_map(|b| b.lsm_merge).sum::<Duration>()/amount;
        let lsm_merge_max = slice.iter().filter_map(|b| b.lsm_merge).max().unwrap_or(Duration::new(0, 0));
        let lsm_merge_count = slice.iter().filter_map(|b| b.lsm_merge).count();

        Self {
            txn_write_avg,
            txn_write_max,

            oracle_commit_avg,
            oracle_commit_max,

            vlog_write_txn_avg,
            vlog_write_txn_max,
            // vlog_write_txn_inner_avg,
            // vlog_write_txn_inner_max,

            lsm_add_total_avg,
            lsm_add_total_max,

            lsm_level_0_add_avg,
            lsm_level_0_add_max,
            lsm_level_0_merge_avg,
            lsm_level_0_merge_max,
            lsm_level_0_merge_count,

            lsm_write_avg,
            lsm_write_max,
            lsm_write_count,
            lsm_merge_avg,
            lsm_merge_max,
            lsm_merge_count,
        }
    }
}



/// The configuration for the database.
#[derive(Debug, Clone)]
pub struct Config {
    /// The path of the folder where the value log files are saved.
    ///
    /// __Default:__ "./vlog"
    pub vlog_folder: PathBuf,
    /// The path of the folder where the LSM tree files are saved.
    ///
    /// __Default:__ "./lsm"
    pub lsm_folder: PathBuf,

    // /// The interval at which the tree store of the oracle gets merged
    // /// into the slice store.
    // ///
    // /// __Default:__ 30 seconds
    // pub oracle_merge_interval: Duration,
    /// The maximum number of entries kept inside the oracle.
    ///
    /// __Default:__ 10,000,000
    pub oracle_store_max_size: usize,
    /// The thereshold size for commits, where commits that are bigger then
    /// this value are directly merged with the oracle's slice store.
    ///
    /// __Default:__ 50,000
    pub oracle_commit_thereshold: usize,
    /// The maximum number of entries kept inside the oracle buffer.
    ///
    /// __Default:__ 1,000,000
    pub oracle_buffer_max_size: usize,

    /// The number of slices per level within the LSM tree.
    ///
    /// __Default:__ 10
    pub lsm_slices_per_level: usize,
    /// The interval at which the tree store within level 0 of the LSM tree
    /// gets merged into the same level`s slice store.
    ///
    /// __Default:__ 30 seconds
    pub lsm_level_zero_merge_interval: Duration,
    /// The maximum number of entries kept inside level 0 of the lsm tree.
    ///
    /// __Default:__ 1,000,000
    pub lsm_level_zero_max_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        let (vlog_folder, lsm_folder) = match std::env::current_dir() {
            Ok(path) => (path.join("vlog"), path.join("lsm")),
            Err(err) => {
                log::warn!("The current working directory could not be accessed. err: {:?}", err);

                (PathBuf::from_str("./vlog").unwrap(), PathBuf::from_str("./lsm").unwrap())
            }
        };

        Self {
            vlog_folder,
            lsm_folder,
            // oracle_merge_interval: Duration::from_secs(30),
            oracle_store_max_size: 10_000_000,
            oracle_commit_thereshold: 50_000,
            oracle_buffer_max_size: 1_000_000,
            lsm_slices_per_level: 10,
            lsm_level_zero_merge_interval: Duration::from_secs(30),
            lsm_level_zero_max_size: 1_000_000,
        }
    }
}



/// The main object containing the connection to the database.
#[derive(Debug)]
pub struct DB {
    /// The internal parts of the database. Get shared with all transactions for quick access.
    inner: Arc<DBInner>,
}

impl Clone for DB {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[derive(Clone, Debug)]
struct DBInner {
    /// The LSM tree.
    lsm: LSMManagerHandle,
    /// The oracle checking commit conflicts.
    oracle: OracleHandle,
    /// The VLog files.
    vlog: VLogHandle,
}

impl DB {
    pub async fn open(config: &Config) -> std::io::Result<Self> {
        let vlog = VLog::new(config).await?;
        let vlog = vlog.run();

        // TODO On restart, inform the oracle of the latest commit etc...
        let oracle = Oracle::new(config);
        let oracle = oracle.run();

        let lsm = LSMManager::new(
            config,
            oracle.clone(),
        ).await?;
        let lsm = lsm.run();

        let inner = Arc::new(DBInner {
            lsm,
            oracle,
            vlog,
        });

        Ok(Self {
            inner,
        })
    }

    pub async fn new_txn(&self) -> Txn {
        let read_ts = self.inner.oracle.get_read_ts().await;
        let inner = Arc::clone(&self.inner);
        let cache = Some(TxnCache {
            keys_read: Vec::with_capacity(128),
            keys_set: BTreeMap::new(),
        });

        Txn {
            inner,
            read_ts,
            cache,
            finished: false,
        }
    }

    pub async fn new_read_only_txn(&self) -> Txn {
        let read_ts = self.inner.oracle.get_read_ts().await;
        let inner = Arc::clone(&self.inner);

        Txn {
            inner,
            read_ts,
            cache: None,
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

    /// The cache of entries read/written. Gets only added if the Txn is not read-only.
    cache: Option<TxnCache>,

    /// Indicates that this transactions got already finished and may no longer be used.
    finished: bool,
}

pub struct TxnCache {
    /// The keys which we have read so far, including those which were not present.
    keys_read: Vec<u128>,
    /// The keys which we have updated.
    keys_set: BTreeMap<u128, Vec<u8>>,
}

impl Txn {
    pub async fn find(&mut self, key: u128) -> std::io::Result<bool> {
        self.check_not_finished()?;

        if let Some(cache) = self.cache.as_mut() {
            cache.keys_read.push(key);

            // Return the cached key if there is one. (Atomicity)
            if let Some(_value) = cache.keys_set.get(&key) {
                return Ok(true);
            }
        }

        match self.inner.lsm.get(key, self.read_ts).await? {
            Some(_pair) => return Ok(true),
            None => return Ok(false),
        };
    }

    pub async fn get(&mut self, key: u128) -> std::io::Result<Option<Vec<u8>>> {
        self.check_not_finished()?;

        if let Some(cache) = self.cache.as_mut() {
            cache.keys_read.push(key);

            // Return the cached key if there is one. (Atomicity)
            if let Some(value) = cache.keys_set.get(&key) {
                return Ok(Some(value.clone()));
            }
        }

        let pair = match self.inner.lsm.get(key, self.read_ts).await? {
            Some(pair) => pair,
            None => return Ok(None),
        };

        let value = match self.inner.vlog.read_value(
            key,
            pair.vlog_id,
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

    /// TODO remove...
    pub async fn get_debug(&mut self, key: u128) -> std::io::Result<Option<Vec<u8>>> {
        self.check_not_finished()?;

        if let Some(cache) = self.cache.as_mut() {
            cache.keys_read.push(key);

            // Return the cached key if there is one. (Atomicity)
            if let Some(value) = cache.keys_set.get(&key) {
                return Ok(Some(value.clone()));
            }
        }

        let pair = match self.inner.lsm.get_debug(key, self.read_ts).await? {
            Some(pair) => pair,
            None => return Ok(None),
        };

        let value = match self.inner.vlog.read_value(
            key,
            pair.vlog_id,
            pair.vlog_offset,
        ).await {
            Ok(value) => value,
            Err(err) => {
                self.finished = true;

                log::warn!("KVPair could not be found inside the VLog file! pair: {:?}", pair);

                return Err(err);
            }
        };

        Ok(Some(value))
    }

    pub fn set(&mut self, key: u128, value: Vec<u8>) -> std::io::Result<()> {
        self.check_not_finished()?;

        let cache = self.cache.as_mut().ok_or(Error::new(ErrorKind::Other, "Txn is read only."))?;

        cache.keys_set.insert(key, value);

        Ok(())
    }

    pub async fn commit(&mut self) -> std::io::Result<BenchCommit> {
        self.check_not_finished()?;

        // Get the cache. Abort if this is a read-only transaction (=self.cache is None)
        let cache = self.cache.take().ok_or(Error::new(ErrorKind::Other, "Txn is read only."))?;

        // Mark this transaction as finished.
        self.finished = true;

        // Prepare the collection of read keys.
        let mut keys_read = cache.keys_read;
        keys_read.sort();
        keys_read.dedup(); // Make sure we have no duplicate values.
        let keys_read = keys_read; // Remove mutability.

        // Extract the colections of written keys.
        let mut keys_set: Vec<VLogEntry> = Vec::with_capacity(cache.keys_set.len());
        let mut keys_set_mini: Vec<u128> = Vec::with_capacity(cache.keys_set.len());
        for (key, value) in cache.keys_set {
            keys_set_mini.push(key);

            keys_set.push(VLogEntry {
                key,
                value,
            })
        }

        let mut bench = BenchCommit::default();

        log::info!("Commit - Asking the oracle...");
        let now = Instant::now();

        // Ask the oracle if we can commit.
        let commit_ts = match self.inner.oracle.commit(keys_read, keys_set_mini, self.read_ts).await {
            Some(commit_ts) => commit_ts,
            None => return Err(Error::new(ErrorKind::Other, "Conflict detected.")),
        };

        let elapsed = now.elapsed();
        bench.oracle_commit = elapsed;
        log::info!("... done. Took {:?}.", elapsed);


        log::info!("Commit - Writing to disk...");
        let now = Instant::now();

        // Create the transaction for the VLog.
        let txn = VLogTxn::new(
            commit_ts,
            keys_set,
        );

        // Write the new values to the value log.
        let pairs = match self.inner.vlog.write_txn(txn).await {
            Ok(pairs) => pairs,
            Err(err) => {
                // Inform the oracle of the failed commit.
                self.inner.oracle.abort_commit(commit_ts).await;

                return Err(err);
            }
        };

        let elapsed = now.elapsed();
        bench.vlog_write_txn = elapsed;
        log::info!("... done. Took {:?}.", elapsed);


        log::info!("Commit - Adding to the lsm...");
        let now = Instant::now();

        self.inner.lsm.insert(pairs).await.unwrap(); // TODO add error handling (requires LSM file error handling...)

        let elapsed = now.elapsed();
        bench.lsm_add_total = elapsed;
        log::info!("... done. Took {:?}.", elapsed);


        log::info!("Commit - Telling the oracle the commit has finished...");
        let now = Instant::now();

        // Inform the oracle of the finished commit.
        self.inner.oracle.finish_commit(commit_ts).await;

        let elapsed = now.elapsed();
        log::info!("... done. Took {:?}.", elapsed);

        Ok(bench)
    }

    fn check_not_finished(&self) -> std::io::Result<()> {
        if self.finished {
            return Err(Error::new(ErrorKind::Other, "Txn is already finished."));
        }

        Ok(())
    }
}



pub(crate) struct MergeHelper<I: Iterator<Item = IT> + Sized, IT: Clone + Sized> {
    iter: I,
    current: Option<IT>,
}

impl<I, IT> MergeHelper<I, IT>
where
    I: Iterator<Item = IT> + Sized,
    IT: Clone + Sized,
{
    pub fn new(
        mut iter: I,
    ) -> Self {
        let current = iter.next();

        Self {
            iter,
            current
        }
    }

    pub fn next(&mut self) {
        self.current = self.iter.next();
    }

    pub fn current(&self) -> Option<IT> {
        self.current.clone()
    }
}

pub(crate) fn merge_sorted<I1, I2, IT>(
    store: &mut Vec<IT>,
    iter1: I1,
    iter2: I2,
)
where
    I1: Iterator<Item = IT> + Sized,
    I2: Iterator<Item = IT> + Sized,
    IT: Ord + Clone + Sized,
{
    let mut iter1 = MergeHelper::new(iter1);
    let mut iter2 = MergeHelper::new(iter2);

    loop {
        let item1 = match iter1.current() {
            Some(item) => item,
            None => break,
        };

        let item2 = match iter2.current() {
            Some(item) => item,
            None => break,
        };

        match item1.cmp(&item2) {
            Ordering::Less => {
                store.push(item1);

                iter1.next();
            }
            Ordering::Greater => {
                store.push(item2);

                iter2.next();
            }
            Ordering::Equal => {
                store.push(item1);

                iter1.next();
                iter2.next();
            }
        }
    }

    // At least one of the two got exhausted. So this effectively adds only
    // the elements of one to the store.
    while let Some(item) = iter1.current() {
        store.push(item);
        iter1.next();
    }
    while let Some(item) = iter2.current() {
        store.push(item);
        iter2.next();
    }
}
