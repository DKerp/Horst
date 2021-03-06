use crate::*;



#[derive(Debug)]
pub enum OracleReq {
    /// Get the latest secure read timestamp. (= the highest finished commit ts)
    GetReadTs,
    /// Try to commit a transaction which has read a list of keys with the given read timestamp.
    Commit(Vec<u128>, Vec<u128>, u128),
    /// Signal the aborting of a commit.
    AbortCommit(u128),
    /// Signal the successfull finishing of a commit.
    FinishCommit(u128),
}

#[derive(Debug)]
pub enum OracleResp {
    GetReadTs(u128),
    Commit(Option<u128>),
    AbortCommit,
    FinishCommit,
}

#[derive(Clone, Debug)]
pub struct OracleHandle {
    sender: mpsc::UnboundedSender<(OracleReq, oneshot::Sender<OracleResp>)>,
}

impl From<mpsc::UnboundedSender<(OracleReq, oneshot::Sender<OracleResp>)>> for OracleHandle {
    fn from(sender: mpsc::UnboundedSender<(OracleReq, oneshot::Sender<OracleResp>)>) -> Self {
        Self {
            sender,
        }
    }
}

impl OracleHandle {
    pub async fn get_read_ts(&self) -> u128 {
        let req = OracleReq::GetReadTs;

        let (tx, rx) = oneshot::channel();

        self.sender.send((req, tx)).unwrap();

        if let OracleResp::GetReadTs(resp) = rx.await.unwrap() {
            return resp
        }

        panic!("Implementation error!");
    }

    pub async fn commit(
        &self,
        keys_read: Vec<u128>,
        keys_set: Vec<u128>,
        read_ts: u128
    ) -> Option<u128> {
        let req = OracleReq::Commit(keys_read, keys_set, read_ts);

        let (tx, rx) = oneshot::channel();

        self.sender.send((req, tx)).unwrap();

        if let OracleResp::Commit(resp) = rx.await.unwrap() {
            return resp
        }

        panic!("Implementation error!");
    }

    pub async fn abort_commit(&self, commit_ts: u128) {
        let req = OracleReq::AbortCommit(commit_ts);

        let (tx, rx) = oneshot::channel();

        self.sender.send((req, tx)).unwrap();

        if let OracleResp::AbortCommit = rx.await.unwrap() {
            return
        }

        panic!("Implementation error!");
    }

    pub async fn finish_commit(&self, commit_ts: u128) {
        let req = OracleReq::FinishCommit(commit_ts);

        let (tx, rx) = oneshot::channel();

        self.sender.send((req, tx)).unwrap();

        if let OracleResp::FinishCommit = rx.await.unwrap() {
            return
        }

        panic!("Implementation error!");
    }
}


pub struct Oracle {
    /// The store taking note of the latest commit stamps for all keys.
    pub commit_store: Vec<(u128, u128)>,
    /// The configured maximum length of the store.
    pub commit_store_max_size: usize,
    /// A small buffer for incomming commits, which are regularely merged with the main store.
    pub commit_buffer: BTreeMap<u128, u128>,
    /// The thereshold size for commits, where commits that are bigger then this value are directly
    /// merged with the main store.
    pub commit_buffer_thereshold: usize,
    /// The maximum size of the commit buffer.
    pub commit_buffer_max_size: usize,
    // /// The interval at which the buffer gets merged into the main store.
    // pub commit_buffer_merge_interval: Duration,

    /// The latest commit timestamp.
    pub commit_ts: u128,
    /// The latest read timestamp.
    pub read_ts: u128,
    /// The minimal timestamp for which the oracle keeps track of the latest commit.
    /// Intended for allowing to keep the oracle store small.
    /// Transactions which started with a read_ts below this value get aborted because
    /// of a conflict for security reasons.
    pub minimal_ts: u128,

    /// A list of pending commits.
    pub pending_commits: Vec<(u128, bool)>,
}

impl Oracle {
    pub fn new(
        config: &Config,
    ) -> Self {
        Self {
            commit_store: Vec::with_capacity(2*config.oracle_store_max_size),
            commit_store_max_size: config.oracle_store_max_size,
            commit_buffer: BTreeMap::new(),
            commit_buffer_thereshold: config.oracle_commit_thereshold,
            commit_buffer_max_size: config.oracle_buffer_max_size,
            // commit_buffer_merge_interval: config.oracle_merge_interval,
            commit_ts: 0,
            read_ts: 0,
            minimal_ts: 0,
            pending_commits: Vec::with_capacity(100),
        }
    }


    /// Run the Oracle in a seperate task and return a handle to it.
    pub fn run(self) -> OracleHandle {
        let (tx, rx) = mpsc::unbounded_channel();

        spawn(async move {
            self.run_inner(rx).await;
        });

        tx.into()
    }

    /// The main loop of the oracle background task.
    async fn run_inner(mut self, mut rx: mpsc::UnboundedReceiver<(OracleReq, oneshot::Sender<OracleResp>)>) {
        // let mut interval = tokio::time::interval(Duration::from_millis(30_000)); // Tick 0.2 times per second.

        loop {
            tokio::select! {
                req = rx.recv() => {
                    let (req, tx) = match req {
                        Some(req) => req,
                        None => break,
                    };

                    let resp = match req {
                        OracleReq::GetReadTs => {
                            OracleResp::GetReadTs(self.read_ts)
                        }
                        OracleReq::Commit(keys_read, keys_set, read_ts) => {
                            let resp = if self.would_conflict(&keys_read, &keys_set, read_ts) {
                                None
                            } else {
                                let commit_ts = self.commit(keys_set);
                                self.add_commit(commit_ts);

                                Some(commit_ts)
                            };

                            OracleResp::Commit(resp)
                        }
                        OracleReq::AbortCommit(commit_ts) => {
                            self.abort_commit(commit_ts);

                            OracleResp::AbortCommit
                        }
                        OracleReq::FinishCommit(commit_ts) => {
                            self.finish_commit(commit_ts);

                            OracleResp::FinishCommit
                        }
                    };

                    tx.send(resp).unwrap();

                    if self.commit_buffer.len()>self.commit_buffer_max_size {
                        log::warn!("Oracle - merging store with temp store - temp len: {}", self.commit_buffer.len());

                        let start = Instant::now();
                        self.merge();
                        let elapsed = start.elapsed();

                        log::warn!("Oracle - merging store with temp store took {:?}", elapsed);
                    }

                    if (self.commit_store.len()+self.commit_buffer.len())>self.commit_store_max_size {
                        self.delete_old_records();
                    }
                }

                // _ = interval.tick() => {
                //     // log::info!("VLog - next tick. txn_buffer.len(): {}", txn_buffer.len());
                //
                //     if self.commit_buffer.is_empty() {
                //         continue;
                //     }
                //
                //     log::warn!("Oracle - merging store with temp store - temp len: {}", self.commit_buffer.len());
                //
                //     let start = Instant::now();
                //     self.merge();
                //     let elapsed = start.elapsed();
                //
                //     log::warn!("Oracle - merging store with temp store took {:?}", elapsed);
                //
                //     if (self.commit_store.len()+self.commit_buffer.len())>self.commit_store_max_length {
                //         self.delete_old_records();
                //     }
                // }
            }
        }
    }


    fn add_commit(&mut self, commit_ts: u128) {
        // Find the place where the new pending commit should be added to.
        let idx = self.find_commit(commit_ts).unwrap_err();

        self.pending_commits.insert(idx, (commit_ts, false));
    }

    fn abort_commit(&mut self, commit_ts: u128) {
        // Find the index of the pending commit.
        let idx = self.find_commit(commit_ts).unwrap();

        self.pending_commits.remove(idx);
    }

    fn finish_commit(&mut self, commit_ts: u128) {
        // Find the index of the pending commit.
        let idx = self.find_commit(commit_ts).unwrap();

        // If it is the first pending commit, we can remove it and all following ones which have
        // been finished. Otherwise we mark it as finished and wait for the earlier ones to finish first.
        if idx>0 {
            self.pending_commits[idx].1 = true;
        } else {
            self.pending_commits.remove(0);

            let mut max = commit_ts;

            loop {
                if let Some(&(ts, finished)) = self.pending_commits.get(0) {
                    if finished {
                        max = ts;
                        self.pending_commits.remove(0);
                        continue;
                    }
                }

                break
            }

            self.read_ts = max;
        }
    }

    pub fn would_conflict(
        &self,
        keys_read: &Vec<u128>,
        keys_set: &Vec<u128>,
        read_ts: u128,
    ) -> bool {
        // We do not store commit records before the minimal value, we can therefor not ensure
        // that there are no conflicts. Abort for security reasons.
        if self.minimal_ts>read_ts {
            return true;
        }

        for &key in keys_read.iter() {
            // If the key is unknown, there can not be a conflict.
            if let Ok(idx) = self.find_key(key) {
                let &(_, latest_version) = self.commit_store.get(idx).unwrap();
                if latest_version>read_ts {
                    return true;
                }
            }

            // Search the buffer.
            if let Some(&commit_ts) = self.commit_buffer.get(&key) {
                if commit_ts>read_ts {
                    return true;
                }
            }
        }

        for &key in keys_set.iter() {
            // If the key is unknown, there can not be a conflict.
            if let Ok(idx) = self.find_key(key) {
                let &(_, latest_version) = self.commit_store.get(idx).unwrap();
                if latest_version>read_ts {
                    return true;
                }
            }

            // Search the buffer.
            if let Some(&commit_ts) = self.commit_buffer.get(&key) {
                if commit_ts>read_ts {
                    return true;
                }
            }
        }

        false
    }

    pub fn commit(
        &mut self,
        keys: Vec<u128>,
    ) -> u128 {
        // Create the commit timestamp.
        self.commit_ts = self.commit_ts+1;
        let commit_ts = self.commit_ts;

        log::info!("Oracle.commit - commit_ts: {}", commit_ts);

        if self.commit_buffer_thereshold>keys.len() {
            // Add the commit to the buffer.

            for key in keys {
                self.commit_buffer.insert(key, commit_ts);
            }
        } else {
            // Add the commit direclty.

            let merged_store_capacity = (self.commit_store.len()+keys.len()).max(2*self.commit_store_max_size);
            let merged_store = Vec::with_capacity(merged_store_capacity);

            let commit_store = std::mem::replace(&mut self.commit_store, merged_store);

            let store_iter = commit_store.into_iter();
            let keys_iter = keys.into_iter().zip([commit_ts].into_iter().cycle());

            merge_sorted(
                &mut self.commit_store,
                store_iter,
                keys_iter,
            );
        }

        commit_ts
    }

    pub fn merge(&mut self) {

        let merged_store_capacity = (self.commit_store.len()+self.commit_buffer.len()).max(2*self.commit_store_max_size);
        let merged_store = Vec::with_capacity(merged_store_capacity);

        let commit_store = std::mem::replace(&mut self.commit_store, merged_store);
        let commit_buffer = std::mem::replace(&mut self.commit_buffer, BTreeMap::new());

        let store_iter = commit_store.into_iter();
        let buffer_iter = commit_buffer.into_iter();

        merge_sorted(
            &mut self.commit_store,
            store_iter,
            buffer_iter,
        );
    }

    pub fn delete_old_records(&mut self) {
        log::warn!("Oracle.delete_old_records triggered. self.read_ts: {}, self.minimal_ts: {}", self.read_ts, self.minimal_ts);

        let middle = (self.read_ts+self.minimal_ts)/2;
        if middle==0 {
            log::warn!("Oracle.delete_old_records: middle==0 - Will not remove any records!");
            return
        }

        let current = self.commit_store.len();

        let start = Instant::now();

        self.commit_store.retain(|&(_, commit_ts)| commit_ts>=middle);
        self.commit_buffer.retain(|_, &mut commit_ts| commit_ts>=middle);

        let elapsed = start.elapsed();

        self.minimal_ts = middle;

        let dif = current - self.commit_store.len();

        log::warn!("Oracle.delete_old_records: Removed {} entries. Took {:?}", dif, elapsed);
    }

    fn find_key(&self, key: u128) -> Result<usize, usize> {
        self.commit_store.binary_search_by(|&(hash, _)| hash.cmp(&key))
    }

    fn find_commit(&self, commit_ts: u128) -> Result<usize, usize> {
        self.pending_commits.binary_search_by(|&(ts, _)| ts.cmp(&commit_ts))
    }
}
