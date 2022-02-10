use crate::*;



#[derive(Debug)]
pub enum RamStoreReq {
    /// Insert a list of new key-value pairs into the store. Can not fail.
    Insert(Vec<KVPair>),
    /// Retrieve a cetain key from the store. Gives back the newest version before or at the given read timestamp.
    Get(SliceGetReq, mpsc::UnboundedSender<SliceResp>),
    /// Retrieve a cetain key from the store. Gives back the newest version before or at the given read timestamp.
    GetDebug(SliceGetReq, mpsc::UnboundedSender<SliceResp>),
}

#[derive(Debug)]
pub enum RamStoreResp {
    /// The response to an insert request, possibly containing a slice of key-value-pairs
    /// to be added to the LSM tree.
    Insert(Option<Vec<KVPair>>),
}

#[derive(Clone, Debug)]
pub struct RamStoreHandle {
    sender: mpsc::UnboundedSender<(RamStoreReq, oneshot::Sender<std::io::Result<RamStoreResp>>)>,
}

impl From<mpsc::UnboundedSender<(RamStoreReq, oneshot::Sender<std::io::Result<RamStoreResp>>)>> for RamStoreHandle {
    fn from(sender: mpsc::UnboundedSender<(RamStoreReq, oneshot::Sender<std::io::Result<RamStoreResp>>)>) -> Self {
        Self {
            sender,
        }
    }
}

impl RamStoreHandle {
    pub async fn insert(
        &self,
        pairs: Vec<KVPair>,
    ) -> std::io::Result<Option<Vec<KVPair>>> {
        let req = RamStoreReq::Insert(pairs);

        let (tx, rx) = oneshot::channel();

        self.sender.send((req, tx)).unwrap();

        let RamStoreResp::Insert(resp) = rx.await.unwrap()?;

        Ok(resp)
    }

    pub fn get(
        &self,
        req: SliceGetReq,
        tx: mpsc::UnboundedSender<SliceResp>,
    ) {
        let req = RamStoreReq::Get(req, tx);

        let (tx, _rx) = oneshot::channel();

        self.sender.send((req, tx)).unwrap();
    }

    pub fn get_debug(
        &self,
        req: SliceGetReq,
        tx: mpsc::UnboundedSender<SliceResp>,
    ) {
        let req = RamStoreReq::GetDebug(req, tx);

        let (tx, _rx) = oneshot::channel();

        self.sender.send((req, tx)).unwrap();
    }
}



pub struct RamStore {
    /// The different key/value pairs currently saved at level 0.
    /// Ordered by the keys.
    pub store: Vec<KVPair>,
    /// A buffer for new inserts into level 0. Gets eventually merged with the main store.
    pub store_buffer: BTreeMap<(u128, u128), KVPair>,
    /// The interval at which the tree store within level 0 of the LSM tree
    /// gets merged into the same level`s slice store.
    pub store_merge_interval: Duration,
    /// The maximum number of entries kept inside level 0 of the lsm tree.
    pub store_max_size: usize,

    /// A handle to the oracle. Gets used to retrieve the current read timestamp.
    pub oracle: OracleHandle,
}

impl RamStore {
    pub fn new(
        config: &Config,
        oracle: OracleHandle,
    ) -> Self {
        Self {
            store: Vec::with_capacity(2*config.lsm_level_zero_max_size),
            store_buffer: BTreeMap::new(),
            store_merge_interval: config.lsm_level_zero_merge_interval,
            store_max_size: config.lsm_level_zero_max_size,
            oracle,
        }
    }

    /// Run the RamStore in a seperate task and return a handle to it.
    pub fn run(self) -> RamStoreHandle {
        let (tx, rx) = mpsc::unbounded_channel();

        spawn(async move {
            self.run_inner(rx).await;
        });

        tx.into()
    }

    /// The main loop of the RamStore background task.
    async fn run_inner(mut self, mut rx: mpsc::UnboundedReceiver<(RamStoreReq, oneshot::Sender<std::io::Result<RamStoreResp>>)>) {
        let mut interval = tokio::time::interval(self.store_merge_interval);

        loop {
            tokio::select! {
                req = rx.recv() => {
                    let (req, tx) = match req {
                        Some(req) => req,
                        None => break,
                    };

                    match req {
                        RamStoreReq::Insert(pairs) => {
                            let resp = self.insert_multi(pairs).await
                                .map(|resp| RamStoreResp::Insert(resp));

                            tx.send(resp).unwrap();
                        }
                        RamStoreReq::Get(mut get_req, tx) => {
                            get_req.last_level = 1; // If given back it should search in level 1.
                            get_req.last_min_ts = u128::MAX; // Guarentees the manager takes the highest slice.

                            match self.get(get_req.key, get_req.read_ts) {
                                Some(pair) => {
                                    let resp = LSMManagerResp::Get(Some(pair));

                                    get_req.tx.send(Ok(resp)).unwrap();
                                }
                                None => {
                                    let resp = SliceResp::Get(get_req);

                                    tx.send(resp).unwrap();
                                }
                            }
                        }
                        RamStoreReq::GetDebug(mut get_req, tx) => {
                            get_req.last_level = 1; // If given back it should search in level 1.
                            get_req.last_min_ts = u128::MAX; // Guarentees the manager takes the highest slice.
                            get_req.debug = true;

                            match self.get_debug(get_req.key, get_req.read_ts) {
                                Some(pair) => {
                                    let resp = LSMManagerResp::GetDebug(Some(pair));

                                    get_req.tx.send(Ok(resp)).unwrap();
                                }
                                None => {
                                    let resp = SliceResp::Get(get_req);

                                    tx.send(resp).unwrap();
                                }
                            }
                        }
                    }
                }

                _ = interval.tick() => {
                    // log::info!("VLog - next tick. txn_buffer.len(): {}", txn_buffer.len());

                    if self.store_buffer.is_empty() {
                        continue;
                    }

                    log::warn!("RamStore - merging store with temp store - temp len: {}", self.store_buffer.len());

                    self.merge();
                }
            }
        }
    }

    pub async fn insert_multi(&mut self, mut pairs: Vec<KVPair>) -> std::io::Result<Option<Vec<KVPair>>> {

        // NOTE We expect the pairs to be presorted.
        // NOTE The version MUST also be different from any other yet inserted version.

        for pair in pairs.drain(..) {
            let key = (pair.key, pair.version);
            self.store_buffer.insert(key, pair);
        }

        // Check if the maximum size got exceeded.
        // If so, merge the tree with the slice store and return a slice containing all values
        // belonging to commit timestamps which have no earlier commits pending.
        if (self.store.len() + self.store_buffer.len()) > self.store_max_size {
            self.merge();

            let read_ts = self.oracle.get_read_ts().await;

            let old = self.split_old(read_ts);

            return Ok(Some(old));
        }

        Ok(None)
    }

    pub fn get(&self, key: u128, read_ts: u128) -> Option<KVPair> {
        log::debug!("RamStore.get triggered. key: {}", key);

        let find1 = self.find_latest_version(key, read_ts);
        let find2 = self.find_latest_version_buffer(key, read_ts);

        if let Some(pair1) = &find1 {
            if let Some(pair2) = &find2 {
                match pair1.cmp(&pair2) {
                    Ordering::Less => return find2,
                    _ => return find1,
                }
            }
        }

        find2
    }

    pub fn get_debug(&self, key: u128, read_ts: u128) -> Option<KVPair> {
        log::debug!("RamStore.get triggered. key: {}", key);

        let find1 = self.find_latest_version(key, read_ts);
        let find2 = self.find_latest_version_buffer(key, read_ts);

        log::warn!("DEBUG - RamStore.get_debug - find1: {:?}, find2: {:?}", find1, find2);

        if let Some(pair1) = &find1 {
            if let Some(pair2) = &find2 {
                match pair1.cmp(&pair2) {
                    Ordering::Less => return find2,
                    _ => return find1,
                }
            }
        }

        find2
    }

    pub fn merge(&mut self) {

        let merged_store_capacity = (self.store.len()+self.store_buffer.len()).max(2*self.store_max_size);
        let merged_store = Vec::with_capacity(merged_store_capacity);

        let store = std::mem::replace(&mut self.store, merged_store);
        let buffer = std::mem::replace(&mut self.store_buffer, BTreeMap::new());

        let store_iter = store.into_iter();
        let buffer_iter = buffer.into_values();

        merge_sorted(
            &mut self.store,
            store_iter,
            buffer_iter,
        );
    }

    /// Splits off all records from the store which have a commit timestamp of at most read_ts.
    /// All newer records maintain in the store. Note that both slices will remain sorted.
    pub fn split_old(&mut self, read_ts: u128) -> Vec<KVPair> {
        let mut new = Vec::with_capacity(self.store.len()/10); // We expect at most ca. 10% to be newer then read_ts.
        let mut old = Vec::with_capacity(self.store.len()); // We might end up removing all pairs.

        for pair in self.store.drain(..) {
            if pair.version>read_ts {
                new.push(pair);
            } else {
                old.push(pair);
            }
        }

        self.store = new;

        old
    }

    fn find_latest_version(&self, key: u128, read_ts: u128) -> Option<KVPair> {
        let compare = KVPair::from_key_and_version(key, read_ts);

        match self.store.binary_search(&compare) {
            Ok(idx) => {
                let pair = self.store.get(idx).unwrap();
                return Some(pair.clone());
            },
            Err(idx) => {
                if idx>0 {
                    let idx = idx-1;

                    let pair = self.store.get(idx).unwrap();
                    if pair.key==key {
                        return Some(pair.clone());
                    }
                }

                return None;
            }
        }
    }

    fn find_latest_version_buffer(&self, key: u128, read_ts: u128) -> Option<KVPair> {
        let begin = (key, 0);
        let end = (key, read_ts+1);

        self.store_buffer.range(begin..end).next_back().map(|(_, value)| value.clone())
    }
}
