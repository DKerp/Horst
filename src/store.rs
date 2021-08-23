use crate::*;



#[derive(Debug)]
pub enum KVStoreReq {
    /// Insert a list of new key-value pairs into the store. Can not fail.
    Insert(Vec<KVPair>),
    /// Retrieve a cetain key from the store. Gives back the newest version before or at the given read timestamp.
    Get(u128, u128),
}

#[derive(Debug)]
pub enum KVStoreResp {
    /// The empty response to an insert request.
    Insert,
    /// A get response, containing the searched for key-value pair if it exists, or none if the key did not exist.
    Get(Option<KVPair>),
}

pub struct KVStoreHandle {
    sender: mpsc::UnboundedSender<(KVStoreReq, oneshot::Sender<std::io::Result<KVStoreResp>>)>,
}

impl From<mpsc::UnboundedSender<(KVStoreReq, oneshot::Sender<std::io::Result<KVStoreResp>>)>> for KVStoreHandle {
    fn from(sender: mpsc::UnboundedSender<(KVStoreReq, oneshot::Sender<std::io::Result<KVStoreResp>>)>) -> Self {
        Self {
            sender,
        }
    }
}

impl KVStoreHandle {
    pub async fn insert(
        &self,
        pairs: Vec<KVPair>,
    ) -> std::io::Result<()> {
        let req = KVStoreReq::Insert(pairs);

        let (tx, rx) = oneshot::channel();

        self.sender.send((req, tx)).unwrap();

        if let KVStoreResp::Insert = rx.await.unwrap()? {
            return Ok(())
        }

        panic!("Implementation error!");
    }

    pub async fn get(
        &self,
        key: u128,
        version: u128,
    ) -> std::io::Result<Option<KVPair>> {
        let req = KVStoreReq::Get(key, version);

        let (tx, rx) = oneshot::channel();

        self.sender.send((req, tx)).unwrap();

        if let KVStoreResp::Get(resp) = rx.await.unwrap()? {
            return Ok(resp)
        }

        panic!("Implementation error!");
    }
}



pub struct KVStore {
    /// The different key/value pairs currently saved.
    /// Ordered by the keys.
    pub store: Vec<KVPair>,
    /// A buffer for new inserts. Gets eventually merged with the main store.
    pub store_buffer: BTreeMap<(u128, u128), KVPair>,
}

impl KVStore {
    pub fn new() -> Self {
        Self {
            store: Vec::with_capacity(0),
            store_buffer: BTreeMap::new(),
        }
    }

    /// Run the KVStore in a seperate task and return a handle to it.
    pub fn run(self) -> KVStoreHandle {
        let (tx, rx) = mpsc::unbounded_channel();

        spawn(async move {
            self.run_inner(rx).await;
        });

        tx.into()
    }

    /// The main loop of the KVStore background task.
    async fn run_inner(mut self, mut rx: mpsc::UnboundedReceiver<(KVStoreReq, oneshot::Sender<std::io::Result<KVStoreResp>>)>) {
        let mut interval = tokio::time::interval(Duration::from_millis(30_000)); // Tick 1 times per second.

        loop {
            tokio::select! {
                req = rx.recv() => {
                    let (req, tx) = match req {
                        Some(req) => req,
                        None => break,
                    };

                    let resp = match req {
                        KVStoreReq::Insert(pairs) => {
                            self.insert_multi(pairs);

                            Ok(KVStoreResp::Insert)
                        }
                        KVStoreReq::Get(key, version) => {
                            let resp = self.get(key, version);

                            Ok(KVStoreResp::Get(resp))
                        }
                    };

                    tx.send(resp).unwrap();
                }

                _ = interval.tick() => {
                    // log::info!("VLog - next tick. txn_buffer.len(): {}", txn_buffer.len());

                    if self.store_buffer.is_empty() {
                        continue;
                    }

                    log::warn!("KVStore - merging store with temp store - temp len: {}", self.store_buffer.len());

                    self.merge();
                }
            }
        }
    }

    pub fn insert_multi(&mut self, mut pairs: Vec<KVPair>) {

        // NOTE We expect the pairs to be presorted.
        // NOTE The version MUST also be different from any other yet inserted version.

        for pair in pairs.drain(..) {
            let key = (pair.key, pair.version);
            self.store_buffer.insert(key, pair);
        }
    }

    pub fn get(&self, key: u128, read_ts: u128) -> Option<KVPair> {
        log::debug!("KVStore.get triggered. key: {}", key);

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

    pub fn merge(&mut self) {
        // Create a store for the new pairs.
        let mut pairs = Vec::with_capacity(self.store_buffer.len());
        // Create a store for the final indexes of the new pairs.
        let mut pairs_indexes = Vec::with_capacity(self.store_buffer.len());
        // We count how many new pairs have been added (virtually) to the merged slice.
        let mut counter = 0usize;


        for pair in self.store_buffer.values() {
            match self.store.binary_search(pair) {
                Ok(idx) => panic!("Found a suppostly new KVPair in the store! idx: {}, pair: {:?}", idx, pair),
                Err(idx) => {
                    // We have do add the number of new pairs which have (virtually) been added so far
                    // in order to get the correct index in the final merged slice.
                    pairs.push(pair.clone());
                    pairs_indexes.push(idx+counter);
                    counter = counter+1;
                }
            }
        }

        self.store = merge_sorted_with_indexes(&mut self.store, pairs, pairs_indexes);
        self.store_buffer.clear();
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



#[derive(Debug, Eq, Clone, Default)]
pub struct KVPair {
    /// The key.
    pub key: u128,

    /// The commit timestamp of this version.
    pub version: u128,

    /// The id of the value log file where the value is saved.
    pub vlog_id: u128,
    /// The offset within the value log file where the value is saved.
    pub vlog_offset: u64,

    // /// Optionally, the concrete value.
    // pub value: Option<Vec<u8>>,
}

impl PartialEq for KVPair {
    fn eq(&self, other: &Self) -> bool {
        self.key==other.key && self.version==other.version
    }
}

impl PartialOrd for KVPair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KVPair {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => return self.version.cmp(&other.version),
            ord => return ord,
        }
    }
}

impl KVPair {
    pub const BYTE_SIZE: u64 = 56;

    pub(crate) fn from_key_and_version(key: u128, version: u128) -> Self {
        Self {
            key,
            version,
            vlog_id: 0,
            vlog_offset: 0,
            // value: None,
        }
    }

    pub(crate) async fn write<W: AsyncWriteExt + Send + Sync + Unpin>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        writer.write_all(&self.key.to_be_bytes()).await?;
        writer.write_all(&self.version.to_be_bytes()).await?;
        writer.write_all(&self.vlog_id.to_be_bytes()).await?;
        writer.write_all(&self.vlog_offset.to_be_bytes()).await
    }

    pub(crate) async fn read_nth<R: AsyncReadExt + AsyncSeekExt + Send + Sync + Unpin>(
        reader: &mut R,
        nth: u64,
    ) -> std::io::Result<KVPair> {
        // Seek to the position of the entry.
        reader.seek(SeekFrom::Start(Slice::HEADER_SIZE + nth*Self::BYTE_SIZE)).await?;

        // Create a buffer for reading the data.
        let mut store = vec![0u8; Self::BYTE_SIZE as usize];

        reader.read_exact(&mut store[..]).await?;

        // Parse the pair.
        let key: [u8; 16] = store[..16].try_into().unwrap();
        let key = u128::from_be_bytes(key);
        let version: [u8; 16] = store[16..32].try_into().unwrap();
        let version = u128::from_be_bytes(version);
        let vlog_id: [u8; 16] = store[32..48].try_into().unwrap();
        let vlog_id = u128::from_be_bytes(vlog_id);
        let vlog_offset: [u8; 8] = store[48..].try_into().unwrap();
        let vlog_offset = u64::from_be_bytes(vlog_offset);

        Ok(Self {
            key,
            version,
            vlog_id,
            vlog_offset,
        })
    }
}
