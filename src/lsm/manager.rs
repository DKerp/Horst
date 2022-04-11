use crate::*;



#[derive(Debug)]
pub enum LSMManagerReq {
    /// Insert a list of new key-value pairs into the lsm tree.
    Insert(Vec<KVPair>),
    /// Retrieve a cetain key from the lsm tree. Gives back the newest version before or at the given read timestamp.
    Get(u128, u128),
    /// Retrieve a cetain key from the lsm tree. Gives back the newest version before or at the given read timestamp.
    GetDebug(u128, u128),
}

#[derive(Debug)]
pub enum LSMManagerResp {
    /// The empty response to an insert request.
    Insert,
    /// The response to a get request, containing the searched for [`KVPair`] if it could be found.
    Get(Option<KVPair>),
    /// The response to a get request, containing the searched for [`KVPair`] if it could be found.
    GetDebug(Option<KVPair>),
}

#[derive(Clone, Debug)]
pub struct LSMManagerHandle {
    sender: mpsc::UnboundedSender<(LSMManagerReq, oneshot::Sender<std::io::Result<LSMManagerResp>>)>,
}

impl From<mpsc::UnboundedSender<(LSMManagerReq, oneshot::Sender<std::io::Result<LSMManagerResp>>)>> for LSMManagerHandle {
    fn from(sender: mpsc::UnboundedSender<(LSMManagerReq, oneshot::Sender<std::io::Result<LSMManagerResp>>)>) -> Self {
        Self {
            sender,
        }
    }
}

impl LSMManagerHandle {
    pub async fn insert(
        &self,
        pairs: Vec<KVPair>,
    ) -> std::io::Result<()> {
        let req = LSMManagerReq::Insert(pairs);

        let (tx, rx) = oneshot::channel();

        self.sender.send((req, tx)).unwrap();

        if let LSMManagerResp::Insert = rx.await.unwrap()? {
            return Ok(())
        }

        panic!("Implementation error!");
    }

    pub async fn get(
        &self,
        key: u128,
        version: u128,
    ) -> std::io::Result<Option<KVPair>> {
        let req = LSMManagerReq::Get(key, version);

        let (tx, rx) = oneshot::channel();

        self.sender.send((req, tx)).unwrap();

        if let LSMManagerResp::Get(resp) = rx.await.unwrap()? {
            return Ok(resp)
        }

        panic!("Implementation error!");
    }

    pub async fn get_debug(
        &self,
        key: u128,
        version: u128,
    ) -> std::io::Result<Option<KVPair>> {
        let req = LSMManagerReq::GetDebug(key, version);

        let (tx, rx) = oneshot::channel();

        self.sender.send((req, tx)).unwrap();

        if let LSMManagerResp::GetDebug(resp) = rx.await.unwrap()? {
            return Ok(resp)
        }

        panic!("Implementation error!");
    }
}


/// The manager of the LSM tree. Maintains a list of all [`Slice`]s of the LSM tree.
#[derive(Debug)]
pub struct LSMManager {
    /// The path where the different slices are saved at.
    pub folder: PathBuf,

    /// The RAM store for level 0 pairs.
    pub ram_store: RamStoreHandle,

    /// The biggest id in use by a [`Slice`]. Gets incremented with each newly created [`Slice`].
    pub latest_id: u128,
    /// The number of slices at each level.
    pub slices_per_level: usize,
    /// The store for all levels above 0 and the [`Slice`]s it contains.
    pub slices: BTreeMap<u64, Vec<SliceHandle>>, // level, slices
}

impl LSMManager {
    pub async fn new(
        config: &Config,
        oracle: OracleHandle,
    ) -> std::io::Result<Self> {
        let folder = config.lsm_folder.clone();
        let slices_per_level = config.lsm_slices_per_level;

        // Check if the folder exists. If not, try to create it.
        if !folder.exists() {
            fs::create_dir_all(&folder).await?;
        }

        let meta = fs::metadata(&folder).await?;

        if !meta.is_dir() {
            return Err(Error::new(ErrorKind::InvalidInput, "The given folder path is not a folder!"));
        }

        if meta.permissions().readonly() {
            return Err(Error::new(ErrorKind::Other, "The folder can not be written!"));
        }

        let ram_store = RamStore::new(
            config,
            oracle,
        );
        let ram_store = ram_store.run();

        // TODO read and set the file permissions as appropriate.

        let mut latest_id = 0u128;
        let mut slices: BTreeMap<u64, Vec<SliceHandle>> = BTreeMap::new();

        // Open all available Slice`s.
        // NOTE since this an initialization method we use the sync version.
        for entry in std::fs::read_dir(&folder)? {
            let entry = entry?;

            let path = entry.path();

            // Skip sub folders.
            if path.is_dir() {
                log::warn!("Found an unexpected subfolder {:?} in the lsm directory {:?}. Skipping.", path, folder);
                continue;
            }

            // All folder entries will have a proper extension, since they can not end in `..`.
            if let Some(ext) = path.extension() {
                if ext!="slice" {
                    log::warn!("Found a file in the lsm folder {:?} which was not a slice: {:?}. Skipping. ext: {:?}", folder, path, ext);
                    continue;
                }
            }

            let mut slice = Slice::open(&path).await?;
            latest_id = latest_id.max(slice.id);

            // Make sure the slice is not corrupted.
            if !slice.is_fully_written().await? {
                log::warn!("Found a corrupted slice. Skipping. slice: {:?}", slice);
                continue;
            }

            match slices.get_mut(&slice.level) {
                Some(level) => {
                    level.push(slice.run());
                }
                None => {
                    let mut level = Vec::with_capacity(slices_per_level);
                    let slice_level = slice.level;
                    level.push(slice.run());

                    slices.insert(slice_level, level);
                }
            }

            for (_, level) in slices.iter_mut() {
                level.sort();
            }
        }

        let folder = folder.to_path_buf();

        Ok(Self {
            folder,
            ram_store,
            latest_id,
            slices_per_level,
            slices,
        })
    }

    /// Run the [`LSMManager`] in a seperate task and return a handle to it.
    pub fn run(self) -> LSMManagerHandle {
        let (tx, rx) = mpsc::unbounded_channel();

        spawn(async move {
            self.run_inner(rx).await;
        });

        tx.into()
    }

    /// The main loop of the [`LSMManager`] background task.
    async fn run_inner(
        mut self,
        mut rx: mpsc::UnboundedReceiver<(LSMManagerReq, oneshot::Sender<std::io::Result<LSMManagerResp>>)>,
    ) {
        let (slice_tx, mut slice_rx) = mpsc::unbounded_channel();
        let (merged_slice_tx, mut merged_slice_rx) = mpsc::unbounded_channel();

        loop {
            tokio::select! {
                req = rx.recv() => {
                    let (req, tx) = match req {
                        Some(req) => req,
                        None => break,
                    };

                    match req {
                        LSMManagerReq::Insert(pairs) => {
                            let resp = match self.ram_store.insert(pairs).await {
                                Ok(resp) => resp,
                                Err(err) => {
                                    tx.send(Err(err)).unwrap();
                                    continue;
                                }
                            };

                            let resp = match resp {
                                Some(pairs) => {
                                    self.add_new_slice(pairs, &merged_slice_tx).await
                                        .map(|_| LSMManagerResp::Insert)
                                }
                                None => Ok(LSMManagerResp::Insert),
                            };

                            tx.send(resp).unwrap();
                        }
                        LSMManagerReq::Get(key, read_ts) => {
                            let get_req = SliceGetReq::new(key, read_ts, tx);

                            self.ram_store.get(get_req, slice_tx.clone());
                        }
                        LSMManagerReq::GetDebug(key, read_ts) => {
                            let get_req = SliceGetReq::new(key, read_ts, tx);

                            self.ram_store.get_debug(get_req, slice_tx.clone());
                        }
                    }
                }

                resp = slice_rx.recv() => {
                    let resp = resp.unwrap(); // We keep a sender half here, so this is never None.

                    match resp {
                        SliceResp::Get(get_req) => {
                            self.get(get_req, slice_tx.clone());
                        }
                        // SliceResp::Merge(_) => {
                        //     panic!("Implementation error!");
                        // }
                        SliceResp::Delete => {
                            panic!("Implementation error!");
                        }
                    }
                }

                slice = merged_slice_rx.recv() => {
                    let slice = slice.unwrap(); // We keep a sender half here, so this is never None.

                    let (slice, to_be_removed) = slice.unwrap(); // TODO err handling...

                    self.add_slice(slice, &merged_slice_tx).await.unwrap();

                    for id in to_be_removed {
                        self.remove_slice(id).await.unwrap();
                    }
                }
            }
        }
    }

    fn get(
        &self,
        get_req: SliceGetReq,
        slice_tx: mpsc::UnboundedSender<SliceResp>,
    ) {
        log::debug!("LSM get req: {:?}", get_req);

        // Skip all levels we are already done with.
        // Find the first slice which has timestamps we have not checked yet.
        let mut level = get_req.last_level;
        while let Some(list) = self.slices.get(&level) {
            // The oldest ones come first, but we need to start checking with the newest.
            let mut idx = list.len();
            while idx>0 {
                idx -= 1;

                let slice = list.get(idx).unwrap();
                // If the slice has timestamps which are below the ones we searched so far,
                // continue searching there.
                if get_req.last_min_ts>slice.min_ts {
                    slice.get(get_req, slice_tx);
                    return;
                }
            }

            level += 1
        }

        // We did not find a slice, so return an empty response.
        let resp = if get_req.debug {
            Ok(LSMManagerResp::GetDebug(None))
        } else {
            Ok(LSMManagerResp::Get(None))
        };

        get_req.tx.send(resp).unwrap();
    }

    async fn add_new_slice(
        &mut self,
        pairs: Vec<KVPair>,
        merged_slice_tx: &mpsc::UnboundedSender<std::io::Result<(Slice, Vec<u128>)>>,
    ) -> std::io::Result<()> {
        let start = Instant::now();

        // Abort early if we have an empty slice.
        if pairs.is_empty() {
            return Ok(())
        }

        let min_ts = pairs[0].version;
        let max_ts = pairs[pairs.len()-1].version;

        let slice = self.create_slice(pairs, min_ts, max_ts).await?;

        self.add_slice(slice, merged_slice_tx).await?;

        let elapsed = start.elapsed();
        log::warn!("Added new slice to the LSM. Took {:?}.", elapsed);

        Ok(())
    }

    async fn create_slice(
        &mut self,
        pairs: Vec<KVPair>,
        min_ts: u128,
        max_ts: u128,
    ) -> std::io::Result<Slice> {
        // Get the next id.
        self.latest_id += 1;
        let id = self.latest_id;

        let folder = self.folder.clone();
        let level = 1;

        Slice::new(id, level, pairs, min_ts, max_ts, folder).await
    }

    async fn add_slice(
        &mut self,
        slice: Slice,
        merged_slice_tx: &mpsc::UnboundedSender<std::io::Result<(Slice, Vec<u128>)>>,
    ) -> std::io::Result<()> {
        // Initialize all lower level lists. This is necessary because we search the levels in order
        // while searching for a key and after a merge a in-between level might be empty.
        // The search ends with the first non-existing level and NOT with the first empty level!
        // TODO make sure this gets only used during start up.
        let mut level = 1u64;
        while slice.level>level {
            if let None = self.slices.get(&level) {
                self.slices.insert(level, Vec::with_capacity(self.slices_per_level));
            }

            level += 1;
        }

        // Get the list where the slice is to be inserted.
        // We perform the possibly necessary merging here to avoid borrow rules violations.
        let list = match self.slices.get_mut(&slice.level) {
            Some(list) => list,
            None => {
                let list = Vec::with_capacity(self.slices_per_level);
                self.slices.insert(slice.level, list);

                self.slices.get_mut(&slice.level).unwrap()
            }
        };

        // Merge the current level if necessary.
        let amount = list.iter().filter(|slice| !slice.merging).count();
        if amount>=self.slices_per_level {
            log::warn!("LSMManager.add_slice - Merge triggered - amount: {}", amount);

            // Get the next id.
            self.latest_id += 1;
            let id = self.latest_id;

            let folder = self.folder.clone();

            Self::merge_level(
                list,
                amount,
                folder,
                id,
                merged_slice_tx.clone(),
            ).await?;
        }

        // Add the slice to the list and sort the list aterwards.
        let slice = slice.run();
        list.push(slice);
        list.sort_by_key(|slice| slice.min_ts);

        Ok(())
    }

    async fn merge_level(
        list: &mut Vec<SliceHandle>,
        amount: usize,
        folder: PathBuf,
        id: u128,
        merged_slice_tx: mpsc::UnboundedSender<std::io::Result<(Slice, Vec<u128>)>>,
    ) -> std::io::Result<()> {
        // Initialize the collection of MergeSlice's.
        let mut old = Vec::with_capacity(amount);
        let mut to_be_removed = Vec::with_capacity(amount);

        // Construct and collect all merge slices.
        for slice in list.iter_mut().filter(|slice| !slice.merging) {
            let merge_slice = MergeSlice::from_slice_handle(slice).await?;

            slice.merging = true;

            old.push(merge_slice);
            to_be_removed.push(slice.id);
        }

        // Merge the slices.
        tokio::spawn(async move {
            let result = Slice::merge_multi(
                old,
                id,
                folder,
            ).await;

            let result = result.map(|slice| (slice, to_be_removed));

            merged_slice_tx.send(result).unwrap();
        });

        Ok(())
    }

    async fn remove_slice(
        &mut self,
        id: u128,
    ) -> std::io::Result<()> {
        for (_, list) in self.slices.iter_mut() {
            for idx in 0..list.len() {
                let slice = list.get(idx).unwrap(); // Can not fail.

                if slice.id==id {
                    let (slice_tx, mut slice_rx) = mpsc::unbounded_channel();

                    slice.delete(slice_tx);

                    slice_rx.recv().await.unwrap();

                    list.remove(idx);

                    return Ok(())
                }
            }
        }

        Err(Error::new(ErrorKind::NotFound, "Slice which was meant to be removed could not be found."))
    }
}
