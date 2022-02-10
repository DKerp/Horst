use crate::*;



#[derive(Default, Clone, Debug)]
pub struct SliceHeader {
    /// The unique id of this LSM slice.
    pub id: u128,
    /// The number of entries inside the slice.
    pub len: u64,
    /// The level inside the LSM-tree of this slice.
    pub level: u64,
    /// The minimal timestamp contained in this slice.
    pub min_ts: u128,
    /// The maximum timestamp contained in this slice.
    pub max_ts: u128,
}

impl Version for SliceHeader {
    const VERSION: u16 = 1;
}

/// Returns the static disk size (64).
impl DiskSize for SliceHeader {
    const STATIC_SIZE: u64 = 16 + 8 + 8 + 16 + 16;
}

#[async_trait]
impl SaveToDisk for SliceHeader {
    async fn save_to_disk<W: AsyncWriteExt + Send + Sync + Unpin>(
        &self,
        disk: &mut W,
    ) -> std::io::Result<()> {
        disk.write_all(&self.id.to_be_bytes()).await?;
        disk.write_all(&self.len.to_be_bytes()).await?;
        disk.write_all(&self.level.to_be_bytes()).await?;
        disk.write_all(&self.min_ts.to_be_bytes()).await?;
        disk.write_all(&self.max_ts.to_be_bytes()).await
    }
}

#[async_trait]
impl LoadFromDisk for SliceHeader {
    async fn load_from_disk<R: AsyncReadExt + Send + Sync + Unpin>(
        disk: &mut R,
    ) -> std::io::Result<Self> {
        // Create a buffer for reading the data.
        let mut store = vec![0u8; Self::STATIC_SIZE as usize];

        Self::load_from_disk_with_buffer(disk, &mut store).await
    }

    async fn load_from_disk_with_buffer<R: AsyncReadExt + Send + Sync + Unpin>(
        disk: &mut R,
        buffer: &mut Vec<u8>,
    ) -> std::io::Result<Self> {
        // Make sure the buffer has the correct size.
        buffer.resize(Self::STATIC_SIZE as usize, 0u8);

        disk.read_exact(&mut buffer[..]).await?;

        // Parse the header.
        let value: [u8; 16] = buffer[..16].try_into().unwrap();
        let id = u128::from_be_bytes(value);
        let value: [u8; 8] = buffer[16..24].try_into().unwrap();
        let len = u64::from_be_bytes(value);
        let value: [u8; 8] = buffer[24..32].try_into().unwrap();
        let level = u64::from_be_bytes(value);
        let value: [u8; 16] = buffer[32..48].try_into().unwrap();
        let min_ts = u128::from_be_bytes(value);
        let value: [u8; 16] = buffer[48..64].try_into().unwrap();
        let max_ts = u128::from_be_bytes(value);

        Ok(Self {
            id,
            len,
            level,
            min_ts,
            max_ts,
        })
    }
}



#[derive(Debug)]
pub enum SliceReq {
    /// Retrieve a cetain key from the lsm tree. Gives back the newest version before or at the given read timestamp.
    Get(SliceGetReq),
    /// Request a merge slice copy.
    Merge,
    /// Request the deletion of the slice.
    Delete,
}

#[derive(Debug)]
pub enum SliceResp {
    /// The response to a get request. If the [`KVPair`] was found it gets added to the [`SliceGetReq`].
    Get(SliceGetReq),
    /// The response to a merge request.
    Merge(MergeSlice),
    /// The empty response to a deletion request.
    Delete,
}

#[derive(Debug)]
pub struct SliceGetReq {
    /* What we are searching for. */
    pub key: u128,
    pub read_ts: u128,

    /* The object necessary for returning the result. */
    pub tx: oneshot::Sender<std::io::Result<LSMManagerResp>>,

    /* Bookkeeping of which slices we have already searched trough. */
    pub last_level: u64,
    pub last_min_ts: u128,

    pub debug: bool,
}

impl SliceGetReq {
    pub fn new(
        key: u128,
        read_ts: u128,
        tx: oneshot::Sender<std::io::Result<LSMManagerResp>>,
    ) -> Self {
        Self {
            key,
            read_ts,
            tx,
            last_level: 1,
            last_min_ts: u128::MAX,
            debug: false,
        }
    }
}

#[derive(Debug)]
pub struct SliceHandle {
    pub id: u128,
    pub min_ts: u128,
    pub max_ts: u128,
    pub sender: mpsc::UnboundedSender<(SliceReq, mpsc::UnboundedSender<SliceResp>)>,
}

impl PartialEq for SliceHandle {
    fn eq(&self, other: &Self) -> bool {
        self.max_ts==other.max_ts
    }
}

impl Eq for SliceHandle {}

impl PartialOrd for SliceHandle {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SliceHandle {
    fn cmp(&self, other: &Self) -> Ordering {
        self.max_ts.cmp(&other.max_ts)
    }
}

impl SliceHandle {
    fn new(
        slice: &Slice,
        sender: mpsc::UnboundedSender<(SliceReq, mpsc::UnboundedSender<SliceResp>)>,
    ) -> Self {
        let id = slice.id;
        let min_ts = slice.min_ts;
        let max_ts = slice.max_ts;

        Self {
            id,
            min_ts,
            max_ts,
            sender,
        }
    }
}

impl SliceHandle {
    pub fn get(
        &self,
        req: SliceGetReq,
        tx: mpsc::UnboundedSender<SliceResp>,
    ) {
        let req = SliceReq::Get(req);

        self.sender.send((req, tx)).unwrap();
    }

    pub fn merge(
        &self,
        tx: mpsc::UnboundedSender<SliceResp>,
    ) {
        let req = SliceReq::Merge;

        self.sender.send((req, tx)).unwrap();
    }

    pub fn delete(
        &self,
        tx: mpsc::UnboundedSender<SliceResp>,
    ) {
        let req = SliceReq::Delete;

        self.sender.send((req, tx)).unwrap();
    }
}

/// A contigues sorted slice of key data.
#[derive(Debug)]
pub struct Slice {
    /// The unique id of this LSM slice.
    pub id: u128,
    /// The path of the underlying file.
    pub path: PathBuf,
    /// The underlying file where the slice is saved.
    file: Cursor<Mmap>,//FileContainer,
    /// The number of entries inside the slice.
    pub len: u64,
    /// The level inside the LSM-tree of this slice.
    pub level: u64,
    /// The minimal timestamp contained in this slice.
    pub min_ts: u128,
    /// The maximum timestamp contained in this slice.
    pub max_ts: u128,
    /// A buffer for reading serialized key value pairs.
    pub buffer: Vec<u8>,
    /// A buffer for incomming read requests.
    pub get_buffer: BTreeMap<KVPair, (SliceGetReq, mpsc::UnboundedSender<SliceResp>)>
}

impl Slice {
    /// Create a new LSM slice with the given data inside the given folder.
    pub async fn new(
        id: u128,
        level: u64,
        pairs: Vec<KVPair>,
        min_ts: u128,
        max_ts: u128,
        folder: impl AsRef<Path>,
    ) -> std::io::Result<Self> {
        let path = folder.as_ref().join(format!("{}.slice", id));

        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;

        let mut writer = BufWriter::new(file);

        let len = pairs.len() as u64;

        let header = SliceHeader {
            id,
            len,
            level,
            min_ts,
            max_ts,
        };

        let main_header = MainHeader {
            file_type: FileType::LSMSlice,
            header_version: SliceHeader::VERSION,
            header_size: header.disk_size(),
        };

        main_header.save_to_disk(&mut writer).await?;
        header.save_to_disk(&mut writer).await?;

        for pair in pairs {
            pair.save_to_disk(&mut writer).await?;
        }

        // Make sure the whole slice has hit the disk, and then close the file with write permission.
        writer.flush().await?;
        let file = writer.into_inner();
        file.sync_all().await?;
        drop(file);

        Self::open(path).await
    }

    /// Open an old LSM slice with the given id inside the given folder. Will also extract the
    /// the relevant meta data from the header data at the beginning of the file.
    pub async fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .await?;

        // let mut file = FileContainer::new(file);

        let file = file.into_std().await;
        let file = unsafe {
            Mmap::map(&file)?
        };
        let mut file = Cursor::new(file);

        // let mut file = CachingReader::new(file, 4096).await?;

        let main_header = MainHeader::load_from_disk(&mut file).await?;
        if main_header.file_type!=FileType::LSMSlice {
            return Err(Error::new(ErrorKind::InvalidData, "Invalid Slice file found. File header indicates the wrong file type."));
        }

        let header = SliceHeader::load_from_disk(&mut file).await?;
        let id = header.id;
        let len = header.len;
        let level = header.level;
        let min_ts = header.min_ts;
        let max_ts = header.max_ts;

        let buffer = vec![0u8; KVPair::STATIC_SIZE as usize];

        Ok(Self {
            id,
            path,
            file,
            len,
            level,
            min_ts,
            max_ts,
            buffer,
            get_buffer: BTreeMap::new(),
        })
    }

    /// Checks if the slice is fully written, or if it got corrupted due to a crash before writting
    /// finished.
    pub async fn is_fully_written(&mut self) -> std::io::Result<bool> {
        // Determine the size of the file.
        let total = self.file.seek(SeekFrom::End(0)).await?;

        // Determine the expected size. (header + body)
        let expected = MainHeader::STATIC_SIZE + SliceHeader::STATIC_SIZE;
        let expected = expected + self.len*KVPair::STATIC_SIZE;

        Ok(total==expected)
    }

    /// Run the [`Slice`] in a seperate task and return a handle to it.
    pub fn run(self) -> SliceHandle {
        let (tx, rx) = mpsc::unbounded_channel();

        let handle = SliceHandle::new(&self, tx);

        spawn(async move {
            self.run_inner(rx).await;
        });

        handle
    }

    /// The main loop of the [`Slice`] task.
    async fn run_inner(
        mut self,
        mut rx: mpsc::UnboundedReceiver<(SliceReq, mpsc::UnboundedSender<SliceResp>)>,
    ) {
        // let mut interval = tokio::time::interval(Duration::from_millis(1));

        loop {
            tokio::select! {
                req = rx.recv() => {
                    let (req, tx) = match req {
                        Some(req) => req,
                        None => break,
                    };

                    match req {
                        SliceReq::Get(mut get_req) => {
                            get_req.last_level = self.level;
                            get_req.last_min_ts = self.min_ts;

                            // let dummy_pair = KVPair::from_key_and_version(
                            //     get_req.key,
                            //     get_req.read_ts,
                            // );
                            //
                            // self.get_buffer.insert(dummy_pair, (get_req, tx));

                            let debug = get_req.debug;

                            let result = if debug {
                                self.get_debug(get_req.key, get_req.read_ts).await
                            } else {
                                self.get(get_req.key, get_req.read_ts).await
                            };

                            match result {
                                Ok(Some(pair)) => {
                                    let resp = if debug {
                                        LSMManagerResp::GetDebug(Some(pair))
                                    } else {
                                        LSMManagerResp::Get(Some(pair))
                                    };

                                    get_req.tx.send(Ok(resp)).unwrap();
                                }
                                Ok(None) => {
                                    let resp = SliceResp::Get(get_req);

                                    tx.send(resp).unwrap();
                                }
                                Err(err) => get_req.tx.send(Err(err)).unwrap(),
                            }
                        }
                        SliceReq::Merge => {
                            // TODO add err handler...
                            let merge_slice = MergeSlice::from_slice(self).await.unwrap();

                            log::warn!("Merge - Successfully seeked to the beginning.");

                            let resp = SliceResp::Merge(merge_slice);

                            tx.send(resp).unwrap();
                            break;
                        }
                        SliceReq::Delete => {
                            panic!("Implementation error!");
                        }
                    }
                }
                // _ = interval.tick() => {
                //     if self.get_buffer.is_empty() {
                //         continue;
                //     }
                //
                //     let mut l_hint = 0u64;
                //
                //     let get_buffer = std::mem::replace(&mut self.get_buffer, BTreeMap::new());
                //
                //     // let get_buffer_len = get_buffer.len();
                //     //
                //     // if get_buffer_len>10 {
                //     //     log::warn!("Slice {} - Trying to get KVPairs... len: {}", self.id, get_buffer_len);
                //     // }
                //     //
                //     // let start = Instant::now();
                //
                //     for (_, (get_req, tx)) in get_buffer.into_iter() {
                //         match self.get2(get_req.key, get_req.read_ts, l_hint).await {
                //             Ok((Some(pair), l)) => {
                //                 l_hint = l;
                //
                //                 let resp = LSMManagerResp::Get(Some(pair));
                //
                //                 get_req.tx.send(Ok(resp)).unwrap();
                //             }
                //             Ok((None, l)) => {
                //                 l_hint = l;
                //
                //                 let resp = SliceResp::Get(get_req);
                //
                //                 tx.send(resp).unwrap();
                //             }
                //             Err(err) => get_req.tx.send(Err(err)).unwrap(),
                //         }
                //     }
                //
                //     // if get_buffer_len>10 {
                //     //     log::warn!("Slice {} - Getting KVPairs took {:?}.", self.id, start.elapsed());
                //     // }
                // }
            }
        }
    }

    pub async fn read_nth(&mut self, nth: u64) -> std::io::Result<KVPair> {
        if nth>=self.len {
            log::error!("Reading KVPair from LSM Slice failed, nth was too big! id: {}, len: {}, nth: {}", self.id, self.len, nth);
            return Err(Error::new(ErrorKind::InvalidInput, "nth was too big."));
        }

        let offset = MainHeader::STATIC_SIZE + SliceHeader::STATIC_SIZE;
        let offset = offset + nth*KVPair::STATIC_SIZE;

        self.file.seek(SeekFrom::Start(offset)).await?;

        KVPair::load_from_disk_with_buffer(
            &mut self.file,
            &mut self.buffer,
        ).await
    }

    /// Try to retrieve the latest version of the key with a commit timestamp lower or equal the
    /// given read_ts.
    pub async fn get(&mut self, key: u128, read_ts: u128) -> std::io::Result<Option<KVPair>> {
        // Use binary search.
        let mut l = 0u64;
        let mut r = self.len-1;

        let compare = KVPair::from_key_and_version(key, read_ts);
        let mut m = (l+r)/2;

        // We store the last loaded value here to reevaluate it after the binary search.
        let mut pair = KVPair::default();
        // We also store if the last miss was a lesser value or not (implying greater);
        let mut less = false;

        log::debug!("Slice.get - key: {}, read_ts: {}", key, read_ts);

        while r>=l {
            // Calculate the middle. (Rust integer division results get floored)
            m = (l+r)/2;

            // Load and parse the corresponding pair from disk.
            pair = self.read_nth(m).await?;

            log::debug!("Slice.get - pair: {:?}", pair);

            // Compare and continue as appropriate.
            match pair.cmp(&compare) {
                Ordering::Less => {
                    less = true;
                    l = match m.checked_add(1) {
                        Some(m) => m,
                        None => break,
                    };
                }
                Ordering::Greater => {
                    less = false;
                    r = match m.checked_sub(1) {
                        Some(m) => m,
                        None => break,
                    };
                }
                Ordering::Equal => return Ok(Some(pair)),
            }
        }

        log::debug!("Slice.get - pair (2): {:?}, less: {:?}", pair, less);

        if less {
            if pair.key==key {
                return Ok(Some(pair));
            }
        } else {
            if m>0 {
                pair = self.read_nth(m-1).await?;

                if pair.key==key {
                    return Ok(Some(pair));
                }
            }
        }

        Ok(None)
    }

    /// Try to retrieve the latest version of the key with a commit timestamp lower or equal the
    /// given read_ts.
    pub async fn get_debug(&mut self, key: u128, read_ts: u128) -> std::io::Result<Option<KVPair>> {
        // Use binary search.
        let mut l = 0u64;
        let mut r = self.len-1;

        let compare = KVPair::from_key_and_version(key, read_ts);
        let mut m = (l+r)/2;

        // We store the last loaded value here to reevaluate it after the binary search.
        let mut pair = KVPair::default();
        // We also store if the last miss was a lesser value or not (implying greater);
        let mut less = false;

        log::warn!("DEBUG - Slice.get - key: {}, read_ts: {}", key, read_ts);

        while r>=l {
            // Calculate the middle. (Rust integer division results get floored)
            m = (l+r)/2;

            // Load and parse the corresponding pair from disk.
            pair = self.read_nth(m).await?;

            log::warn!("DEBUG - Slice.get - pair: {:?}", pair);

            // Compare and continue as appropriate.
            match pair.cmp(&compare) {
                Ordering::Less => {
                    l = m+1;
                    less = true;
                }
                Ordering::Greater => {
                    r = m-1;
                    less = false;
                }
                Ordering::Equal => return Ok(Some(pair)),
            }
        }

        log::warn!("DEBUG - Slice.get - pair (2): {:?}, less: {:?}, m: {}", pair, less, m);

        let m_debug = m.checked_sub(2).unwrap_or(0);
        for m_debug in m_debug..m_debug+5 {
            let pair = self.read_nth(m_debug).await?;

            log::warn!("DEBUG - Slice.get - neighbours - m: {}, pair: {:?}", m_debug, pair);

            if pair.version==1 {
                return Ok(Some(pair));
            }
        }

        // if less {
        //     if pair.key==key {
        //         return Ok(Some(pair));
        //     }
        // } else {
        //     if m>0 {
        //         pair = self.read_nth(m-1).await?;
        //
        //         if pair.key==key {
        //             return Ok(Some(pair));
        //         }
        //     }
        // }

        Ok(None)
    }

    /// Try to retrieve the latest version of the key with a commit timestamp lower or equal the
    /// given read_ts.
    pub async fn get2(&mut self, key: u128, read_ts: u128, l_hint: u64) -> std::io::Result<(Option<KVPair>, u64)> {
        // Use binary search.
        let mut l = l_hint;
        let mut r = self.len-1;

        let compare = KVPair::from_key_and_version(key, read_ts);
        let mut m = (l+r)/2;

        // We store the last loaded value here to reevaluate it after the binary search.
        let mut pair = KVPair::default();
        // We also store if the last miss was a lesser value or not (implying greater);
        let mut less = false;

        log::debug!("Slice.get - key: {}, read_ts: {}", key, read_ts);

        while r>=l {
            // Calculate the middle. (Rust integer division results get floored)
            m = (l+r)/2;

            // Load and parse the corresponding pair from disk.
            pair = self.read_nth(m).await?;

            log::debug!("Slice.get - pair: {:?}", pair);

            // Compare and continue as appropriate.
            match pair.cmp(&compare) {
                Ordering::Less => {
                    l = m+1;
                    less = true;
                }
                Ordering::Greater => {
                    r = m-1;
                    less = false;
                }
                Ordering::Equal => return Ok((Some(pair), l+1)),
            }
        }

        log::debug!("Slice.get - pair (2): {:?}, less: {:?}", pair, less);

        l = 0;//l.checked_sub(1).unwrap_or(0);

        if less {
            if pair.key==key {
                return Ok((Some(pair), l));
            }

            return Ok((None, l));
        } else {
            if m>0 {
                pair = self.read_nth(m-1).await?;

                if pair.key==key {
                    return Ok((Some(pair), l));
                }
            }

            return Ok((None, l))
        }
    }

    pub async fn remove(self) -> std::io::Result<()> {
        fs::remove_file(&self.path).await
    }

    pub async fn merge_multi(
        mut old: Vec<MergeSlice>,
        id: u128,
        folder: impl AsRef<Path>,
    ) -> std::io::Result<Slice> {
        let start = Instant::now();

        let path = folder.as_ref().join(format!("{}.slice", id));

        old.sort();

        let mut to_be_removed: Vec<MergeSlice> = Vec::with_capacity(old.len());

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;

        // Calculate the size of the merged slice
        let len = {
            let mut total = 0;
            for slice in old.iter() {
                total += slice.len;
            }
            total
        };

        // Calculate the level of the merged slice.
        let level = old[0].level+1;

        // Calculate the minimal/maximal timestamp of the merged slice.
        let min_ts = old[0].min_ts;
        let max_ts = old[old.len()-1].max_ts;

        // Initialize - load the first pair from each file.
        for slice in old.iter_mut() {
            slice.read_next().await?;
        }

        let mut writer = BufWriter::new(file);

        let header = SliceHeader {
            id,
            len,
            level,
            min_ts,
            max_ts,
        };

        let main_header = MainHeader {
            file_type: FileType::LSMSlice,
            header_version: SliceHeader::VERSION,
            header_size: header.disk_size(),
        };

        main_header.save_to_disk(&mut writer).await?;
        header.save_to_disk(&mut writer).await?;

        while !old.is_empty() {
            // Find the "smallest" pair from all.
            let mut idx_min = 0;
            let mut pair_min: KVPair = old[0].current_pair;

            let mut idx = 1;
            while let Some(slice) = old.get(idx) {
                if pair_min>slice.current_pair {
                    pair_min = slice.current_pair;
                    idx_min = idx;
                }

                idx += 1;
            }

            // Write that pair to the merged slice.
            pair_min.save_to_disk(&mut writer).await?;

            // Load the next element from the slice we just took the pair from.
            let element = old.get_mut(idx_min).unwrap();
            if element.read_next().await? {
                let element = old.remove(idx_min);
                to_be_removed.push(element);
            }
        }

        // Make sure the whole slice has hit the disk, and then close the file with write permission.
        writer.flush().await?;
        let file = writer.into_inner();
        file.sync_all().await?;
        drop(file);

        log::warn!("merge_multi - Trying to open merged file.");

        // Open the newly created slice.
        let slice = Slice::open(&path).await?;

        // Delete the old slices from the disk.
        for slice in to_be_removed {
            slice.remove().await?;
        }

        let elapsed = start.elapsed();
        log::warn!("Merged multiple slices. Took {:?}.", elapsed);

        Ok(slice)
    }
}



/// Defines a util structure for performing the slice merge.
#[derive(Debug)]
pub struct MergeSlice {
    path: PathBuf,
    file: BufReader<fs::File>,
    len: u64,
    level: u64,
    min_ts: u128,
    max_ts: u128,
    buffer: Vec<u8>,
    current_pair: KVPair,
    next_pair_nr: u64,
}

impl PartialEq for MergeSlice {
    fn eq(&self, other: &Self) -> bool {
        self.max_ts==other.max_ts
    }
}

impl Eq for MergeSlice {}

impl PartialOrd for MergeSlice {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeSlice {
    fn cmp(&self, other: &Self) -> Ordering {
        self.max_ts.cmp(&other.max_ts)
    }
}

impl MergeSlice {
    async fn from_slice(inner: Slice) -> std::io::Result<Self> {
        let mut file = fs::OpenOptions::new()
            .read(true)
            .open(&inner.path)
            .await?;

        // let mut file = inner.file.into_inner();//.into_inner();

        let offset = MainHeader::STATIC_SIZE + SliceHeader::STATIC_SIZE;

        file.seek(SeekFrom::Start(offset)).await?;

        let file = BufReader::new(file);

        Ok(Self {
            path: inner.path,
            file,
            len: inner.len,
            level: inner.level,
            min_ts: inner.min_ts,
            max_ts: inner.max_ts,
            buffer: inner.buffer,
            current_pair: KVPair::default(),
            next_pair_nr: 0,
        })
    }

    async fn read_next(&mut self) -> std::io::Result<bool> {
        if self.next_pair_nr>=self.len {
            return Ok(true); // signal that we are done.
        }

        self.current_pair = KVPair::load_from_disk_with_buffer(
            &mut self.file,
            &mut self.buffer,
        ).await?;

        self.next_pair_nr += 1;

        Ok(false)
    }

    // async fn seek_to_begin(&mut self) -> std::io::Result<u64> {
    //     let offset = MainHeader::STATIC_SIZE + SliceHeader::STATIC_SIZE;
    //
    //     self.file.seek(SeekFrom::Start(offset)).await
    // }

    pub async fn remove(self) -> std::io::Result<()> {
        fs::remove_file(&self.path).await
    }
}
