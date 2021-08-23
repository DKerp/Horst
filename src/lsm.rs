use crate::*;



#[derive(Debug)]
pub enum LSMManagerReq {
    /// Create a new [`Slice`] on disk with the given list [`KVPair`]s, where all pairs have a commit
    /// timestamp between the given min_ts and max_ts.
    AddSlice(Vec<KVPair>, u128, u128),
    /// Retrieve a cetain key from the lsm tree. Gives back the newest version before or at the given read timestamp.
    Get(u128, u128, oneshot::Sender<std::io::Result<KVStoreResp>>),
}

#[derive(Debug)]
pub enum LSMManagerResp {
    /// The empty response to an add slice request.
    AddSlice,
}

pub struct LSMManagerHandle {
    sender: mpsc::UnboundedSender<(LSMManagerReq, Option<oneshot::Sender<std::io::Result<LSMManagerResp>>>)>,
}

impl From<mpsc::UnboundedSender<(LSMManagerReq, Option<oneshot::Sender<std::io::Result<LSMManagerResp>>>)>> for LSMManagerHandle {
    fn from(sender: mpsc::UnboundedSender<(LSMManagerReq, Option<oneshot::Sender<std::io::Result<LSMManagerResp>>>)>) -> Self {
        Self {
            sender,
        }
    }
}

impl LSMManagerHandle {
    pub async fn add_slice(
        &self,
        pairs: Vec<KVPair>,
        min_ts: u128,
        max_ts: u128,
    ) -> std::io::Result<()> {
        let req = LSMManagerReq::AddSlice(pairs, min_ts, max_ts);

        let (tx, rx) = oneshot::channel();

        self.sender.send((req, Some(tx))).unwrap();

        rx.await.unwrap()?;
        Ok(())
    }

    pub fn get(
        &self,
        key: u128,
        version: u128,
        tx: oneshot::Sender<std::io::Result<KVStoreResp>>,
    ) {
        let req = LSMManagerReq::Get(key, version, tx);

        self.sender.send((req, None)).unwrap();
    }
}


/// The manager of the LSM tree. Maintains a list of all [`Slice`]s of the LSM tree.
#[derive(Debug)]
pub struct LSMManager {
    /// The path where the different slices are saved at.
    pub path: PathBuf,
    /// The biggest id in use by a [`Slice`]. Get incremented with each newly created [`Slice`].
    pub latest_id: u128,
    /// The number of slices at each level.
    pub slices_per_level: u64,
    /// The store for all levels (above 0) and the [`Slice`]s it contains.
    pub slices: Vec<Vec<Slice>>,
}

impl LSMManager {
    pub async fn new(
        folder: impl AsRef<Path>,
        slices_per_level: u64,
    ) -> std::io::Result<Self> {
        let meta = fs::metadata(folder.as_ref()).await?;

        if !meta.is_dir() {
            return Err(Error::new(ErrorKind::InvalidInput, "The given folder path is not a folder!"));
        }

        if meta.permissions().readonly() {
            return Err(Error::new(ErrorKind::Other, "The folder can not be written!"));
        }

        // TODO read and set the file permissions as appropriate.

        let path = folder.as_ref().to_path_buf();

        Ok(Self {
            path,
            latest_id: 0,
            slices_per_level,
            slices: Vec::new(),
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
        mut rx: mpsc::UnboundedReceiver<(LSMManagerReq, Option<oneshot::Sender<std::io::Result<LSMManagerResp>>>)>,
    ) {
        let (slice_tx, mut slice_rx) = mpsc::unbounded_channel();

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

                resp = slice_rx.recv() => {
                    match resp {
                        SliceResp::Get(get_req) => {
                            if get_req.pair.is_some() {
                                
                            }
                        }
                        SliceResp::Merge(merge_slice) => {

                        }
                        SliceResp::Delete(level, id) => {

                        }
                    }
                }
            }
        }
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
    Delete(u64, u128),
}

#[derive(Debug)]
pub struct SliceGetReq {
    pub key: u128,
    pub version: u128,
    pub tx: oneshot::Sender<std::io::Result<KVStoreResp>>,
    pub pair: Option<KVPair>,
}

pub struct SliceHandle {
    sender: mpsc::UnboundedSender<(SliceReq, mpsc::UnboundedSender<std::io::Result<SliceResp>>)>,
}

impl From<mpsc::UnboundedSender<(SliceReq, mpsc::UnboundedSender<std::io::Result<SliceResp>>)>> for SliceHandle {
    fn from(sender: mpsc::UnboundedSender<(SliceReq, mpsc::UnboundedSender<std::io::Result<SliceResp>>)>) -> Self {
        Self {
            sender,
        }
    }
}

impl SliceHandle {
    pub async fn get(
        &self,
        req: SliceGetReq,
        tx: mpsc::UnboundedSender<std::io::Result<SliceResp>>,
    ) {
        let req = SliceReq::Get(req);

        self.sender.send((req, tx)).unwrap();
    }

    pub async fn merge(
        &self,
        tx: mpsc::UnboundedSender<std::io::Result<SliceResp>>,
    ) {
        let req = SliceReq::Merge;

        self.sender.send((req, tx)).unwrap();
    }

    pub async fn delete(
        &self,
        tx: mpsc::UnboundedSender<std::io::Result<SliceResp>>,
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
    pub file: fs::File,
    /// The number of entries inside the slice.
    pub len: u64,
    /// The level inside the LSM-tree of this slice.
    pub level: u64,
    /// The minimal timestamp contained in this slice.
    pub min_ts: u128,
    /// The maximum timestamp contained in this slice.
    pub max_ts: u128,
}

impl Slice {
    pub const HEADER_SIZE: u64 = 64;

    /// Create a new LSM slice with the given data inside the given folder.
    /// The header data gets written immidately, but the actual slice data get written
    /// to the file later on.
    pub async fn new(
        id: u128,
        len: u64,
        level: u64,
        min_ts: u128,
        max_ts: u128,
        folder: impl AsRef<Path>,
    ) -> std::io::Result<Self> {
        let path = folder.as_ref().join(format!("{}.vlog", id));

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path.clone())
            .await?;

        let mut slice = Self {
            id,
            path,
            file,
            len,
            level,
            min_ts,
            max_ts,
        };

        slice.write_header().await?;

        Ok(slice)
    }

    /// Open an old LSM slice with the given id inside the given folder. Will also extract the
    /// the relevant meta data from the header data at the beginning of the file.
    pub async fn open(id: u128, folder: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = folder.as_ref().join(format!("{}.slice", id));

        let file = fs::OpenOptions::new()
            .read(true)
            .open(path.clone())
            .await?;

        let mut slice = Self {
            id,
            path,
            file,
            len: 0,
            level: 0,
            min_ts: 0,
            max_ts: 0,
        };

        slice.read_header().await?;

        Ok(slice)
    }

    /// Run the [`Slice`] in a seperate task and return a handle to it.
    pub fn run(self) -> SliceHandle {
        let (tx, rx) = mpsc::unbounded_channel();

        spawn(async move {
            self.run_inner(rx).await;
        });

        tx.into()
    }

    /// The main loop of the [`Slice`] task.
    async fn run_inner(
        mut self,
        mut rx: mpsc::UnboundedReceiver<(SliceReq, mpsc::UnboundedSender<std::io::Result<SliceResp>>)>,
    ) {

    }

    async fn read_header(&mut self) -> std::io::Result<()> {
        // Create a buffer for reading the data.
        let mut store = vec![0u8; Self::HEADER_SIZE as usize];

        // Read the header.
        self.file.seek(SeekFrom::Start(0)).await?;
        self.file.read_exact(&mut store[..]).await?;

        // Parse the header.
        let value: [u8; 16] = store[..16].try_into().unwrap();
        self.id = u128::from_be_bytes(value);
        let value: [u8; 8] = store[16..24].try_into().unwrap();
        self.len = u64::from_be_bytes(value);
        let value: [u8; 8] = store[24..32].try_into().unwrap();
        self.level = u64::from_be_bytes(value);
        let value: [u8; 16] = store[32..48].try_into().unwrap();
        self.min_ts = u128::from_be_bytes(value);
        let value: [u8; 16] = store[48..64].try_into().unwrap();
        self.max_ts = u128::from_be_bytes(value);

        Ok(())
    }

    async fn write_header(&mut self) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(0)).await?;

        let mut writer = BufWriter::new(&mut self.file);

        writer.write_all(&self.id.to_be_bytes()).await?;
        writer.write_all(&self.len.to_be_bytes()).await?;
        writer.write_all(&self.level.to_be_bytes()).await?;
        writer.write_all(&self.min_ts.to_be_bytes()).await?;
        writer.write_all(&self.max_ts.to_be_bytes()).await?;

        Ok(())
    }

    pub async fn read_nth(&mut self, nth: u64) -> std::io::Result<KVPair> {
        if nth>=self.len {
            return Err(Error::new(ErrorKind::InvalidInput, "nth was too big."));
        }

        KVPair::read_nth(&mut self.file, nth).await
    }

    /// Try to retrieve the latest version of the key with a commit timestamp lower or equal the
    /// given read_ts.
    pub async fn get(&mut self, key: u128, read_ts: u128) -> std::io::Result<Option<KVPair>> {
        // Use binary search.
        let mut l = 0u64;
        let mut r = self.len-1;

        let compare = KVPair::from_key_and_version(key, read_ts);
        let mut m = (l+r)/2;

        while r>=l {
            // Calculate the middle. (Rust integer division results get floored)
            m = (l+r)/2;
            // Load and parse the corresponding pair from disk.
            let pair = self.read_nth(m).await?;
            // Compare and continue as appropriate.
            match pair.cmp(&compare) {
                Ordering::Less => l = m+1,
                Ordering::Greater => r = m-1,
                Ordering::Equal => return Ok(Some(pair)),
            }
        }

        if m>0 {
            let pair = self.read_nth(m-1).await?;
            if pair.key==key {
                return Ok(Some(pair));
            }
        }

        Ok(None)
    }

    /// Creates a clone of this slice usuable for merging into a bigger slice.
    pub async fn create_merge_slice(&self) -> std::io::Result<MergeSlice> {
        let file = fs::OpenOptions::new()
            .read(true)
            .open(&self.path)
            .await?;

        Ok(MergeSlice {
            reader: BufReader::new(file),
            len: self.len,
            level: self.level,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            current_pair: KVPair::default(),
            current_pair_nr: 0,
        })
    }

    pub async fn merge_multi<W>(
        mut old: Vec<MergeSlice>,
        id: u128,
        folder: impl AsRef<Path>,
    ) -> std::io::Result<Slice> {
        let path = folder.as_ref().join(format!("{}.slice", id));

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path.clone())
            .await?;

        // Calculate the size of the merged slice
        let len = {
            let mut total = 0;
            for slice in old.iter() {
                total = total + slice.len;
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

        while !old.is_empty() {
            // Find the "smallest" pair from all.
            let mut iter = old.iter().enumerate();
            let mut counter = 0;
            let mut min: &KVPair = &iter.next().unwrap().1.current_pair;
            for (idx, element) in iter {
                let pair = &element.current_pair;
                if min>pair {
                    min = pair;
                    counter = idx;
                }
            }

            // Write that pair to the merged slice.
            min.write(&mut writer).await?;

            // Load the next element from the slice we just took the pair from.
            let element = old.get_mut(counter).unwrap();
            if element.read_next().await? {
                old.remove(counter);
            }
        }

        // Make sure the whole slice has hit the disk, and then close the file with write permission.
        writer.flush().await?;
        let file = writer.into_inner();
        file.sync_all().await?;
        drop(file);

        // Open the file again in read-only mode.
        let file = fs::OpenOptions::new()
            .read(true)
            .open(path.clone())
            .await?;

        Ok(Self {
            id,
            path,
            file,
            len,
            level,
            min_ts,
            max_ts,
        })
    }
}

/// Defines a util structure for performing the slice merge.
#[derive(Debug)]
pub struct MergeSlice {
    reader: BufReader<fs::File>,
    len: u64,
    level: u64,
    min_ts: u128,
    max_ts: u128,
    current_pair: KVPair,
    current_pair_nr: u64,
}

// impl From<Slice> for MergeSlice {
//     fn from(slice: Slice) -> Self {
//         Self {
//             reader: BufReader::new(slice.file),
//             len: slice.len,
//             level: slice.level,
//             min_ts: slice.min_ts,
//             max_ts: slice.max_ts,
//             current_pair: KVPair::default(),
//             current_pair_nr: 0,
//         }
//     }
// }

impl MergeSlice {
    async fn read_next(&mut self) -> std::io::Result<bool> {
        if self.current_pair_nr>=self.len {
            return Ok(true); // signal that we are done.
        }

        self.current_pair_nr = self.current_pair_nr + 1;
        self.current_pair = KVPair::read_nth(&mut self.reader, self.current_pair_nr).await?;

        Ok(false)
    }
}
