use crate::*;



#[derive(Debug)]
pub enum LSMManagerReq {
    /// Create a new [`Slice`] on disk with the given list [`KVPair`]s, where all pairs have a commit
    /// timestamp between the given min_ts and max_ts.
    AddSlice(Vec<KVPair>, u128, u128),
    /// Retrieve a cetain key from the lsm tree. Gives back the newest version before or at the given read timestamp.
    Get(u128, u128),
}

#[derive(Debug)]
pub enum LSMManagerResp {
    /// The empty response to an add slice request.
    AddSlice,
    /// The response to a get request, containing the searched for [`KVPair`] if it could be found.
    Get(Option<KVPair>)
}

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
    pub async fn add_slice(
        &self,
        pairs: Vec<KVPair>,
        min_ts: u128,
        max_ts: u128,
    ) -> std::io::Result<()> {
        let req = LSMManagerReq::AddSlice(pairs, min_ts, max_ts);

        let (tx, rx) = oneshot::channel();

        self.sender.send((req, tx)).unwrap();

        if let LSMManagerResp::AddSlice = rx.await.unwrap()? {
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
}


/// The manager of the LSM tree. Maintains a list of all [`Slice`]s of the LSM tree.
#[derive(Debug)]
pub struct LSMManager {
    /// The path where the different slices are saved at.
    pub path: PathBuf,
    /// The biggest id in use by a [`Slice`]. Get incremented with each newly created [`Slice`].
    pub latest_id: u128,
    /// The number of slices at each level.
    pub slices_per_level: usize,
    /// The store for all levels and the [`Slice`]s it contains.
    pub slices: Vec<Vec<SliceHandle>>,
}

impl LSMManager {
    pub async fn new(
        folder: impl AsRef<Path>,
        slices_per_level: usize,
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
        mut rx: mpsc::UnboundedReceiver<(LSMManagerReq, oneshot::Sender<std::io::Result<LSMManagerResp>>)>,
    ) {
        let (slice_tx, mut slice_rx) = mpsc::unbounded_channel();

        loop {
            tokio::select! {
                req = rx.recv() => {
                    let (req, tx) = match req {
                        Some(req) => req,
                        None => break,
                    };

                    match req {
                        LSMManagerReq::AddSlice(pairs, min_ts, max_ts) => {
                            let resp = self.add_new_slice(pairs, min_ts, max_ts).await
                                .map(|_| LSMManagerResp::AddSlice);

                            tx.send(resp).unwrap();
                        }
                        LSMManagerReq::Get(key, read_ts) => {
                            if !self.can_find_securely(read_ts) {
                                let resp = Err(Error::new(ErrorKind::Other, "The LSM tree can not serve values with such an old read_ts"));

                                tx.send(resp).unwrap();
                                continue;
                            }

                            let get_req = SliceGetReq::new(key, read_ts, tx);

                            self.get(get_req, slice_tx.clone());
                        }
                    }
                }

                resp = slice_rx.recv() => {
                    let resp = resp.unwrap(); // We keep a sender half here, so this is never None.

                    match resp {
                        SliceResp::Get(get_req) => {
                            if let Some(pair) = get_req.pair {
                                let resp = Ok(LSMManagerResp::Get(Some(pair)));

                                get_req.tx.send(resp).unwrap();
                                continue;
                            }
                            if let Some(err) = get_req.error {
                                let resp = Err(err);

                                get_req.tx.send(resp).unwrap();
                                continue;
                            }

                            self.get(get_req, slice_tx.clone());
                        }
                        SliceResp::Merge(_) => {
                            panic!("Implementation error!");
                        }
                        SliceResp::Delete => {
                            panic!("Implementation error!");
                        }
                    }
                }
            }
        }
    }

    /// Checks if we can search for a key with the given read timestamp securely.
    /// Since we delete obselete key values once the newer ones enter the lsm tree, not finding them
    /// in the lsm tree is no guarentee that at the given read timestamp they did not exist.
    /// Given so we must abort transactions early in this case.
    fn can_find_securely(&self, read_ts: u128) -> bool {
        if self.slices.is_empty() {
            return true; // TODO can this happen???
        }

        if self.slices[0].is_empty() {
            return true;
        }

        read_ts >= self.slices[0][0].max_ts
    }

    fn get(
        &self,
        get_req: SliceGetReq,
        slice_tx: mpsc::UnboundedSender<SliceResp>,
    ) {
        // If this request has already visited a slice, find it and proceed to the next.
        if let Some(last_slice_id) = get_req.last_slice_id {
            // If we can not find the last visited slice, we start from the beginning.
            // This may happen if the last searched for slice get merged in the meantime.
            // TODO find a better solution...
            if let Some((idx1, idx2)) = self.find_slice_by_id(last_slice_id) {
                // First check the current level, skipping up to idx2
                if let Some(slice) = self.slices[idx1].get(idx2+1) {
                    slice.get(get_req, slice_tx);
                    return;
                }
                // Check all further levels, taking the first slice we can find.
                // TODO technically it should be impossible for one level to be empty with the
                // next level to have a slice. But this approach seems to be safest for now.
                // We should that the LSM tree works currectly trough...
                let mut idx1 = idx1+1;
                while self.slices.len()>idx1 {
                    if let Some(slice) = self.slices[idx1].get(0) {
                        slice.get(get_req, slice_tx);
                        return;
                    }
                    idx1 = idx1+1;
                }

                // There are no more slices... fall trough to the 'does not exit' case below.
            } else {
                // Find the first available slice and send it the request.
                for list in self.slices.iter() {
                    for slice in list.iter() {
                        slice.get(get_req, slice_tx);
                        return;
                    }
                }
            }
        } else {
            // Find the first available slice and send it the request.
            for list in self.slices.iter() {
                for slice in list.iter() {
                    slice.get(get_req, slice_tx);
                    return;
                }
            }
        }

        // We did not find a slice, so return an empty response.
        let resp = Ok(LSMManagerResp::Get(None));

        get_req.tx.send(resp).unwrap();
    }

    async fn add_new_slice(
        &mut self,
        pairs: Vec<KVPair>,
        min_ts: u128,
        max_ts: u128,
    ) -> std::io::Result<()> {
        // Abort early if we have an empty slice.
        if pairs.is_empty() {
            return Ok(())
        }

        let slice = self.create_slice(pairs, min_ts, max_ts).await?;

        self.add_slice(slice).await
    }

    async fn create_slice(
        &mut self,
        pairs: Vec<KVPair>,
        min_ts: u128,
        max_ts: u128,
    ) -> std::io::Result<Slice> {
        // Get the next id.
        self.latest_id = self.latest_id+1;
        let id = self.latest_id;

        let folder = self.path.clone();
        let level = 0;

        Slice::new(id, level, pairs, min_ts, max_ts, folder).await
    }

    async fn add_slice(
        &mut self,
        slice: Slice,
    ) -> std::io::Result<()> {
        let level = slice.level as usize;

        // Make room for all levels up to the desired level, if necessary.
        // NOTE We use this function also during startup, at which we might encounter slices
        // from higher levels before the level below it might not be initialized.
        while self.slices.len()>level {
            self.slices.push(Vec::with_capacity(self.slices_per_level));
        }

        if self.slices[level-1].len()>=self.slices_per_level {
            self.merge_level(level).await?;
        }

        let slice = slice.run();

        self.slices[level-1].insert(0, slice);

        Ok(())
    }

    async fn merge_level(&mut self, level: usize) -> std::io::Result<()> {
        // Determine the number of slices we are merging.
        let amount = self.slices[level-1].len();

        // Create the channels for retrieving the slices for merging.
        let (slice_tx, mut slice_rx) = mpsc::unbounded_channel();

        // Initialize the collection of MergeSlice's.
        let mut old = Vec::with_capacity(amount);

        // Send the request to all slices.
        for slice in self.slices[level-1].iter() {
            slice.merge(slice_tx.clone());
        }

        // Wait for all slices to answer.
        while amount>old.len() {
            let merge_slice = match slice_rx.recv().await.unwrap() {
                SliceResp::Merge(merge_slice) => merge_slice,
                _ => panic!("Implementation error!"),
            };

            old.push(merge_slice);
        }

        // Purge the level, since all slices are now gone.
        self.slices[level-1].clear();

        // Get the next id.
        self.latest_id = self.latest_id+1;
        let id = self.latest_id;

        // Merge the slices.
        let slice = Slice::merge_multi(old, id, self.path.clone()).await?;
        let slice = slice.run();

        // Add the new slice to the next level.
        match self.slices.get_mut(level) {
            Some(list) => list.insert(0, slice),
            None => {
                let mut list = Vec::with_capacity(self.slices_per_level);
                list.push(slice);
                self.slices.insert(level, list);
            }
        }

        Ok(())
    }

    fn find_slice_by_id(&self, id: u128) -> Option<(usize, usize)> { // (idx 1st vec, idx 2nd vec)
        for idx1 in 0..self.slices.len() {
            for idx2 in 0..self.slices[idx1].len() {
                if self.slices[idx1][idx2].id==id {
                    return Some((idx1, idx2))
                }
            }
        }

        None
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
    pub key: u128,
    pub read_ts: u128,
    pub tx: oneshot::Sender<std::io::Result<LSMManagerResp>>,
    pub pair: Option<KVPair>,
    pub error: Option<Error>,
    pub last_slice_id: Option<u128>,
}

impl SliceGetReq {
    fn new(
        key: u128,
        read_ts: u128,
        tx: oneshot::Sender<std::io::Result<LSMManagerResp>>,
    ) -> Self {
        Self {
            key,
            read_ts,
            tx,
            pair: None,
            error: None,
            last_slice_id: None,
        }
    }
}

#[derive(Debug)]
pub struct SliceHandle {
    id: u128,
    min_ts: u128,
    max_ts: u128,
    sender: mpsc::UnboundedSender<(SliceReq, mpsc::UnboundedSender<SliceResp>)>,
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
        level: u64,
        pairs: Vec<KVPair>,
        min_ts: u128,
        max_ts: u128,
        folder: impl AsRef<Path>,
    ) -> std::io::Result<Self> {
        let path = Self::construct_path(id, folder.as_ref());

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path.clone())
            .await?;

        let len = pairs.len() as u64;

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
        slice.write_body(pairs).await?;

        drop(slice);

        Self::open(id, folder).await
    }

    /// Open an old LSM slice with the given id inside the given folder. Will also extract the
    /// the relevant meta data from the header data at the beginning of the file.
    pub async fn open(id: u128, folder: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = Self::construct_path(id, folder.as_ref());

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

    /// Checks if the slice is fully written, or if it got corrupted due to a crash before writting
    /// finished.
    pub async fn is_fully_written(&mut self) -> std::io::Result<bool> {
        // Determine the size of the file.
        let total = self.file.seek(SeekFrom::End(0)).await?;

        // Determine the expected size. (header + body)
        let expected = Self::HEADER_SIZE + self.len*KVPair::BYTE_SIZE;

        Ok(total==expected)
    }

    fn construct_path(id: u128, folder: &Path) -> PathBuf {
        folder.join(format!("{}.slice", id))
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
        loop {
            let req = rx.recv().await;
            let (req, tx) = match req {
                Some(req) => req,
                None => break,
            };

            match req {
                SliceReq::Get(mut get_req) => {
                    get_req.last_slice_id = Some(self.id);

                    match self.get(get_req.key, get_req.read_ts).await {
                        Ok(resp) => get_req.pair = resp,
                        Err(err) => get_req.error = Some(err),
                    }

                    let resp = SliceResp::Get(get_req);

                    tx.send(resp).unwrap();
                }
                SliceReq::Merge => {
                    let resp = SliceResp::Merge(self.into());

                    tx.send(resp).unwrap();
                    break;
                }
                SliceReq::Delete => {
                    panic!("Implementation error!");
                }
            }
        }
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

    async fn write_body(&mut self, mut pairs: Vec<KVPair>) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(Self::HEADER_SIZE)).await?;

        let mut writer = BufWriter::new(&mut self.file);

        for pair in pairs.drain(..) {
            pair.write(&mut writer).await?;
        }

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

    pub async fn merge_multi(
        mut old: Vec<MergeSlice>,
        id: u128,
        folder: impl AsRef<Path>,
    ) -> std::io::Result<Slice> {
        let path = Self::construct_path(id, folder.as_ref());

        old.sort();

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

        // TODO delete the old slices...

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

impl From<Slice> for MergeSlice {
    fn from(slice: Slice) -> Self {
        Self {
            reader: BufReader::new(slice.file),
            len: slice.len,
            level: slice.level,
            min_ts: slice.min_ts,
            max_ts: slice.max_ts,
            current_pair: KVPair::default(),
            current_pair_nr: 0,
        }
    }
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
    async fn read_next(&mut self) -> std::io::Result<bool> {
        if self.current_pair_nr>=self.len {
            return Ok(true); // signal that we are done.
        }

        self.current_pair_nr = self.current_pair_nr + 1;
        self.current_pair = KVPair::read_nth(&mut self.reader, self.current_pair_nr).await?;

        Ok(false)
    }
}
