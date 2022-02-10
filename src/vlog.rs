use crate::*;



/// The current biggest size of a single VLog value. (1MB)
pub const VLOG_VALUE_MAX: u64 = 1<<20;

/// The key sentence which gets added at the beginning of each transaction inside the value log file.
pub static VLOG_TXN_BEGIN: &[u8; 8] = b"New TXN!";



#[derive(Default, Clone, Debug)]
pub struct VLogHeader {
    pub id: u128,
}

impl Version for VLogHeader {
    const VERSION: u16 = 1;
}

/// A transaction as saved inside the value log.
#[derive(Default, Clone, Debug)]
pub struct VLogTxn {
    pub header: VLogTxnHeader,
    pub body: Vec<VLogEntry>,
}

impl VLogTxn {
    pub fn new(
        write_ts: u128,
        body: Vec<VLogEntry>,
    ) -> Self {
        let mut total_len = 0u64;
        for entry in body.iter() {
            total_len += entry.disk_size();
        }

        let header = VLogTxnHeader {
            write_ts,
            total_len,
        };

        Self {
            header,
            body,
        }
    }
}

/// The header of a transaction as saved inside the value log.
#[derive(Default, Clone, Debug)]
pub struct VLogTxnHeader {
    /// The timestamp of this transaction.
    pub write_ts: u128,
    /// The total size in bytes of the key/value pairs set in this transaction.
    pub total_len: u64,
}

#[derive(Default, Clone, Debug)]
pub struct VLogEntry {
    pub key: u128,
    pub value: Vec<u8>,
}


/// Returns the static disk size (16).
impl DiskSize for VLogHeader {
    // id (u128)
    const STATIC_SIZE: u64 = 16;
}

#[async_trait]
impl SaveToDisk for VLogHeader {
    async fn save_to_disk<W: AsyncWriteExt + Send + Sync + Unpin>(
        &self,
        disk: &mut W,
    ) -> std::io::Result<()> {
        disk.write_all(&self.id.to_be_bytes()).await
    }
}

#[async_trait]
impl LoadFromDisk for VLogHeader {
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
        let id: [u8; 16] = buffer[..16].try_into().unwrap();
        let id = u128::from_be_bytes(id);

        Ok(Self {
            id,
        })
    }
}

/// Returns the static disk size (32).
impl DiskSize for VLogTxnHeader {
    // VLOG_TXN_BEGIN ([u8; 8] + write_tx (u128) + total_len (u64)
    const STATIC_SIZE: u64 = 8 + 16 + 8;
}

#[async_trait]
impl SaveToDisk for VLogTxnHeader {
    async fn save_to_disk<W: AsyncWriteExt + Send + Sync + Unpin>(
        &self,
        disk: &mut W,
    ) -> std::io::Result<()> {
        disk.write_all(VLOG_TXN_BEGIN).await?;
        disk.write_all(&self.write_ts.to_be_bytes()).await?;
        disk.write_all(&self.total_len.to_be_bytes()).await
    }
}

#[async_trait]
impl LoadFromDisk for VLogTxnHeader {
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
        if &buffer[..8]!=VLOG_TXN_BEGIN {
            return Err(Error::new(ErrorKind::InvalidData, "Wrong key word at the beginning of the transaction."));
        }
        let write_ts: [u8; 16] = buffer[8..24].try_into().unwrap();
        let write_ts = u128::from_be_bytes(write_ts);
        let total_len: [u8; 8] = buffer[24..].try_into().unwrap();
        let total_len = u64::from_be_bytes(total_len);

        Ok(Self {
            write_ts,
            total_len,
        })
    }
}

/// Returns the static disk size, excluding the value (24).
impl DiskSize for VLogEntry {
    // key (u128) + value.len() (usize/u64)
    const STATIC_SIZE: u64 = 16 + 8;

    fn disk_size(&self) -> u64 {
        Self::STATIC_SIZE + (self.value.len() as u64)
    }
}

#[async_trait]
impl SaveToDisk for VLogEntry {
    async fn save_to_disk<W: AsyncWriteExt + Send + Sync + Unpin>(
        &self,
        disk: &mut W,
    ) -> std::io::Result<()> {
        disk.write_all(&self.key.to_be_bytes()).await?;
        let value_len = self.value.len() as u64;
        disk.write_all(&value_len.to_be_bytes()).await?;
        disk.write_all(&self.value[..]).await
    }
}

#[async_trait]
impl LoadFromDisk for VLogEntry {
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
        let key: [u8; 16] = buffer[..16].try_into().unwrap();
        let key = u128::from_be_bytes(key);
        let value_len: [u8; 8] = buffer[16..].try_into().unwrap();
        let value_len = u64::from_be_bytes(value_len);

        // Make sure that value_len is reasonable.
        if value_len>VLOG_VALUE_MAX {
            return Err(Error::new(ErrorKind::Other, "VLog corruption: value_len is too big!"));
        }

        // Now read the value from disk.
        let mut value = vec![0u8; value_len as usize];
        disk.read_exact(&mut value).await?;

        Ok(Self {
            key,
            value,
        })
    }
}



pub struct VLogFile {
    /// The unique id of this value log.
    pub id: u128,
    /// The underlying file on the harddisk.
    pub file: fs::File,
    /// The path to the file.
    pub path: PathBuf,
    /// A buffer for reading serialized key value pairs.
    pub buffer: Vec<u8>,
}

impl VLogFile {
    /// Create a new VLogFile with the given id inside the given folder.
    pub async fn new(id: u128, folder: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = folder.as_ref().join(format!("{}.vlog", id));

        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;

        let header = VLogHeader {
            id,
        };

        let main_header = MainHeader {
            file_type: FileType::VLog,
            header_version: VLogHeader::VERSION,
            header_size: header.disk_size(),
        };

        main_header.save_to_disk(&mut file).await?;
        header.save_to_disk(&mut file).await?;

        drop(file); // Just to be sure...

        Self::open(path).await
    }

    /// Open an old VLogFile with the given id inside the given folder.
    pub async fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref();

        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .await?;

        let main_header = MainHeader::load_from_disk(&mut file).await?;
        if main_header.file_type!=FileType::VLog {
            return Err(Error::new(ErrorKind::InvalidData, "Invalid VLog file found. File header indicates the wrong file type."));
        }

        let header = VLogHeader::load_from_disk(&mut file).await?;
        let id = header.id;

        let buffer = vec![0u8; VLogEntry::STATIC_SIZE as usize];

        let path = path.to_path_buf();

        Ok(Self {
            id,
            file,
            path,
            buffer,
        })
    }

    /// Write a key/value pair to the disk and return a disciption of it linked with the VLog entry.
    pub async fn write_txn_multi(
        &mut self,
        txn_buffer: Vec<(VLogTxn, oneshot::Sender<std::io::Result<VLogResp>>)>,
    ) -> std::io::Result<()> {

        // Collect the necessary value log info for finding the value later again.
        let vlog_id = self.id;
        let mut vlog_offset = self.file.seek(SeekFrom::End(0)).await?;

        // Prepare the BufWriter and the store for the return value.
        let mut writer = BufWriter::new(&mut self.file);

        for (txn, tx) in txn_buffer {
            let resp = Self::write_txn(
                &mut writer,
                txn,
                &mut vlog_offset,
                vlog_id,
            ).await.map(|resp| VLogResp::WriteTxn(resp));

            tx.send(resp).unwrap();
        }

        writer.flush().await?;

        Ok(())
    }

    async fn write_txn<W: AsyncWriteExt + Send + Sync + Unpin>(
        writer: &mut W,
        txn: VLogTxn,
        vlog_offset: &mut u64,
        vlog_id: u128,
    ) -> std::io::Result<Vec<KVPair>> {
        let mut store = Vec::with_capacity(txn.body.len());

        // Save the header.
        txn.header.save_to_disk(writer).await?;
        (*vlog_offset) += txn.header.disk_size();

        // Save the body and construct the KVPairs for the LSM.
        for entry in txn.body.iter() {
            // Save the entry on disk.
            entry.save_to_disk(writer).await?;

            // Construct and add the KVPair.
            store.push(KVPair {
                key: entry.key,
                version: txn.header.write_ts,
                vlog_id,
                vlog_offset: *vlog_offset,
            });

            (*vlog_offset) += entry.disk_size();
        }

        Ok(store)
    }

    /// Try to read a certain value from the VLog. Will verify that is the correct value.
    pub async fn read_value(
        &mut self,
        key: u128,
        offset: u64,
    ) -> std::io::Result<Vec<u8>> {
        // Seek to the position.
        self.file.seek(SeekFrom::Start(offset)).await?;

        // Read and parse the header.
        let entry = VLogEntry::load_from_disk_with_buffer(
            &mut self.file,
            &mut self.buffer,
        ).await?;

        // Make sure we have the right entry.
        if key!=entry.key {
            return Err(Error::new(ErrorKind::InvalidData, "VLog corruption: key is different!"));
        }

        Ok(entry.value)
    }
}



#[derive(Debug)]
pub enum VLogReq {
    /// Write a list of key-value pairs, each having a certain write timestamp/version.
    WriteTxn(VLogTxn),
    /// Read a certain key with a certain write timestamp/version from the log, which is located at
    /// a certain offset.
    ReadValue(u128, u128, u64), // key, vlog_id, offset
}

#[derive(Debug)]
pub enum VLogResp {
    /// A write response, containing a list of key-value pairs, including file and offset information.
    WriteTxn(Vec<KVPair>),
    /// A read response containing the value belonging to the requested key and version.
    ReadValue(Vec<u8>),
}

#[derive(Clone, Debug)]
pub struct VLogHandle {
    sender: mpsc::UnboundedSender<(VLogReq, oneshot::Sender<std::io::Result<VLogResp>>)>,
}

impl From<mpsc::UnboundedSender<(VLogReq, oneshot::Sender<std::io::Result<VLogResp>>)>> for VLogHandle {
    fn from(sender: mpsc::UnboundedSender<(VLogReq, oneshot::Sender<std::io::Result<VLogResp>>)>) -> Self {
        Self {
            sender,
        }
    }
}

impl VLogHandle {
    /// Write a list key/value pair to the disk and return a desciption of them linked with the VLog entry.
    pub async fn write_txn(
        &self,
        txn: VLogTxn
    ) -> std::io::Result<Vec<KVPair>> {
        let req = VLogReq::WriteTxn(txn);

        let (tx, rx) = oneshot::channel();

        self.sender.send((req, tx)).unwrap();

        if let VLogResp::WriteTxn(resp) = rx.await.unwrap()? {
            return Ok(resp)
        }

        panic!("Implementation error!");
    }

    /// Try to read a certain value from the VLog. Will verify that it is the correct value.
    pub async fn read_value(
        &self,
        key: u128,
        vlog_id: u128,
        offset: u64,
    ) -> std::io::Result<Vec<u8>> {
        let req = VLogReq::ReadValue(key, vlog_id, offset);

        let (tx, rx) = oneshot::channel();

        self.sender.send((req, tx)).unwrap();

        if let VLogResp::ReadValue(resp) = rx.await.unwrap()? {
            return Ok(resp)
        }

        panic!("Implementation error!");
    }
}


pub struct VLog {
    /// The folder where all vlog files are being saved at.
    pub folder: PathBuf,
    /// The highest id given to any VLogFile.
    pub last_id: u128,

    /// All available value log files.
    pub value_logs: BTreeMap<u128, VLogFile>,
}

impl VLog {
    /// Create a new VLog instance which takes care of all VLogFile`s.
    pub async fn new(config: &Config) -> std::io::Result<Self> {
        let folder = config.vlog_folder.clone();

        // Check if the folder exists. If not, try to create it.
        if !folder.exists() {
            fs::create_dir_all(&folder).await?;
        }

        // Init the last_id value.
        // The correct value gets determined by evaluating all available value logs.
        let mut last_id = 0u128;

        // Init the store for the value logs.
        let mut value_logs = BTreeMap::new();

        // Open all available VLogFile`s.
        // NOTE since this an initialization method we use the sync version.
        for entry in std::fs::read_dir(&folder)? {
            let entry = entry?;

            let path = entry.path();

            // Skip sub folders.
            if path.is_dir() {
                log::warn!("Found an unexpected subfolder {:?} in the vlog directory {:?}. Skipping.", path, folder);
                continue;
            }

            // All folder entries will have a proper extension, since they can not end in `..`.
            if let Some(ext) = path.extension() {
                if ext!="vlog" {
                    log::warn!("Found a file in the vlogs folder {:?} which was not a vlog: {:?}. Skipping. ext: {:?}", folder, path, ext);
                    continue;
                }
            }

            let vlog_file = VLogFile::open(&path).await?;
            let vlog_id = vlog_file.id;
            last_id = last_id.max(vlog_id);

            if let Some(duplicate) = value_logs.insert(vlog_id, vlog_file) {
                log::error!(
                    "Found two vlog files with the same id! id: {}, path: {:?}, path_duplicate: {:?}",
                    vlog_id, path, duplicate.path,
                );
                return Err(Error::new(ErrorKind::InvalidData, "Found two vlog files with the same id."));
            }
        }

        // Open a new value log if there is none yet.
        if value_logs.is_empty() {
            last_id += 1;
            let vlog_id = last_id;
            let vlog_file = VLogFile::new(vlog_id, &folder).await?;

            value_logs.insert(vlog_id, vlog_file);
        }

        Ok(Self {
            folder,
            last_id,
            value_logs,
        })
    }

    /// Run the VLog in a seperate task and return a handle to it.
    pub fn run(self) -> VLogHandle {
        let (tx, rx) = mpsc::unbounded_channel();

        spawn(async move {
            self.run_inner(rx).await;
        });

        tx.into()
    }

    async fn run_inner(mut self, mut rx: mpsc::UnboundedReceiver<(VLogReq, oneshot::Sender<std::io::Result<VLogResp>>)>) {
        let mut txn_buffer = Vec::with_capacity(10_000);

        let mut interval = tokio::time::interval(Duration::from_millis(100)); // Tick 10 times per second.

        loop {
            tokio::select! {
                req = rx.recv() => {
                    let (req, tx) = match req {
                        Some(req) => req,
                        None => break,
                    };

                    let resp = match req {
                        VLogReq::WriteTxn(txn) => {
                            // We buffer them for more efficient use of the file write buffer.
                            txn_buffer.push((txn, tx));
                            continue;
                        }
                        VLogReq::ReadValue(key, vlog_id, offset) => {
                            self.read_value(key, vlog_id, offset).await.map(|resp| VLogResp::ReadValue(resp))
                        }
                    };

                    tx.send(resp).unwrap();
                }
                _ = interval.tick() => {
                    // log::info!("VLog - next tick. txn_buffer.len(): {}", txn_buffer.len());

                    if txn_buffer.is_empty() {
                        continue;
                    }

                    let txn_buffer = txn_buffer.split_off(0);

                    if let Err(err) = self.write_txn(txn_buffer).await {
                        log::error!("Writing batched transactions failed! err: {:?}", err);
                    }
                }
            }
        }
    }


    /// Write a key/value pair to the disk and return a desciption of it linked with the VLog entry.
    pub async fn write_txn(
        &mut self,
        txn_buffer: Vec<(VLogTxn, oneshot::Sender<std::io::Result<VLogResp>>)>,
    ) -> std::io::Result<()> {
        let vlog_file = self.value_logs.iter_mut()
            .map(|(_key, value)| value)
            .next_back()
            .ok_or_else(|| {
                log::error!("There are no VLog files open!");

                Error::new(ErrorKind::Other, "There are currently no VLog files open.")
            })?;

        vlog_file.write_txn_multi(txn_buffer).await
    }

    /// Try to read a certain value from the VLog. Will verify that is the correct value.
    pub async fn read_value(
        &mut self,
        key: u128,
        vlog_id: u128,
        offset: u64,
    ) -> std::io::Result<Vec<u8>> {
        let vlog_file = self.value_logs.get_mut(&vlog_id).ok_or_else(|| {
            log::error!("Could not find value log. id: {}, key: {}, offset: {}", vlog_id, key, offset);

            Error::new(ErrorKind::NotFound, "The requested VLog could not be found.")
        })?;

        vlog_file.read_value(key, offset).await
    }
}
