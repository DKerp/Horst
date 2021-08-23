use crate::*;



/// The size of written [`VLog`] entry header. Contains the key, the version as well as the value's length in bytes.
pub const VLOG_HEADER_SIZE: usize = 24; // 16 (key) + 8 (value length)
/// The size of a vlog transaction header.
pub const VLOG_TXN_HEADER_SIZE: u64 = 40; // 8 (header, s.b.) + 16 (version) + 16 (total length of entries)

/// The key sentence which gets added at the beginning of each transaction inside the value log file.
pub static VLOG_TXN_BEGIN: &[u8; 8] = b"New TXN!";



#[derive(Debug)]
pub enum VLogReq {
    /// Write a list of key-value pairs, each having a certain write timestamp/version.
    WriteTxn(Vec<(u128, Vec<u8>)>, u128),
    /// Read a certain key with a certain write timestamp/version from the log, which is located at
    /// a certain offset.
    ReadValue(u128, u64),
}

#[derive(Debug)]
pub enum VLogResp {
    /// A write response, containing a list of key-value pairs, including file and offset information.
    WriteTxn(Vec<KVPair>),
    /// A read response containing the value belonging to the requested key and version.
    ReadValue(Vec<u8>),
}

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
        pairs: Vec<(u128, Vec<u8>)>,
        version: u128,
    ) -> std::io::Result<Vec<KVPair>> {
        let req = VLogReq::WriteTxn(pairs, version);

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
        // version: u128,
        offset: u64,
    ) -> std::io::Result<Vec<u8>> {
        let req = VLogReq::ReadValue(key, offset);

        let (tx, rx) = oneshot::channel();

        self.sender.send((req, tx)).unwrap();

        if let VLogResp::ReadValue(resp) = rx.await.unwrap()? {
            return Ok(resp)
        }

        panic!("Implementation error!");
    }
}


pub struct VLog {
    /// The unique id of this value log.
    pub id: u128,

    /// The underlying file on the harddisk.
    pub file: fs::File,

    /// A buffer for reading serialized key value pairs.
    pub buffer: Vec<u8>,
}

impl VLog {
    pub const BUFFER_SIZE: usize = 40;

    /// Create a new VLog with the given id inside the given folder.
    pub async fn new(id: u128, folder: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = folder.as_ref().join(format!("{}.vlog", id));

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .await?;

        let buffer = vec![0u8; Self::BUFFER_SIZE];

        Ok(Self {
            id,
            file,
            buffer,
        })
    }

    /// Open an old VLog with the given id inside the given folder.
    pub async fn open(id: u128, folder: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = folder.as_ref().join(format!("{}.vlog", id));

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .await?;

        let buffer = vec![0u8; Self::BUFFER_SIZE];

        Ok(Self {
            id,
            file,
            buffer,
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
                        VLogReq::WriteTxn(pairs, version) => {
                            // We buffer them for more efficient use of the file write buffer.
                            txn_buffer.push((pairs, version, tx));
                            continue;
                        }
                        VLogReq::ReadValue(key, offset) => {
                            self.read_value(key, offset).await.map(|resp| VLogResp::ReadValue(resp))
                        }
                    };

                    tx.send(resp).unwrap();
                }
                _ = interval.tick() => {
                    // log::info!("VLog - next tick. txn_buffer.len(): {}", txn_buffer.len());

                    for (pairs, version, tx) in txn_buffer.drain(..) {
                        let resp = self.write_txn(pairs, version).await.map(|resp| VLogResp::WriteTxn(resp));

                        tx.send(resp).unwrap();
                    }
                }
            }
        }
    }


    /// Write a key/value pair to the disk and return a disciption of it linked with the VLog entry.
    pub async fn write_txn(
        &mut self,
        mut pairs: Vec<(u128, Vec<u8>)>,
        version: u128,
    ) -> std::io::Result<Vec<KVPair>> {

        // Collect the necessary value log info for finding the value later again.
        let vlog_id = self.id;
        let mut vlog_offset = self.file.seek(SeekFrom::End(0)).await?;

        // Calculate the total size of the transaction data (excluding the transaction header);
        let mut total = (pairs.len()*VLOG_HEADER_SIZE) as u128; // The entry header data.
        // Add the length of all values.
        for (_, value) in pairs.iter() {
            total = total + (value.len() as u128);
        }

        // Prepare the BufWriter and the store for the return value.
        let mut writer = BufWriter::new(&mut self.file);
        let mut store = Vec::with_capacity(pairs.len());

        // Write the transaction header data.
        writer.write_all(VLOG_TXN_BEGIN).await?;
        writer.write_all(&version.to_be_bytes()).await?;
        writer.write_all(&total.to_be_bytes()).await?;

        // Adjust the vlog offset.
        vlog_offset = vlog_offset+VLOG_TXN_HEADER_SIZE;

        // Write all the entries.
        for (key, value) in pairs.drain(..) {

            /* Write the header into the value log. */

            // Write the key itself.
            writer.write_all(&key.to_be_bytes()).await?;

            // Write the version.
            // writer.write_all(&version.to_be_bytes()).await?;

            // Write the value length.
            let value_len = value.len() as u64;
            writer.write_all(&value_len.to_be_bytes()).await?;

            /* Write the value into the value log. */

            // Write the value.
            writer.write_all(&value[..]).await?;

            /* Everything was succesfull. Construct the response. */

            // We only save the value directly if it is small enough.
            // let value = if 128>value.len() {
            //     None
            // } else {
            //     Some(value)
            // };

            let pair = KVPair {
                key,
                version,
                vlog_id,
                vlog_offset,
                // value,
            };

            // Adjust the vlog offset for the next entry.
            vlog_offset = vlog_offset + (VLOG_HEADER_SIZE as u64) + value_len;

            store.push(pair);
        }

        writer.flush().await?;

        Ok(store)
    }

    /// Try to read a certain value from the VLog. Will verify that is the correct value.
    pub async fn read_value(
        &mut self,
        key: u128,
        // version: u128,
        offset: u64,
    ) -> std::io::Result<Vec<u8>> {
        // Seek to the position.
        self.file.seek(SeekFrom::Start(offset)).await?;

        // Read and parse the header.
        let n = self.file.read_exact(&mut self.buffer[..VLOG_HEADER_SIZE]).await?;
        if n!=VLOG_HEADER_SIZE {
            return Err(Error::new(ErrorKind::Other, "read_exact did not return the specified amount of bytes."));
        }

        // Parse the header.
        let (parsed_key, value_len) = self.parse_header()?;

        // Make sure we have the right entry.
        if key!=parsed_key {
            return Err(Error::new(ErrorKind::Other, "VLog corruption: key is different!"));
        }
        // if version!=parsed_version {
        //     return Err(Error::new(ErrorKind::Other, "VLog corruption: version is different!"));
        // }

        // Make sure that value_len is reasonable.
        if value_len>(1<<20) {
            return Err(Error::new(ErrorKind::Other, "VLog corruption: value_len is too big!"));
        }

        let mut value = vec![0u8; value_len];

        // Read the value.
        let n = self.file.read_exact(&mut value[..]).await?;
        if n!=value_len {
            return Err(Error::new(ErrorKind::Other, "read_exact did not return the specified amount of bytes."));
        }

        Ok(value)
    }

    pub fn parse_header(&self) -> std::io::Result<(u128, usize)> {

        let key: [u8; 16] = self.buffer[..16].try_into().unwrap();
        let key = u128::from_be_bytes(key);

        // let version: [u8; 16] = self.buffer[16..32].try_into().unwrap();
        // let version = u128::from_be_bytes(version);

        let value_len: [u8; 8] = self.buffer[16..24].try_into().unwrap();
        let value_len = u64::from_be_bytes(value_len) as usize;

        Ok((key, value_len))
    }
}
