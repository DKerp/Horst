use crate::*;



mod slice;
pub use slice::*;

mod manager;
pub use manager::*;

mod ram;
pub use ram::*;



#[derive(Debug, Eq, Clone, Copy, Default)]
pub struct KVPair {
    /// The key.
    pub key: u128,

    /// The commit timestamp of this version.
    pub version: u128,

    /// The id of the value log file where the value is saved.
    pub vlog_id: u128,
    /// The offset within the value log file where the value is saved.
    pub vlog_offset: u64,
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

/// Returns the static disk size (56).
impl DiskSize for KVPair {
    const STATIC_SIZE: u64 = 16 + 16 + 16 + 8;
}

#[async_trait]
impl SaveToDisk for KVPair {
    async fn save_to_disk<W: AsyncWriteExt + Send + Sync + Unpin>(
        &self,
        disk: &mut W,
    ) -> std::io::Result<()> {
        disk.write_all(&self.key.to_be_bytes()).await?;
        disk.write_all(&self.version.to_be_bytes()).await?;
        disk.write_all(&self.vlog_id.to_be_bytes()).await?;
        disk.write_all(&self.vlog_offset.to_be_bytes()).await
    }
}

#[async_trait]
impl LoadFromDisk for KVPair {
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

        // Parse the pair.
        let key: [u8; 16] = buffer[..16].try_into().unwrap();
        let key = u128::from_be_bytes(key);
        let version: [u8; 16] = buffer[16..32].try_into().unwrap();
        let version = u128::from_be_bytes(version);
        let vlog_id: [u8; 16] = buffer[32..48].try_into().unwrap();
        let vlog_id = u128::from_be_bytes(vlog_id);
        let vlog_offset: [u8; 8] = buffer[48..].try_into().unwrap();
        let vlog_offset = u64::from_be_bytes(vlog_offset);

        Ok(Self {
            key,
            version,
            vlog_id,
            vlog_offset,
        })
    }
}

impl KVPair {
    pub(crate) fn from_key_and_version(key: u128, version: u128) -> Self {
        Self {
            key,
            version,
            vlog_id: 0,
            vlog_offset: 0,
        }
    }
}
