//! Contains the definition of the file header used for files created by this crate.
//!
//! Adding a [`MainHeader`] to all files created and maintained by this library helps in parsing
//! the files correctly over multiple versions of this crate.
//!
//! Individual type of files may contain an additional header of varying size containing more meta
//! data as appropriate.
//!
//! The objects contained hearin are for internal usage only. You will not need them while using this
//! library.
//!
//! You will only need them when inspecting the files of this library directly, for example when
//! checking for file corruptions on your own.
use crate::*;



/// A magic value added to the beginning of all files on disk to indicate that they belong to this crate.
///
/// Gets used to verify that a certain file read from the disk does indeed belong to this crate.
///
/// Its binary representation in hexadecimal form looks like this:
///
/// ```text
/// [00, 42, 42, 42, 42, 42, 42, 42]
/// ```
pub static HORST_MAGIC: u64 = 66 + (66<<8) + (66<<16) + (66<<24) + (66<<32) + (66<<40) + (66<<48);


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum FileType {
    VLog = 1,
    LSMSlice = 2,
}

impl TryFrom<u16> for FileType {
    type Error = Error;

    fn try_from(file_type: u16) -> Result<Self, Self::Error> {
        match file_type {
            1 => Ok(Self::VLog),
            2 => Ok(Self::LSMSlice),
            _ => Err(Error::new(ErrorKind::InvalidData, "Unknown file type!")),
        }
    }
}


/// The header of all files created by this crate.
pub struct MainHeader {
    pub file_type: FileType,
    pub header_version: u16,
    pub header_size: u64,
}

/// Returns the static disk size (20).
impl DiskSize for MainHeader {
    const STATIC_SIZE: u64 = 8 + 2 + 2 + 8;
}

#[async_trait]
impl SaveToDisk for MainHeader {
    async fn save_to_disk<W: AsyncWriteExt + Send + Sync + Unpin>(
        &self,
        disk: &mut W,
    ) -> std::io::Result<()> {
        disk.write_all(&HORST_MAGIC.to_be_bytes()).await?;
        disk.write_all(&(self.file_type as u16).to_be_bytes()).await?;
        disk.write_all(&self.header_version.to_be_bytes()).await?;
        disk.write_all(&self.header_size.to_be_bytes()).await
    }
}

#[async_trait]
impl LoadFromDisk for MainHeader {
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
        let magic: [u8; 8] = buffer[..8].try_into().unwrap();
        let magic = u64::from_be_bytes(magic);
        let file_type: [u8; 2] = buffer[8..10].try_into().unwrap();
        let file_type = u16::from_be_bytes(file_type);
        let file_type: FileType = file_type.try_into()?;
        let header_version: [u8; 2] = buffer[10..12].try_into().unwrap();
        let header_version = u16::from_be_bytes(header_version);
        let header_size: [u8; 8] = buffer[12..].try_into().unwrap();
        let header_size = u64::from_be_bytes(header_size);

        // Make sure the values are correct.
        if magic!=HORST_MAGIC {
            return Err(Error::new(ErrorKind::InvalidData, "Wrong magic number at the beginning of the file."));
        }

        Ok(Self {
            file_type,
            header_version,
            header_size,
        })
    }
}
