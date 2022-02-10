use crate::*;



#[derive(Debug)]
pub(crate) struct CachedPage {
    pub buffer: Vec<u8>,
    pub filled: usize,
    pub hit_counter: u128,
}

impl CachedPage {
    pub fn is_filled(&self) -> bool {
        self.filled>=self.buffer.len()
    }

    pub fn get_write_buf(&mut self) -> &mut [u8] {
        let begin = self.filled.min(self.buffer.len()-1);

        &mut self.buffer[begin..]
    }

    pub fn advance_filled(&mut self, amount: usize) {
        self.filled += amount;
    }
}

#[derive(Debug)]
pub(crate) struct CachingReader<R> {
    inner: R,
    store: BTreeMap<u64, CachedPage>,
    page_size: u64,
    total_size: u64,
    hit_counter: u128,
    current_pos: u64,
    current_inner_pos: u64,
    seek_in_progress: bool,
}

impl<R> CachingReader<R>
where
    R: AsyncRead + AsyncSeek + Send + Sync + Unpin + Sized,
{
    pub async fn new(
        mut inner: R,
        page_size: u64,
    ) -> std::io::Result<Self> {
        let total_size = inner.seek(SeekFrom::End(0)).await?;
        let current_pos = inner.seek(SeekFrom::Start(0)).await?;
        let current_inner_pos = current_pos;

        let store = BTreeMap::new();
        let page_size = page_size.max(1024).min(1024*1024);

        Ok(Self {
            inner,
            store,
            total_size,
            page_size,
            hit_counter: 0,
            current_pos,
            current_inner_pos,
            seek_in_progress: false,
        })
    }

    pub fn into_inner(self) -> R {
        self.inner
    }

    fn is_eof(&self) -> bool {
        self.current_pos>=self.total_size
    }

    fn get_current_page_for_read(&mut self) -> Option<&[u8]> {
        let nr = self.current_pos/self.page_size;
        let offset = (self.current_pos%self.page_size) as usize;

        let page = self.store.get_mut(&nr)?;
        if !page.is_filled() {
            return None;
        }

        page.hit_counter += 1;
        self.hit_counter += 1;

        let offset = offset.min(page.buffer.len()-1);

        Some(&page.buffer[offset..])
    }

    fn fill_current_page(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Option<Poll<std::io::Result<()>>> {
        let nr = self.current_pos/self.page_size;

        let page = match self.store.get_mut(&nr) {
            Some(page) => page,
            None => {
                // First seek to the correct position to read the page.
                let pos = self.page_size*nr;
                match self.seek_inner(
                    pos,
                    cx,
                ) {
                    Poll::Ready(Ok(())) => (),
                    poll => return Some(poll), // returns both pending and error.
                }

                // Create the new page.
                self.create_page(nr);
                // Can not fail because it got just created.
                self.store.get_mut(&nr).unwrap()
            }
        };

        loop {
            let buf = page.get_write_buf();
            if buf.is_empty() {
                break;
            }

            let mut buf = ReadBuf::new(buf);

            let result = match AsyncRead::poll_read(
                Pin::new(&mut self.inner),
                cx,
                &mut buf,
            ) {
                Poll::Ready(result) => result,
                Poll::Pending => return Some(Poll::Pending),
            };

            if let Err(err) = result {
                return Some(Poll::Ready(Err(err)));
            }

            let amount = buf.filled().len();

            log::debug!("CachingReader.fill_current_page - nr: {}, amount: {}", nr, amount);

            if amount==0 {
                return Some(Poll::Ready(Err(Error::new(ErrorKind::UnexpectedEof, "Page could not be fully loaded."))));
            }

            page.advance_filled(amount);
        }

        None
    }

    fn create_page(&mut self, nr: u64) {
        let size = (self.total_size-self.current_pos).min(self.page_size);

        log::debug!("CachingReader.create_page - nr: {}, size: {}", nr, size);

        let buffer = vec![0u8; size as usize];

        let page = CachedPage {
            buffer,
            filled: 0,
            hit_counter: 0,
        };

        self.store.insert(nr, page);
    }

    fn seek_inner(
        &mut self,
        pos: u64,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        log::debug!("seek_inner called.");

        let mut i = 1u32;
        loop {
            log::debug!("seek_inner - loop iteration {}", i);
            i += 1;

            // log::warn!("seek_inner - seek was in progress...");

            let result = match AsyncSeek::poll_complete(
                Pin::new(&mut self.inner),
                cx,
            ) {
                Poll::Ready(result) => result,
                Poll::Pending => return Poll::Pending,
            };

            let new_pos = match result {
                Ok(new_pos) => new_pos,
                Err(err) => return Poll::Ready(Err(err)),
            };

            self.current_inner_pos = new_pos;

            if self.seek_in_progress {
                self.seek_in_progress = false;
                log::debug!("seek_inner - Seek is no longer in progress!");

                if new_pos!=pos {
                    return Poll::Ready(Err(Error::new(ErrorKind::Other, "Seek ended in the wrong position.")));
                }

                return Poll::Ready(Ok(()));
            }

            log::debug!("seek_inner - Starting to seek... pos: {}", pos);
            if let Err(err) = AsyncSeek::start_seek(
                Pin::new(&mut self.inner),
                SeekFrom::Start(pos),
            ) {
                return Poll::Ready(Err(err));
            }

            self.seek_in_progress = true;
            log::debug!("seek_inner - Seek is NOW in progress!");
        }
    }

    fn advance_pos(&mut self, amount: u64) {
        self.current_pos += amount;
    }
}

impl<R> AsyncRead for CachingReader<R>
where
    R: AsyncRead + AsyncSeek + Send + Sync + Unpin + Sized,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>
    ) -> Poll<std::io::Result<()>> {
        let reader = self.get_mut();

        if reader.is_eof() {
            return Poll::Ready(Ok(()));
        }

        if let Some(page_buffer) = reader.get_current_page_for_read() {
            log::debug!("CachingReader.poll_read - page_buffer.len(): {}", page_buffer.len());

            let amount = buf.remaining().min(page_buffer.len());

            buf.put_slice(&page_buffer[..amount]);

            reader.advance_pos(amount as u64);

            return Poll::Ready(Ok(()));
        }

        if let Some(poll) = reader.fill_current_page(cx) {
            return poll;
        }

        if let Some(page_buffer) = reader.get_current_page_for_read() {
            log::debug!("CachingReader.poll_read - page_buffer.len() (2): {}", page_buffer.len());
            let amount = buf.remaining().min(page_buffer.len());

            buf.put_slice(&page_buffer[..amount]);

            reader.advance_pos(amount as u64);

            return Poll::Ready(Ok(()));
        }

        Poll::Ready(Err(Error::new(ErrorKind::Other, "Implementation error.")))
    }
}

impl<R> AsyncSeek for CachingReader<R>
where
    R: AsyncRead + AsyncSeek + Send + Sync + Unpin + Sized,
{
    fn start_seek(
        self: Pin<&mut Self>,
        position: SeekFrom,
    ) -> std::io::Result<()> {
        log::debug!("CachingReader.poll_seek - position: {:?}", position);

        let reader = self.get_mut();

        match position {
            SeekFrom::Start(pos) => {
                reader.current_pos = pos.min(reader.total_size);
            }
            SeekFrom::End(dif) => {
                let dif = dif.min(0);

                let dif: u64 = dif.abs().try_into().unwrap();

                if dif>reader.total_size {
                    return Err(Error::new(ErrorKind::InvalidData, "You can not seek into a negative position."));
                }

                reader.current_pos = reader.total_size-dif;
            }
            SeekFrom::Current(dif) => {
                if 0>dif {
                    let dif: u64 = dif.abs().try_into().unwrap();

                    if dif>reader.current_pos {
                        return Err(Error::new(ErrorKind::InvalidData, "You can not seek into a negative position."));
                    }

                    reader.current_pos -= dif;
                } else {
                    let dif: u64 = dif.try_into().unwrap();

                    let new_pos = reader.current_pos.checked_add(dif)
                        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "Your seek would produce an position overflow."))?;

                    reader.current_pos = new_pos.min(reader.total_size);
                }
            }
        }

        Ok(())
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<u64>> {
        let pos = self.get_mut().current_pos;

        log::debug!("CachingReader.poll_complete - pos: {}", pos);

        Poll::Ready(Ok(pos))
    }
}



/// Utility structure for implementing some costum logging for debugging purposes.
#[derive(Debug)]
pub(crate) struct FileContainer {
    file: fs::File,
}

impl FileContainer {
    pub fn new(file: fs::File) -> Self {
        Self {
            file
        }
    }

    pub fn into_inner(self) -> fs::File {
        self.file
    }
}

impl AsyncRead for FileContainer {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>
    ) -> Poll<std::io::Result<()>> {
        log::debug!("FileContainer.poll_read triggered");

        AsyncRead::poll_read(
            Pin::new(&mut self.get_mut().file),
            cx,
            buf,
        )
    }
}

impl AsyncSeek for FileContainer {
    fn start_seek(
        self: Pin<&mut Self>,
        position: SeekFrom,
    ) -> std::io::Result<()> {
        log::debug!("FileContainer.poll_seek - position: {:?}", position);

        AsyncSeek::start_seek(
            Pin::new(&mut self.get_mut().file),
            position,
        )
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<u64>> {
        log::debug!("FileContainer.poll_complete triggered");

        AsyncSeek::poll_complete(
            Pin::new(&mut self.get_mut().file),
            cx,
        )
    }
}
