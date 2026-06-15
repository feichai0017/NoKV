//! Staging memory used by the Python training read path.
//!
//! The public Python API exposes ordinary system memory and Unix page-locked
//! host memory. Keep this boundary explicit so future CUDA-registered, RDMA
//! registered, or device-backed allocators can be added without changing the
//! read/scatter path.

use std::fmt;
use std::ops::Range;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StagingMemoryKind {
    System,
    PageLocked,
}

impl StagingMemoryKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::PageLocked => "page_locked",
        }
    }

    pub(crate) fn parse(raw: &str) -> Option<Self> {
        match raw {
            "system" => Some(Self::System),
            "page_locked" => Some(Self::PageLocked),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum StagingMemoryError {
    #[cfg(not(unix))]
    Unsupported(&'static str),
    PageLock(std::io::Error),
    PageUnlock(std::io::Error),
}

impl fmt::Display for StagingMemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(not(unix))]
            Self::Unsupported(kind) => write!(f, "staging memory kind {kind:?} is unsupported"),
            Self::PageLock(err) => write!(f, "failed to lock page_locked staging memory: {err}"),
            Self::PageUnlock(err) => {
                write!(f, "failed to unlock page_locked staging memory: {err}")
            }
        }
    }
}

impl std::error::Error for StagingMemoryError {}

type StagingResult<T> = Result<T, StagingMemoryError>;

#[derive(Debug)]
struct PageLockedBuffer {
    bytes: Vec<u8>,
    locked_capacity: usize,
}

impl PageLockedBuffer {
    fn with_capacity(capacity: usize) -> StagingResult<Self> {
        let bytes = Vec::with_capacity(capacity);
        lock_pages(&bytes, capacity)?;
        Ok(Self {
            bytes,
            locked_capacity: capacity,
        })
    }

    fn reserve(&mut self, capacity: usize) -> StagingResult<()> {
        if capacity <= self.bytes.capacity() {
            return Ok(());
        }

        let current_capacity = self.bytes.capacity();
        let doubled = current_capacity.saturating_mul(2);
        let new_capacity = capacity.max(doubled).max(1);
        let mut bytes = Vec::with_capacity(new_capacity);
        bytes.extend_from_slice(&self.bytes);
        lock_pages(&bytes, new_capacity)?;

        let old = std::mem::replace(
            self,
            Self {
                bytes,
                locked_capacity: new_capacity,
            },
        );
        old.unlock()?;
        Ok(())
    }

    fn unlock(mut self) -> StagingResult<()> {
        let result = unlock_pages(&self.bytes, self.locked_capacity);
        self.locked_capacity = 0;
        result
    }
}

impl Drop for PageLockedBuffer {
    fn drop(&mut self) {
        if self.locked_capacity != 0 {
            let _ = unlock_pages(&self.bytes, self.locked_capacity);
            self.locked_capacity = 0;
        }
    }
}

#[derive(Debug)]
pub(crate) struct StagingBuffer {
    storage: StagingStorage,
}

#[derive(Debug)]
enum StagingStorage {
    System(Vec<u8>),
    PageLocked(PageLockedBuffer),
}

impl StagingBuffer {
    pub(crate) fn with_capacity(kind: StagingMemoryKind, capacity: usize) -> StagingResult<Self> {
        match kind {
            StagingMemoryKind::System => Ok(Self::system_with_capacity(capacity)),
            StagingMemoryKind::PageLocked => Ok(Self {
                storage: StagingStorage::PageLocked(PageLockedBuffer::with_capacity(capacity)?),
            }),
        }
    }

    pub(crate) fn system_with_capacity(capacity: usize) -> Self {
        Self {
            storage: StagingStorage::System(Vec::with_capacity(capacity)),
        }
    }

    pub(crate) fn kind(&self) -> StagingMemoryKind {
        match self.storage {
            StagingStorage::System(_) => StagingMemoryKind::System,
            StagingStorage::PageLocked(_) => StagingMemoryKind::PageLocked,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match &self.storage {
            StagingStorage::System(bytes) => bytes.len(),
            StagingStorage::PageLocked(buffer) => buffer.bytes.len(),
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        match &self.storage {
            StagingStorage::System(bytes) => bytes.capacity(),
            StagingStorage::PageLocked(buffer) => buffer.bytes.capacity(),
        }
    }

    pub(crate) fn reserve(&mut self, capacity: usize) -> StagingResult<()> {
        match &mut self.storage {
            StagingStorage::System(bytes) => {
                let current_capacity = bytes.capacity();
                if current_capacity < capacity {
                    bytes.reserve(capacity - current_capacity);
                }
                Ok(())
            }
            StagingStorage::PageLocked(buffer) => buffer.reserve(capacity),
        }
    }

    pub(crate) fn resize(&mut self, len: usize, value: u8) -> StagingResult<()> {
        self.reserve(len)?;
        match &mut self.storage {
            StagingStorage::System(bytes) => bytes.resize(len, value),
            StagingStorage::PageLocked(buffer) => buffer.bytes.resize(len, value),
        }
        Ok(())
    }

    pub(crate) fn clear(&mut self) {
        match &mut self.storage {
            StagingStorage::System(bytes) => bytes.clear(),
            StagingStorage::PageLocked(buffer) => buffer.bytes.clear(),
        }
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        match &self.storage {
            StagingStorage::System(bytes) => bytes,
            StagingStorage::PageLocked(buffer) => &buffer.bytes,
        }
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        match &mut self.storage {
            StagingStorage::System(bytes) => bytes,
            StagingStorage::PageLocked(buffer) => &mut buffer.bytes,
        }
    }

    pub(crate) fn get(&self, range: Range<usize>) -> Option<&[u8]> {
        self.as_slice().get(range)
    }

    pub(crate) fn get_byte(&self, index: usize) -> Option<u8> {
        self.as_slice().get(index).copied()
    }
}

#[cfg(unix)]
fn lock_pages(bytes: &[u8], capacity: usize) -> StagingResult<()> {
    if capacity == 0 {
        return Ok(());
    }
    let result = unsafe { libc::mlock(bytes.as_ptr().cast(), capacity) };
    if result == 0 {
        Ok(())
    } else {
        Err(StagingMemoryError::PageLock(std::io::Error::last_os_error()))
    }
}

#[cfg(not(unix))]
fn lock_pages(_bytes: &[u8], _capacity: usize) -> StagingResult<()> {
    Err(StagingMemoryError::Unsupported("page_locked"))
}

#[cfg(unix)]
fn unlock_pages(bytes: &[u8], capacity: usize) -> StagingResult<()> {
    if capacity == 0 {
        return Ok(());
    }
    let result = unsafe { libc::munlock(bytes.as_ptr().cast(), capacity) };
    if result == 0 {
        Ok(())
    } else {
        Err(StagingMemoryError::PageUnlock(
            std::io::Error::last_os_error(),
        ))
    }
}

#[cfg(not(unix))]
fn unlock_pages(_bytes: &[u8], _capacity: usize) -> StagingResult<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_buffer_resizes_and_preserves_bytes() {
        let mut buffer = StagingBuffer::system_with_capacity(2);
        assert_eq!(buffer.kind(), StagingMemoryKind::System);
        assert!(buffer.capacity() >= 2);

        buffer.resize(4, 7).unwrap();
        assert_eq!(buffer.as_slice(), &[7, 7, 7, 7]);

        buffer.as_mut_slice()[1] = 3;
        buffer.reserve(16).unwrap();
        assert_eq!(buffer.get_byte(1), Some(3));
        assert!(buffer.capacity() >= 16);

        buffer.clear();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.kind().as_str(), "system");
    }

    #[test]
    fn page_locked_buffer_works_or_reports_os_error() {
        let mut buffer = match StagingBuffer::with_capacity(StagingMemoryKind::PageLocked, 4096) {
            Ok(buffer) => buffer,
            Err(err) => {
                let message = err.to_string();
                assert!(message.contains("page_locked"));
                return;
            }
        };

        assert_eq!(buffer.kind(), StagingMemoryKind::PageLocked);
        assert_eq!(buffer.kind().as_str(), "page_locked");
        buffer.resize(32, 9).unwrap();
        assert_eq!(buffer.as_slice(), &[9; 32]);
        buffer.as_mut_slice()[0] = 5;
        buffer.reserve(8192).unwrap();
        assert_eq!(buffer.get_byte(0), Some(5));
    }
}
