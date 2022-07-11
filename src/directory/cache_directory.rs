use std::{path::Path, fmt};

use crate::Directory;

use super::{RamDirectory, FileHandle, error::{OpenReadError, OpenWriteError, DeleteError}, WritePtr, FileSlice, WatchHandle};

/// A Directory storing recent segments in memory
///
/// Meant for speeding up frequent indexing operations that require
/// committing a lot of small segments. It relies on "soft commit" support.
#[derive(Clone)]
pub struct CacheDirectory {
    inner: Box<dyn Directory>,
    ram_directory: RamDirectory,
}

impl CacheDirectory {
    /// Create a `CacheDirectory` that wraps an `inner` directory
    pub fn create<T: Into<Box<dyn Directory>>>(inner: T) -> CacheDirectory {
        CacheDirectory {
            inner: inner.into(),
            ram_directory: RamDirectory::create(),
        }
    }
}

impl fmt::Debug for CacheDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CacheDirectory")
    }
}

impl Directory for CacheDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Box<dyn FileHandle>, OpenReadError> {
        self.ram_directory.get_file_handle(path).or_else(|_error| self.inner.get_file_handle(path))
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        self.ram_directory.open_read(path).or_else(|_error| self.inner.open_read(path))
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        self.ram_directory.open_write(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.ram_directory.delete(path).or_else(|_error| self.inner.delete(path))
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.ram_directory.exists(path).or_else(|_error| self.inner.exists(path))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.ram_directory.atomic_read(path).or_else(|_error| self.inner.atomic_read(path))
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        self.ram_directory.atomic_write(path, data)
    }

    fn watch(&self, watch_callback: super::WatchCallback) -> crate::Result<WatchHandle> {
        self.ram_directory.watch(watch_callback)
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn persist(&self) -> crate::Result<()> {
        self.ram_directory.persist(self.inner.as_ref())
    }
}
