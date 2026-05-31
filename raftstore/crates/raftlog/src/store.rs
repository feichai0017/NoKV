use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::codec::{
    open_log_file, read_entries_from_path, read_marker, sync_parent, write_entry, write_marker,
};
use crate::{Error, LogEntry, LogMarker, Result};

pub struct SegmentedRaftLog {
    path: PathBuf,
    marker_path: PathBuf,
    file: File,
    entries: Vec<LogEntry>,
    purged: Option<LogMarker>,
}

impl SegmentedRaftLog {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        std::fs::create_dir_all(dir.as_ref())?;
        let path = dir.as_ref().join("000001.log");
        let marker_path = dir.as_ref().join("purged.meta");
        let file = open_log_file(&path)?;
        let purged = read_marker(&marker_path)?;
        let entries = read_entries_from_path(&path, purged.map(|marker| marker.index))?;
        Ok(Self {
            path,
            marker_path,
            file,
            entries,
            purged,
        })
    }

    pub fn append(&mut self, entries: &[LogEntry]) -> Result<()> {
        let rewrite_prefix = self.prepare_append(entries)?;
        if let Some(prefix) = rewrite_prefix {
            self.rewrite_entries(&prefix)?;
        }
        for entry in entries {
            write_entry(&mut self.file, entry)?;
        }
        self.entries.extend_from_slice(entries);
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    pub fn recover(&self) -> Result<Vec<LogEntry>> {
        Ok(self.entries.clone())
    }

    pub fn truncate_since(&mut self, index: u64) -> Result<()> {
        let retained = self
            .recover()?
            .into_iter()
            .filter(|entry| entry.index < index)
            .collect::<Vec<_>>();
        self.rewrite_entries(&retained)
    }

    pub fn purge_upto(&mut self, marker: LogMarker) -> Result<()> {
        let marker = match self.last_purged()? {
            Some(current) if current.index >= marker.index => current,
            _ => marker,
        };
        write_marker(&self.marker_path, marker)?;
        self.purged = Some(marker);
        let retained = self
            .entries
            .iter()
            .filter(|entry| entry.index > marker.index)
            .cloned()
            .collect::<Vec<_>>();
        self.rewrite_entries(&retained)
    }

    pub fn last_purged(&self) -> Result<Option<LogMarker>> {
        Ok(self.purged)
    }

    fn prepare_append(&self, entries: &[LogEntry]) -> Result<Option<Vec<LogEntry>>> {
        let Some(first) = entries.first() else {
            return Ok(None);
        };
        let marker = self.last_purged()?;
        if let Some(marker) = marker {
            if first.index <= marker.index {
                return Err(Error::AppendPurgedEntry {
                    index: first.index,
                    purged_index: marker.index,
                });
            }
        }

        for pair in entries.windows(2) {
            let expected = pair[0].index + 1;
            let actual = pair[1].index;
            if actual != expected {
                return Err(Error::NonConsecutiveAppend { expected, actual });
            }
        }

        let prefix = self
            .entries
            .iter()
            .take_while(|entry| entry.index < first.index)
            .cloned()
            .collect::<Vec<_>>();
        if let Some(last) = prefix.last() {
            let expected = last.index + 1;
            if first.index != expected {
                return Err(Error::NonConsecutiveAppend {
                    expected,
                    actual: first.index,
                });
            }
        } else if let Some(marker) = marker {
            let expected = marker.index + 1;
            if first.index != expected {
                return Err(Error::NonConsecutiveAppend {
                    expected,
                    actual: first.index,
                });
            }
        }
        if prefix.len() != self.entries.len() {
            Ok(Some(prefix))
        } else {
            Ok(None)
        }
    }

    fn rewrite_entries(&mut self, entries: &[LogEntry]) -> Result<()> {
        let tmp_path = self.path.with_extension("rewrite");
        {
            let mut tmp = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            for entry in entries {
                write_entry(&mut tmp, entry)?;
            }
            tmp.sync_data()?;
        }
        std::fs::rename(&tmp_path, &self.path)?;
        sync_parent(&self.path)?;
        self.file = open_log_file(&self.path)?;
        self.entries = entries.to_vec();
        Ok(())
    }
}
