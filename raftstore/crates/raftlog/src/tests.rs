use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

use crate::{Error, LogEntry, LogMarker, SegmentedRaftLog};

#[test]
fn append_and_recover_entries() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = SegmentedRaftLog::open(dir.path()).unwrap();
    log.append(&[
        LogEntry {
            region_id: 7,
            index: 1,
            term: 3,
            payload: b"a".to_vec(),
        },
        LogEntry {
            region_id: 7,
            index: 2,
            term: 3,
            payload: b"b".to_vec(),
        },
    ])
    .unwrap();
    log.sync().unwrap();
    let recovered = log.recover().unwrap();
    assert_eq!(recovered.len(), 2);
    assert_eq!(recovered[1].payload, b"b");
}

#[test]
fn detects_corrupt_record() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = SegmentedRaftLog::open(dir.path()).unwrap();
    log.append(&[LogEntry {
        region_id: 1,
        index: 1,
        term: 1,
        payload: b"payload".to_vec(),
    }])
    .unwrap();
    drop(log);
    let path = dir.path().join("000001.log");
    let mut file = OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    file.write_all(&[0xff]).unwrap();
    drop(file);
    assert!(matches!(
        SegmentedRaftLog::open(dir.path()),
        Err(Error::Corrupt { .. })
    ));
}

#[test]
fn truncate_since_removes_conflicting_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = SegmentedRaftLog::open(dir.path()).unwrap();
    log.append(&[
        LogEntry {
            region_id: 7,
            index: 1,
            term: 1,
            payload: b"a".to_vec(),
        },
        LogEntry {
            region_id: 7,
            index: 2,
            term: 1,
            payload: b"b".to_vec(),
        },
        LogEntry {
            region_id: 7,
            index: 3,
            term: 2,
            payload: b"c".to_vec(),
        },
    ])
    .unwrap();
    log.truncate_since(3).unwrap();
    drop(log);

    let log = SegmentedRaftLog::open(dir.path()).unwrap();
    let recovered = log.recover().unwrap();
    assert_eq!(recovered.len(), 2);
    assert_eq!(recovered[1].index, 2);
}

#[test]
fn purge_upto_persists_marker_and_filters_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = SegmentedRaftLog::open(dir.path()).unwrap();
    log.append(&[
        LogEntry {
            region_id: 7,
            index: 1,
            term: 1,
            payload: b"a".to_vec(),
        },
        LogEntry {
            region_id: 7,
            index: 2,
            term: 1,
            payload: b"b".to_vec(),
        },
        LogEntry {
            region_id: 7,
            index: 3,
            term: 2,
            payload: b"c".to_vec(),
        },
    ])
    .unwrap();
    log.purge_upto(LogMarker {
        region_id: 7,
        index: 2,
        term: 1,
        node_id: 1,
    })
    .unwrap();
    drop(log);

    let log = SegmentedRaftLog::open(dir.path()).unwrap();
    assert_eq!(
        log.last_purged().unwrap(),
        Some(LogMarker {
            region_id: 7,
            index: 2,
            term: 1,
            node_id: 1,
        })
    );
    let recovered = log.recover().unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].index, 3);
}

#[test]
fn purge_beyond_tail_makes_marker_the_log_frontier() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = SegmentedRaftLog::open(dir.path()).unwrap();
    log.append(&[
        LogEntry {
            region_id: 7,
            index: 1,
            term: 1,
            payload: b"a".to_vec(),
        },
        LogEntry {
            region_id: 7,
            index: 2,
            term: 1,
            payload: b"b".to_vec(),
        },
    ])
    .unwrap();
    log.purge_upto(LogMarker {
        region_id: 7,
        index: 5,
        term: 3,
        node_id: 1,
    })
    .unwrap();

    assert!(log.recover().unwrap().is_empty());
    assert_eq!(
        log.last_purged().unwrap(),
        Some(LogMarker {
            region_id: 7,
            index: 5,
            term: 3,
            node_id: 1,
        })
    );
    log.append(&[LogEntry {
        region_id: 7,
        index: 6,
        term: 4,
        payload: b"next".to_vec(),
    }])
    .unwrap();
    assert_eq!(log.recover().unwrap()[0].index, 6);
}

#[test]
fn append_rejects_holes_after_existing_log() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = SegmentedRaftLog::open(dir.path()).unwrap();
    log.append(&[LogEntry {
        region_id: 1,
        index: 1,
        term: 1,
        payload: b"a".to_vec(),
    }])
    .unwrap();

    let err = log
        .append(&[LogEntry {
            region_id: 1,
            index: 3,
            term: 1,
            payload: b"c".to_vec(),
        }])
        .unwrap_err();
    assert!(matches!(
        err,
        Error::NonConsecutiveAppend {
            expected: 2,
            actual: 3
        }
    ));
}

#[test]
fn append_replaces_overlapping_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = SegmentedRaftLog::open(dir.path()).unwrap();
    log.append(&[
        LogEntry {
            region_id: 1,
            index: 1,
            term: 1,
            payload: b"a".to_vec(),
        },
        LogEntry {
            region_id: 1,
            index: 2,
            term: 1,
            payload: b"old".to_vec(),
        },
    ])
    .unwrap();
    log.append(&[
        LogEntry {
            region_id: 1,
            index: 2,
            term: 2,
            payload: b"new".to_vec(),
        },
        LogEntry {
            region_id: 1,
            index: 3,
            term: 2,
            payload: b"c".to_vec(),
        },
    ])
    .unwrap();

    let recovered = log.recover().unwrap();
    assert_eq!(recovered.len(), 3);
    assert_eq!(recovered[1].term, 2);
    assert_eq!(recovered[1].payload, b"new");
}
