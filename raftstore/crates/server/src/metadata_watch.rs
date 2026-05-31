use nokv_proto::nokv::metadata::v1 as metadatapb;
use nokv_raftnode::{ApplyWatchProvider, ApplyWatchReplayRequest};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::watch_wire::{chunk_apply_watch_keys, matching_apply_watch_keys};
use crate::{internal_error, DEFAULT_APPLY_WATCH_BUFFER};

pub(crate) type MetadataWatchApplyStream =
    ReceiverStream<Result<metadatapb::MetadataWatchApplyResponse, Status>>;

pub(crate) fn metadata_watch_apply_stream<E>(
    engine: &E,
    request: metadatapb::MetadataWatchApplyRequest,
) -> Result<MetadataWatchApplyStream, Status>
where
    E: ApplyWatchProvider,
{
    let mut source = engine.subscribe_apply();
    let (tx, rx) = tokio::sync::mpsc::channel(
        request
            .buffer
            .try_into()
            .ok()
            .filter(|buffer| *buffer > 0)
            .unwrap_or(DEFAULT_APPLY_WATCH_BUFFER),
    );
    let replay = engine
        .replay_apply(ApplyWatchReplayRequest {
            region_id: request.resume_region_id,
            term: request.resume_term,
            index: request.resume_index,
            key_prefix: request.key_prefix.clone(),
        })
        .map_err(internal_error)?;
    tokio::spawn(async move {
        if replay.expired {
            let _ = tx
                .send(Ok(metadatapb::MetadataWatchApplyResponse {
                    event: None,
                    dropped_events: 1,
                }))
                .await;
            return;
        }
        let mut last_replayed = replay.events.iter().fold(None, advance_apply_watch_cursor);
        for event in replay.events {
            if send_metadata_apply_event(&tx, &request, &event)
                .await
                .is_err()
            {
                return;
            }
        }
        loop {
            match source.recv().await {
                Ok(event) => {
                    if apply_watch_event_at_or_before(&event, last_replayed) {
                        continue;
                    }
                    if send_metadata_apply_event(&tx, &request, &event)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    last_replayed = advance_apply_watch_cursor(last_replayed, &event);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                    if tx
                        .send(Ok(metadatapb::MetadataWatchApplyResponse {
                            event: None,
                            dropped_events: dropped,
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    Ok(ReceiverStream::new(rx))
}

async fn send_metadata_apply_event(
    tx: &tokio::sync::mpsc::Sender<Result<metadatapb::MetadataWatchApplyResponse, Status>>,
    request: &metadatapb::MetadataWatchApplyRequest,
    event: &metadatapb::MetadataApplyWatchEvent,
) -> Result<(), ()> {
    let keys = matching_apply_watch_keys(&event.keys, &request.key_prefix);
    for chunk in chunk_apply_watch_keys(keys) {
        let response = metadatapb::MetadataWatchApplyResponse {
            event: Some(metadatapb::MetadataApplyWatchEvent {
                region_id: event.region_id,
                term: event.term,
                index: event.index,
                source: event.source,
                commit_version: event.commit_version,
                keys: chunk,
            }),
            dropped_events: 0,
        };
        tx.send(Ok(response)).await.map_err(|_| ())?;
    }
    Ok(())
}

fn advance_apply_watch_cursor(
    current: Option<(u64, u64)>,
    event: &metadatapb::MetadataApplyWatchEvent,
) -> Option<(u64, u64)> {
    let event_cursor = (event.term, event.index);
    match current {
        Some(cursor) if !apply_watch_cursor_after(event_cursor, cursor) => Some(cursor),
        _ => Some(event_cursor),
    }
}

fn apply_watch_event_at_or_before(
    event: &metadatapb::MetadataApplyWatchEvent,
    cursor: Option<(u64, u64)>,
) -> bool {
    let Some(cursor) = cursor else {
        return false;
    };
    !apply_watch_cursor_after((event.term, event.index), cursor)
}

fn apply_watch_cursor_after(candidate: (u64, u64), cursor: (u64, u64)) -> bool {
    if cursor.0 == 0 {
        return candidate.1 > cursor.1;
    }
    candidate.0 > cursor.0 || (candidate.0 == cursor.0 && candidate.1 > cursor.1)
}
