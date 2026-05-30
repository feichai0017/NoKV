use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::error::v1 as errorpb;
use nokv_proto::nokv::kv::v1 as kvpb;
use tonic::Status;

#[derive(Debug, Clone, Default)]
pub(crate) struct ExecutionRuntime {
    last_admission: Arc<Mutex<adminpb::ExecutionAdmissionStatus>>,
}

impl ExecutionRuntime {
    pub(crate) fn record_admission(
        &self,
        class: adminpb::ExecutionAdmissionClass,
        context: Option<&kvpb::Context>,
        region_error: Option<&errorpb::RegionError>,
    ) {
        let (reason, accepted, detail) = classify_admission(region_error);
        let status = adminpb::ExecutionAdmissionStatus {
            observed: true,
            class: class as i32,
            reason: reason as i32,
            accepted,
            region_id: context.map(|context| context.region_id).unwrap_or_default(),
            peer_id: context
                .and_then(|context| context.peer.as_ref())
                .map(|peer| peer.peer_id)
                .unwrap_or_default(),
            request_id: 0,
            detail,
            at_unix_nano: now_unix_nano(),
        };
        if let Ok(mut last) = self.last_admission.lock() {
            *last = status;
        }
    }

    pub(crate) fn record_invalid(
        &self,
        class: adminpb::ExecutionAdmissionClass,
        context: Option<&kvpb::Context>,
        detail: impl Into<String>,
    ) {
        let status = adminpb::ExecutionAdmissionStatus {
            observed: true,
            class: class as i32,
            reason: adminpb::ExecutionAdmissionReason::Invalid as i32,
            accepted: false,
            region_id: context.map(|context| context.region_id).unwrap_or_default(),
            peer_id: context
                .and_then(|context| context.peer.as_ref())
                .map(|peer| peer.peer_id)
                .unwrap_or_default(),
            request_id: 0,
            detail: detail.into(),
            at_unix_nano: now_unix_nano(),
        };
        if let Ok(mut last) = self.last_admission.lock() {
            *last = status;
        }
    }

    pub(crate) fn snapshot(&self) -> Result<adminpb::ExecutionAdmissionStatus, Status> {
        self.last_admission
            .lock()
            .map_err(|_| Status::internal("execution admission mutex poisoned"))
            .map(|status| status.clone())
    }
}

fn classify_admission(
    region_error: Option<&errorpb::RegionError>,
) -> (adminpb::ExecutionAdmissionReason, bool, String) {
    let Some(region_error) = region_error else {
        return (
            adminpb::ExecutionAdmissionReason::Accepted,
            true,
            String::new(),
        );
    };
    if region_error.store_not_match.is_some() {
        return (
            adminpb::ExecutionAdmissionReason::StoreNotMatch,
            false,
            "store not match".to_owned(),
        );
    }
    if region_error.region_not_found.is_some() {
        return (
            adminpb::ExecutionAdmissionReason::NotHosted,
            false,
            "region not hosted".to_owned(),
        );
    }
    if region_error.epoch_not_match.is_some() {
        return (
            adminpb::ExecutionAdmissionReason::EpochMismatch,
            false,
            "epoch mismatch".to_owned(),
        );
    }
    if region_error.key_not_in_region.is_some() {
        return (
            adminpb::ExecutionAdmissionReason::KeyNotInRegion,
            false,
            "key not in region".to_owned(),
        );
    }
    if region_error.not_leader.is_some() {
        return (
            adminpb::ExecutionAdmissionReason::NotLeader,
            false,
            "not leader".to_owned(),
        );
    }
    (
        adminpb::ExecutionAdmissionReason::Unspecified,
        false,
        "stale command".to_owned(),
    )
}

fn now_unix_nano() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}
