use nokv_holtstore::{
    BlockedRootEvent, BlockedSchedulerOperation, HoltMetadataStore, PendingRootEvent,
    PendingSchedulerOperation,
};
use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::meta::v1 as metapb;
use tonic::Status;

use crate::topology::{
    root_event_transition_id, scheduler_operation_action, scheduler_operation_transition_id,
};

pub trait RestartDiagnosticsProvider: Send + Sync + 'static {
    fn pending_root_event_count(&self) -> Result<u64, Status> {
        Ok(0)
    }

    fn blocked_root_event_count(&self) -> Result<u64, Status> {
        Ok(0)
    }

    fn pending_scheduler_operation_count(&self) -> Result<u64, Status> {
        Ok(0)
    }

    fn blocked_topology_statuses(&self) -> Result<Vec<adminpb::ExecutionTopologyStatus>, Status> {
        Ok(Vec::new())
    }

    fn pending_topology_statuses(&self) -> Result<Vec<adminpb::ExecutionTopologyStatus>, Status> {
        Ok(Vec::new())
    }
}

#[derive(Debug, Default)]
pub struct EmptyRestartDiagnostics;

impl RestartDiagnosticsProvider for EmptyRestartDiagnostics {}

impl RestartDiagnosticsProvider for HoltMetadataStore {
    fn pending_root_event_count(&self) -> Result<u64, Status> {
        self.pending_root_events()
            .map(|events| events.len() as u64)
            .map_err(|err| Status::internal(err.to_string()))
    }

    fn blocked_root_event_count(&self) -> Result<u64, Status> {
        self.blocked_root_events()
            .map(|events| events.len() as u64)
            .map_err(|err| Status::internal(err.to_string()))
    }

    fn pending_scheduler_operation_count(&self) -> Result<u64, Status> {
        self.pending_scheduler_operations()
            .map(|ops| ops.len() as u64)
            .map_err(|err| Status::internal(err.to_string()))
    }

    fn blocked_topology_statuses(&self) -> Result<Vec<adminpb::ExecutionTopologyStatus>, Status> {
        let mut statuses: Vec<adminpb::ExecutionTopologyStatus> = self
            .blocked_root_events()
            .map(|events| {
                events
                    .iter()
                    .map(blocked_root_event_topology_status)
                    .collect()
            })
            .map_err(|err| Status::internal(err.to_string()))?;
        statuses.extend(
            self.blocked_scheduler_operations()
                .map_err(|err| Status::internal(err.to_string()))?
                .iter()
                .map(blocked_scheduler_operation_topology_status),
        );
        Ok(statuses)
    }

    fn pending_topology_statuses(&self) -> Result<Vec<adminpb::ExecutionTopologyStatus>, Status> {
        let mut statuses = self
            .pending_root_events()
            .map_err(|err| Status::internal(err.to_string()))?
            .iter()
            .map(pending_root_event_topology_status)
            .collect::<Vec<_>>();
        statuses.extend(
            self.pending_scheduler_operations()
                .map_err(|err| Status::internal(err.to_string()))?
                .iter()
                .map(pending_scheduler_operation_topology_status),
        );
        Ok(statuses)
    }
}

fn blocked_root_event_topology_status(
    blocked: &BlockedRootEvent,
) -> adminpb::ExecutionTopologyStatus {
    adminpb::ExecutionTopologyStatus {
        transition_id: blocked.transition_id.clone(),
        region_id: root_event_region_id(&blocked.event),
        action: root_event_action(&blocked.event).to_owned(),
        outcome: adminpb::ExecutionTopologyOutcome::Failed as i32,
        publish: adminpb::ExecutionPublishState::TerminalBlocked as i32,
        last_error: blocked.last_error.clone(),
        ..Default::default()
    }
}

fn pending_root_event_topology_status(
    pending: &PendingRootEvent,
) -> adminpb::ExecutionTopologyStatus {
    adminpb::ExecutionTopologyStatus {
        transition_id: root_event_transition_id(&pending.event),
        region_id: root_event_region_id(&pending.event),
        action: root_event_action(&pending.event).to_owned(),
        outcome: adminpb::ExecutionTopologyOutcome::Applied as i32,
        publish: adminpb::ExecutionPublishState::TerminalPending as i32,
        last_error: "root event publish pending".to_owned(),
        ..Default::default()
    }
}

fn root_event_region_id(event: &metapb::RootEvent) -> u64 {
    match event.payload.as_ref() {
        Some(metapb::root_event::Payload::PeerChange(change)) => change.region_id,
        Some(metapb::root_event::Payload::RegionDescriptor(descriptor)) => descriptor
            .descriptor
            .as_ref()
            .map(|region| region.region_id)
            .unwrap_or_default(),
        Some(metapb::root_event::Payload::RegionRemoval(removal)) => removal.region_id,
        _ => 0,
    }
}

fn root_event_action(event: &metapb::RootEvent) -> &'static str {
    match event.payload.as_ref() {
        Some(metapb::root_event::Payload::PeerChange(_)) => "peer change",
        Some(metapb::root_event::Payload::RegionDescriptor(_)) => "region descriptor",
        Some(metapb::root_event::Payload::RegionRemoval(_)) => "region removal",
        _ => "root event",
    }
}

fn pending_scheduler_operation_topology_status(
    pending: &PendingSchedulerOperation,
) -> adminpb::ExecutionTopologyStatus {
    let last_error = if pending.attempts == 0 {
        "scheduler operation pending".to_owned()
    } else {
        format!(
            "scheduler operation pending after {} attempt(s)",
            pending.attempts
        )
    };
    adminpb::ExecutionTopologyStatus {
        transition_id: scheduler_operation_transition_id(&pending.operation),
        region_id: pending.operation.region_id,
        action: scheduler_operation_action(&pending.operation).to_owned(),
        outcome: adminpb::ExecutionTopologyOutcome::Queued as i32,
        publish: adminpb::ExecutionPublishState::NotRequired as i32,
        last_error,
        ..Default::default()
    }
}

fn blocked_scheduler_operation_topology_status(
    blocked: &BlockedSchedulerOperation,
) -> adminpb::ExecutionTopologyStatus {
    adminpb::ExecutionTopologyStatus {
        transition_id: scheduler_operation_transition_id(&blocked.operation),
        region_id: blocked.operation.region_id,
        action: scheduler_operation_action(&blocked.operation).to_owned(),
        outcome: adminpb::ExecutionTopologyOutcome::Failed as i32,
        publish: adminpb::ExecutionPublishState::NotRequired as i32,
        last_error: format!(
            "scheduler operation blocked after {} attempt(s): {}",
            blocked.attempts, blocked.last_error
        ),
        ..Default::default()
    }
}
