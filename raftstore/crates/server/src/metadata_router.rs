//! Multi-region MetadataPlane routing for one raftstore process.

use nokv_proto::nokv::metadata::v1 as metadatapb;
use nokv_raftnode::{ApplyWatchProvider, MetadataCommandExecutor, MetadataReadExecutor};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Response, Status};

use crate::region_registry::{metadata_region_lookup, RegionServiceLookup, RegionServiceRegistry};
use crate::{
    AppliedRegionDescriptorProvider, MetadataPlaneService, RaftRuntimeStatusProvider,
    DEFAULT_APPLY_WATCH_BUFFER,
};

#[derive(Clone)]
pub struct MultiRegionMetadataPlaneService<E> {
    regions: RegionServiceRegistry<MetadataPlaneService<E>>,
}

impl<E> MultiRegionMetadataPlaneService<E> {
    pub fn new(
        regions: impl IntoIterator<Item = (u64, MetadataPlaneService<E>)>,
    ) -> Result<Self, Status> {
        Ok(Self {
            regions: RegionServiceRegistry::new(regions)?,
        })
    }

    pub fn insert_region(
        &self,
        region_id: u64,
        service: MetadataPlaneService<E>,
    ) -> Result<(), Status> {
        self.regions.insert_region(region_id, service)
    }

    pub fn remove_region(&self, region_id: u64) -> Result<Option<MetadataPlaneService<E>>, Status> {
        self.regions.remove_region(region_id)
    }

    fn service_for_context(
        &self,
        context: Option<&metadatapb::MetadataContext>,
    ) -> Result<RegionServiceLookup<MetadataPlaneService<E>>, Status>
    where
        MetadataPlaneService<E>: Clone,
    {
        metadata_region_lookup(&self.regions, context)
    }
}

macro_rules! delegate_metadata_request {
    ($self:expr, $request:expr, $method:ident, $response_type:ident) => {{
        let request = $request.into_inner();
        match $self.service_for_context(request.context.as_ref())? {
            RegionServiceLookup::Hosted(service) => service.$method(Request::new(request)).await,
            RegionServiceLookup::Missing(region_error) => {
                Ok(Response::new(metadatapb::$response_type {
                    region_error: Some(region_error),
                    ..Default::default()
                }))
            }
        }
    }};
}

#[tonic::async_trait]
impl<E> metadatapb::metadata_plane_server::MetadataPlane for MultiRegionMetadataPlaneService<E>
where
    E: AppliedRegionDescriptorProvider
        + ApplyWatchProvider
        + MetadataCommandExecutor
        + MetadataReadExecutor
        + RaftRuntimeStatusProvider,
{
    async fn get(
        &self,
        request: Request<metadatapb::MetadataGetRequest>,
    ) -> Result<Response<metadatapb::MetadataGetResponse>, Status> {
        delegate_metadata_request!(self, request, get, MetadataGetResponse)
    }

    async fn batch_get(
        &self,
        request: Request<metadatapb::MetadataBatchGetRequest>,
    ) -> Result<Response<metadatapb::MetadataBatchGetResponse>, Status> {
        delegate_metadata_request!(self, request, batch_get, MetadataBatchGetResponse)
    }

    async fn scan(
        &self,
        request: Request<metadatapb::MetadataScanRequest>,
    ) -> Result<Response<metadatapb::MetadataScanResponse>, Status> {
        delegate_metadata_request!(self, request, scan, MetadataScanResponse)
    }

    async fn commit_metadata(
        &self,
        request: Request<metadatapb::MetadataCommitRequest>,
    ) -> Result<Response<metadatapb::MetadataCommitResponse>, Status> {
        delegate_metadata_request!(self, request, commit_metadata, MetadataCommitResponse)
    }

    type WatchApplyStream = ReceiverStream<Result<metadatapb::MetadataWatchApplyResponse, Status>>;

    async fn watch_apply(
        &self,
        request: Request<metadatapb::MetadataWatchApplyRequest>,
    ) -> Result<Response<Self::WatchApplyStream>, Status> {
        let request = request.into_inner();
        let buffer = if request.buffer == 0 {
            DEFAULT_APPLY_WATCH_BUFFER
        } else {
            request.buffer as usize
        };
        let (tx, rx) = tokio::sync::mpsc::channel(buffer);
        for service in self.regions.values()? {
            let tx = tx.clone();
            let request = request.clone();
            tokio::spawn(async move {
                let response = service.watch_apply(Request::new(request)).await;
                let mut stream = match response {
                    Ok(response) => response.into_inner(),
                    Err(err) => {
                        let _ = tx.send(Err(err)).await;
                        return;
                    }
                };
                while let Some(item) = stream.next().await {
                    if tx.send(item).await.is_err() {
                        return;
                    }
                }
            });
        }
        drop(tx);
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
