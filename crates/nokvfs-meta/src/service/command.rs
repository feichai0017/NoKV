use super::*;

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub(super) fn commit_metadata(
        &self,
        command: MetadataCommand,
    ) -> Result<CommitResult, MetadError> {
        self.metadata.commit_metadata(command).map_err(Into::into)
    }
}
