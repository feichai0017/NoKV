//! Standalone raftstore process startup, root publication, and coordinator loops.

pub(crate) mod bootstrap;
pub(crate) mod coordinator;
mod hosted_region;
mod metrics;
mod region_open;
mod root_publication;
mod scheduler_operations;
pub(crate) mod startup;

#[cfg(test)]
mod tests;
