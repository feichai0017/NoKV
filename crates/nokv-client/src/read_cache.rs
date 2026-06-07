use std::collections::{HashMap, VecDeque};

use nokv_object::FileReadPipeline;

#[derive(Debug)]
pub(super) struct ReadPipelineCache {
    capacity: usize,
    pipelines: HashMap<String, FileReadPipeline>,
    order: VecDeque<String>,
}

impl ReadPipelineCache {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            pipelines: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub(super) fn take(&mut self, key: &str) -> FileReadPipeline {
        self.order.retain(|existing| existing != key);
        self.pipelines.remove(key).unwrap_or_default()
    }

    pub(super) fn insert(&mut self, key: String, pipeline: FileReadPipeline) {
        self.pipelines.insert(key.clone(), pipeline);
        self.order.retain(|existing| existing != &key);
        self.order.push_back(key);
        while self.pipelines.len() > self.capacity {
            let Some(victim) = self.order.pop_front() else {
                break;
            };
            self.pipelines.remove(&victim);
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.pipelines.len()
    }

    #[cfg(test)]
    pub(super) fn contains(&self, key: &str) -> bool {
        self.pipelines.contains_key(key)
    }
}
