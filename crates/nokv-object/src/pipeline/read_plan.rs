use std::collections::{HashMap, VecDeque};

use crate::chunk::ObjectReadBlock;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectReadPlan {
    pub output_len: usize,
    pub blocks: Vec<ObjectReadBlock>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ObjectReadPlanKey {
    pub object_id: u64,
    pub generation: u64,
    pub offset: u64,
    pub len: usize,
}

#[derive(Clone, Debug)]
pub struct ObjectReadPlanCache {
    capacity: usize,
    plans: HashMap<ObjectReadPlanKey, ObjectReadPlan>,
    order: VecDeque<ObjectReadPlanKey>,
}

impl ObjectReadPlan {
    pub fn new(output_len: usize, blocks: Vec<ObjectReadBlock>) -> Self {
        Self { output_len, blocks }
    }
}

impl ObjectReadPlanKey {
    pub fn new(object_id: u64, generation: u64, offset: u64, len: usize) -> Self {
        Self {
            object_id,
            generation,
            offset,
            len,
        }
    }
}

impl ObjectReadPlanCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            plans: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub fn get(&mut self, key: &ObjectReadPlanKey) -> Option<ObjectReadPlan> {
        if let Some(plan) = self.plans.get(key).cloned() {
            self.touch(*key);
            return Some(plan);
        }
        let mut selected = None;
        for cached_key in self.order.iter().rev().copied() {
            let Some(cached_plan) = self.plans.get(&cached_key) else {
                continue;
            };
            if let Some(plan) = slice_cached_read_plan(cached_key, cached_plan, *key) {
                selected = Some((cached_key, plan));
                break;
            }
        }
        let (cached_key, plan) = selected?;
        self.touch(cached_key);
        Some(plan)
    }

    pub fn insert(&mut self, key: ObjectReadPlanKey, plan: ObjectReadPlan) {
        self.order.retain(|existing| existing != &key);
        self.order.push_back(key);
        self.plans.insert(key, plan);
        while self.plans.len() > self.capacity {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.plans.remove(&oldest);
        }
    }

    pub fn len(&self) -> usize {
        self.plans.len()
    }

    pub fn is_empty(&self) -> bool {
        self.plans.is_empty()
    }

    fn touch(&mut self, key: ObjectReadPlanKey) {
        self.order.retain(|existing| existing != &key);
        self.order.push_back(key);
    }
}

fn slice_cached_read_plan(
    cached_key: ObjectReadPlanKey,
    cached_plan: &ObjectReadPlan,
    request_key: ObjectReadPlanKey,
) -> Option<ObjectReadPlan> {
    if cached_key.object_id != request_key.object_id
        || cached_key.generation != request_key.generation
        || request_key.offset < cached_key.offset
    {
        return None;
    }
    let cached_end = cached_key.offset.checked_add(cached_key.len as u64)?;
    let requested_end = request_key.offset.checked_add(request_key.len as u64)?;
    if requested_end > cached_end {
        return None;
    }
    let mut blocks = Vec::new();
    for block in &cached_plan.blocks {
        let block_start = cached_key.offset.checked_add(block.output_offset as u64)?;
        let block_end = block_start.checked_add(block.len as u64)?;
        let overlap_start = block_start.max(request_key.offset);
        let overlap_end = block_end.min(requested_end);
        if overlap_start >= overlap_end {
            continue;
        }
        let object_delta = overlap_start.checked_sub(block_start)?;
        blocks.push(ObjectReadBlock {
            object_key: block.object_key.clone(),
            digest_uri: block.digest_uri.clone(),
            object_offset: block.object_offset.checked_add(object_delta)?,
            object_len: block.object_len,
            len: usize::try_from(overlap_end - overlap_start).ok()?,
            output_offset: usize::try_from(overlap_start - request_key.offset).ok()?,
        });
    }
    Some(ObjectReadPlan::new(request_key.len, blocks))
}
