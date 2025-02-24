// Copyright 2025 FastLabs Developers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use mea::latch::Latch;
use mea::semaphore::Semaphore;

use crate::mutex::Mutex;
use crate::ManageObject;

/// Queue strategy when deque objects from the [`Pool`].
#[derive(Debug, Default, Clone, Copy)]
pub enum QueueStrategy {
    /// First in first out.
    ///
    /// This strategy behaves like a queue.
    #[default]
    Fifo,
    /// Last in first out.
    ///
    /// This strategy behaves like a stack.
    Lifo,
}

/// The configuration of [`Pool`].
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct PoolConfig {
    /// Maximum size of the [`Pool`].
    pub max_size: usize,

    /// Queue strategy of the [`Pool`].
    ///
    /// Determines the order of objects being queued and dequeued.
    pub queue_strategy: QueueStrategy,
}

impl PoolConfig {
    /// Creates a new [`PoolConfig`].
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            queue_strategy: QueueStrategy::default(),
        }
    }

    /// Returns a new [`PoolConfig`] with the specified queue strategy.
    pub fn with_queue_strategy(mut self, queue_strategy: QueueStrategy) -> Self {
        self.queue_strategy = queue_strategy;
        self
    }
}

#[derive(Clone)]
pub struct Pool<M: ManageObject> {
    state: Arc<PoolState<M>>,
}

impl<M> std::fmt::Debug for Pool<M>
where
    M: ManageObject,
    M::Object: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (*self.state).fmt(f)
    }
}

struct PoolState<M: ManageObject> {
    config: PoolConfig,
    manager: M,

    /// The total number of current obtained objects + futures waiting for an object.
    users: AtomicUsize,
    /// A semaphore that limits the number of objects in the pool.
    permits: Semaphore,
    /// A countdown latch that is released when the pool is being shutdown.
    shutdown: Latch,
    /// A deque that holds the objects.
    slots: Mutex<PoolDeque<M::Object>>,
}

#[derive(Debug)]
struct PoolDeque<T> {
    deque: VecDeque<T>,
    current_size: usize,
    max_size: usize,
}

impl<M> std::fmt::Debug for PoolState<M>
where
    M: ManageObject,
    M::Object: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("slots", &self.slots)
            .field("config", &self.config)
            .field("users", &self.users)
            .field("permits", &self.permits)
            .field("shutdown", &self.shutdown)
            .finish()
    }
}

impl<M: ManageObject> PoolState<M> {
    fn push_back(&self, mut o: M::Object) {
        let mut slots = self.slots.lock();
        if slots.current_size <= slots.max_size {
            slots.deque.push_back(o);
            drop(slots);
            self.permits.release(1)
        } else {
            slots.current_size -= 1;
            drop(slots);
            self.manager.on_detached(&mut o);
        }
    }

    fn detach_object(&self, o: &mut M::Object) {
        let mut slots = self.slots.lock();
    }
}
