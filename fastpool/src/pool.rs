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
use std::sync::Arc;
use std::sync::Weak;

use mea::semaphore::Semaphore;

use crate::mutex::Mutex;
use crate::ManageObject;
use crate::ObjectStatus;

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

pub struct Pool<M: ManageObject> {
    config: PoolConfig,
    manager: M,

    /// A semaphore that limits the maximum of users of the pool.
    permits: Semaphore,
    /// A deque that holds the objects.
    slots: Mutex<PoolDeque<ObjectState<M::Object>>>,
}

#[derive(Debug)]
struct PoolDeque<T> {
    deque: VecDeque<T>,
    current_size: usize,
    max_size: usize,
}

impl<M> std::fmt::Debug for Pool<M>
where
    M: ManageObject,
    M::Object: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("slots", &self.slots)
            .field("config", &self.config)
            .field("permits", &self.permits)
            .finish()
    }
}

impl<M: ManageObject> Pool<M> {
    pub fn new(config: PoolConfig, manager: M) -> Arc<Self> {
        let permits = Semaphore::new(config.max_size);
        let slots = Mutex::new(PoolDeque {
            deque: VecDeque::with_capacity(config.max_size),
            current_size: 0,
            max_size: config.max_size,
        });

        Arc::new(Self {
            config,
            manager,
            permits,
            slots,
        })
    }

    pub async fn get(self: &Arc<Self>) -> Result<Object<M>, M::Error> {
        let permit = self.permits.acquire(1).await;

        let object = loop {
            let existing = match self.config.queue_strategy {
                QueueStrategy::Fifo => self.slots.lock().deque.pop_front(),
                QueueStrategy::Lifo => self.slots.lock().deque.pop_back(),
            };

            match existing {
                None => {
                    let object = self.manager.create().await?;
                    let state = ObjectState {
                        o: object,
                        status: ObjectStatus::default(),
                    };
                    self.slots.lock().current_size += 1;
                    break Object {
                        state: Some(state),
                        pool: Arc::downgrade(self),
                    };
                }
                Some(object) => {
                    let mut unready_object = UnreadyObject {
                        state: Some(object),
                        pool: Arc::downgrade(self),
                    };

                    let state = unready_object.state();
                    let status = state.status;
                    if self
                        .manager
                        .is_recyclable(&mut state.o, &status)
                        .await
                        .is_ok()
                    {
                        state.status.recycle_count += 1;
                        state.status.recycled = Some(std::time::Instant::now());
                        break unready_object.ready();
                    }
                }
            };
        };

        permit.forget();
        Ok(object)
    }

    fn push_back(&self, o: ObjectState<M::Object>) {
        let mut slots = self.slots.lock();

        assert!(
            slots.current_size <= slots.max_size,
            "invariant broken: current_size <= max_size (actual: {} <= {})",
            slots.current_size,
            slots.max_size,
        );

        slots.deque.push_back(o);
        drop(slots);
        self.permits.release(1);
    }

    fn detach_object(&self, o: &mut M::Object) {
        let mut slots = self.slots.lock();

        assert!(
            slots.current_size <= slots.max_size,
            "invariant broken: current_size <= max_size (actual: {} <= {})",
            slots.current_size,
            slots.max_size,
        );

        slots.current_size -= 1;
        drop(slots);
        self.permits.release(1);
        self.manager.on_detached(o);
    }
}

pub struct Object<M: ManageObject> {
    state: Option<ObjectState<M::Object>>,
    pool: Weak<Pool<M>>,
}

impl<M> std::fmt::Debug for Object<M>
where
    M: ManageObject,
    M::Object: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Object")
            .field("state", &self.state)
            .finish()
    }
}

impl<M: ManageObject> Drop for Object<M> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            if let Some(pool) = self.pool.upgrade() {
                pool.push_back(state);
            }
        }
    }
}

impl<M: ManageObject> std::ops::Deref for Object<M> {
    type Target = M::Object;
    fn deref(&self) -> &M::Object {
        // SAFETY: `state` is always `Some` when `Object` is owned.
        &self.state.as_ref().unwrap().o
    }
}

impl<M: ManageObject> std::ops::DerefMut for Object<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: `state` is always `Some` when `Object` is owned.
        &mut self.state.as_mut().unwrap().o
    }
}

impl<M: ManageObject> AsRef<M::Object> for Object<M> {
    fn as_ref(&self) -> &M::Object {
        self
    }
}

impl<M: ManageObject> AsMut<M::Object> for Object<M> {
    fn as_mut(&mut self) -> &mut M::Object {
        self
    }
}

impl<M: ManageObject> Object<M> {
    /// Detaches the object from the [`Pool`].
    ///
    /// This reduces the size of the pool by one.
    pub fn detach(mut self) -> M::Object {
        // SAFETY: `state` is always `Some` when `Object` is owned.
        let mut o = self.state.take().unwrap().o;
        if let Some(pool) = self.pool.upgrade() {
            pool.detach_object(&mut o);
        }
        o
    }

    /// Returns the status of the object.
    pub fn status(&self) -> ObjectStatus {
        // SAFETY: `state` is always `Some` when `Object` is owned.
        self.state.as_ref().unwrap().status
    }
}

/// A wrapper of ObjectStatus that detaches the object from the pool when dropped.
struct UnreadyObject<M: ManageObject> {
    state: Option<ObjectState<M::Object>>,
    pool: Weak<Pool<M>>,
}

impl<M: ManageObject> Drop for UnreadyObject<M> {
    fn drop(&mut self) {
        if let Some(mut state) = self.state.take() {
            if let Some(pool) = self.pool.upgrade() {
                // Why not just call `pool.detach_object(&mut state.o)`?
                // 1. No need to release the permit because the object is not ready.
                // 2. No need to modify users because it's handle in the caller side.
                pool.slots.lock().current_size -= 1;
                pool.manager.on_detached(&mut state.o);
            }
        }
    }
}

impl<M: ManageObject> UnreadyObject<M> {
    fn ready(mut self) -> Object<M> {
        // SAFETY: `state` is always `Some` when `UnreadyObject` is owned.
        let state = Some(self.state.take().unwrap());
        let pool = self.pool.clone();
        Object { state, pool }
    }

    fn state(&mut self) -> &mut ObjectState<M::Object> {
        // SAFETY: `state` is always `Some` when `UnreadyObject` is owned.
        self.state.as_mut().unwrap()
    }
}

#[derive(Debug)]
struct ObjectState<T> {
    o: T,
    status: ObjectStatus,
}
