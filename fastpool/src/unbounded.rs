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

//! Unbounded object pools.

use std::collections::VecDeque;
use std::future::Future;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Weak;

use crate::mutex::Mutex;
use crate::ManageObject;
use crate::ObjectStatus;
use crate::QueueStrategy;

/// The configuration of [`Pool`].
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct PoolConfig {
    /// Queue strategy of the [`Pool`].
    ///
    /// Determines the order of objects being queued and dequeued.
    pub queue_strategy: QueueStrategy,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl PoolConfig {
    /// Creates a new [`PoolConfig`].
    pub fn new() -> Self {
        Self {
            queue_strategy: QueueStrategy::default(),
        }
    }

    /// Returns a new [`PoolConfig`] with the specified queue strategy.
    pub fn with_queue_strategy(mut self, queue_strategy: QueueStrategy) -> Self {
        self.queue_strategy = queue_strategy;
        self
    }
}

/// The current pool status.
///
/// See [`Pool::status`].
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct PoolStatus {
    /// The current size of the pool.
    pub current_size: usize,

    /// The number of idle objects in the pool.
    pub idle_count: usize,
}

/// The default [`ManageObject`] implementation for unbounded pool.
#[derive(Debug, Copy, Clone)]
pub struct NeverManageObject<T: Send + Sync> {
    _marker: std::marker::PhantomData<T>,
}

impl<T: Send + Sync> Default for NeverManageObject<T> {
    fn default() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

/// The error returned by [`NeverManageObject::create`].
pub struct PoolIsEmpty(());

impl std::fmt::Debug for PoolIsEmpty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unbounded pool is empty")
    }
}

impl std::fmt::Display for PoolIsEmpty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for PoolIsEmpty {}

impl<T: Send + Sync> ManageObject for NeverManageObject<T> {
    type Object = T;
    type Error = PoolIsEmpty;

    fn create(&self) -> impl Future<Output = Result<Self::Object, Self::Error>> + Send {
        std::future::ready(Err(PoolIsEmpty(())))
    }

    fn is_recyclable(
        &self,
        _: &mut Self::Object,
        _: &ObjectStatus,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }
}

/// Generic runtime-agnostic object pool for Async Rust.
///
/// You can use it for reusing objects that are expensive to create, like database connections.
///
/// Typically, this pool should be wrapped in an [`Arc`] in order to call [`Pool::get`]. This
/// is intended so that the user can leverage [`Arc::downgrade`] for running background
/// maintenance tasks.
pub struct Pool<T, M: ManageObject<Object = T> = NeverManageObject<T>> {
    config: PoolConfig,
    manager: M,

    /// A deque that holds the objects.
    slots: Mutex<PoolDeque<ObjectState<T>>>,
}

#[derive(Debug)]
struct PoolDeque<T> {
    deque: VecDeque<T>,
    current_size: usize,
}

impl<T, M> std::fmt::Debug for Pool<T, M>
where
    T: std::fmt::Debug,
    M: ManageObject<Object = T>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("slots", &self.slots)
            .field("config", &self.config)
            .finish()
    }
}

impl<T: Send + Sync> Pool<T> {
    /// Creates a new [`Pool`] from config and [`NeverManageObject`].
    pub fn from_config(config: PoolConfig) -> Arc<Self> {
        Self::new(config, NeverManageObject::<T>::default())
    }
}

impl<T, M: ManageObject<Object = T>> Pool<T, M> {
    /// Creates a new [`Pool`].
    pub fn new(config: PoolConfig, manager: M) -> Arc<Self> {
        let slots = Mutex::new(PoolDeque {
            deque: VecDeque::new(),
            current_size: 0,
        });

        Arc::new(Self {
            config,
            manager,
            slots,
        })
    }

    /// Retrieves an [`Object`] from this [`Pool`].
    ///
    /// This method should be called with an [`Arc`] of the pool.
    pub async fn get(self: &Arc<Self>) -> Result<Object<T, M>, M::Error> {
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

        Ok(object)
    }

    ///
    pub fn put(self: &Arc<Self>, o: T) {
        let mut slots = self.slots.lock();
        slots.current_size += 1;
        slots.deque.push_back(ObjectState {
            o,
            status: ObjectStatus::default(),
        });
        drop(slots);
    }

    /// Returns the current status of the pool.
    ///
    /// The status returned by the pool is not guaranteed to be consistent.
    ///
    /// While this features provides [eventual consistency], the numbers will be
    /// off when accessing the status of a pool under heavy load. These numbers
    /// are meant for an overall insight.
    ///
    /// [eventual consistency]: (https://en.wikipedia.org/wiki/Eventual_consistency)
    pub fn status(&self) -> PoolStatus {
        let slots = self.slots.lock();
        let (current_size, idle_count) = (slots.current_size, slots.deque.len());
        drop(slots);

        PoolStatus {
            current_size,
            idle_count,
        }
    }

    fn push_back(&self, o: ObjectState<T>) {
        let mut slots = self.slots.lock();
        slots.deque.push_back(o);
        drop(slots);
    }

    fn detach_object(&self, o: &mut T) {
        let mut slots = self.slots.lock();
        slots.current_size -= 1;
        drop(slots);
        self.manager.on_detached(o);
    }
}

/// A wrapper of the actual pooled object.
///
/// This object implements [`Deref`] and [`DerefMut`]. You can use it as if it was of type
/// `M::Object`.
///
/// This object implements [`Drop`] that returns the underlying object to the pool on drop. You may
/// call [`Object::detach`] to detach the object from the pool before dropping it.
pub struct Object<T, M: ManageObject<Object = T> = NeverManageObject<T>> {
    state: Option<ObjectState<T>>,
    pool: Weak<Pool<T, M>>,
}

impl<T, M> std::fmt::Debug for Object<T, M>
where
    T: std::fmt::Debug,
    M: ManageObject<Object = T>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Object")
            .field("state", &self.state)
            .finish()
    }
}

impl<T, M: ManageObject<Object = T>> Drop for Object<T, M> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            if let Some(pool) = self.pool.upgrade() {
                pool.push_back(state);
            }
        }
    }
}

impl<T, M: ManageObject<Object = T>> Deref for Object<T, M> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: `state` is always `Some` when `Object` is owned.
        &self.state.as_ref().unwrap().o
    }
}

impl<T, M: ManageObject<Object = T>> DerefMut for Object<T, M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: `state` is always `Some` when `Object` is owned.
        &mut self.state.as_mut().unwrap().o
    }
}

impl<T, M: ManageObject<Object = T>> AsRef<M::Object> for Object<T, M> {
    fn as_ref(&self) -> &M::Object {
        self
    }
}

impl<T, M: ManageObject<Object = T>> AsMut<M::Object> for Object<T, M> {
    fn as_mut(&mut self) -> &mut M::Object {
        self
    }
}

impl<T, M: ManageObject<Object = T>> Object<T, M> {
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
struct UnreadyObject<T, M: ManageObject<Object = T> = NeverManageObject<T>> {
    state: Option<ObjectState<T>>,
    pool: Weak<Pool<T, M>>,
}

impl<T, M: ManageObject<Object = T>> Drop for UnreadyObject<T, M> {
    fn drop(&mut self) {
        if let Some(mut state) = self.state.take() {
            if let Some(pool) = self.pool.upgrade() {
                pool.detach_object(&mut state.o);
            }
        }
    }
}

impl<T, M: ManageObject<Object = T>> UnreadyObject<T, M> {
    fn ready(mut self) -> Object<T, M> {
        // SAFETY: `state` is always `Some` when `UnreadyObject` is owned.
        let state = Some(self.state.take().unwrap());
        let pool = self.pool.clone();
        Object { state, pool }
    }

    fn state(&mut self) -> &mut ObjectState<T> {
        // SAFETY: `state` is always `Some` when `UnreadyObject` is owned.
        self.state.as_mut().unwrap()
    }
}

#[derive(Debug)]
struct ObjectState<T> {
    o: T,
    status: ObjectStatus,
}
