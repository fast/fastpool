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

//! Bounded object pools.
//!
//! A bounded pool creates and recycles objects with full management. You _cannot_ put an object to
//! the pool manually.
//!
//! The pool is bounded by the `max_size` config option of [`PoolConfig`]. If the pool reaches the
//! maximum size, it will block all the [`Pool::get`] calls until an object is returned to the pool
//! or an object is detached from the pool.
//!
//! Typically, a bounded pool is used wrapped in an [`Arc`] in order to call [`Pool::get`].
//! This is intended so that users can leverage [`Arc::downgrade`] for running background
//! maintenance tasks (e.g., [`Pool::retain`]).
//!
//! Bounded pools are useful for pooling database connections.
//!
//! ## Examples
//!
//! Read the following simple demo or more complex examples in the examples directory.
//!
//! ```
//! use std::future::Future;
//!
//! use fastpool::ManageObject;
//! use fastpool::ObjectStatus;
//! use fastpool::bounded::Pool;
//! use fastpool::bounded::PoolConfig;
//!
//! struct Compute;
//! impl Compute {
//!     async fn do_work(&self) -> i32 {
//!         42
//!     }
//! }
//!
//! struct Manager;
//! impl ManageObject for Manager {
//!     type Object = Compute;
//!     type Error = ();
//!
//!     async fn create(&self) -> Result<Self::Object, Self::Error> {
//!         Ok(Compute)
//!     }
//!
//!     async fn is_recyclable(
//!         &self,
//!         o: &mut Self::Object,
//!         status: &ObjectStatus,
//!     ) -> Result<(), Self::Error> {
//!         Ok(())
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! let pool = Pool::new(PoolConfig::new(16), Manager);
//! let o = pool.get().await.unwrap();
//! assert_eq!(o.do_work().await, 42);
//! # }
//! ```

use std::collections::VecDeque;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use mea::semaphore::OwnedSemaphorePermit;
use mea::semaphore::Semaphore;

use crate::ManageObject;
use crate::ObjectStatus;
use crate::QueueStrategy;
use crate::RetainResult;
use crate::mutex::Mutex;
use crate::retain_spec;

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

/// The current pool status.
///
/// See [`Pool::status`].
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct PoolStatus {
    /// The maximum size of the pool.
    pub max_size: usize,

    /// The current size of the pool.
    pub current_size: usize,

    /// The number of idle objects in the pool.
    pub idle_count: usize,

    /// The number of futures waiting for an object.
    pub wait_count: usize,
}

/// Generic runtime-agnostic object pool with a maximum size.
///
/// See the [module level documentation](self) for more.
pub struct Pool<M: ManageObject> {
    config: PoolConfig,
    manager: M,

    /// A counter that tracks the sum of waiters + obtained objects.
    users: AtomicUsize,
    /// A semaphore that limits the maximum of users of the pool.
    permits: Arc<Semaphore>,
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
            .field("users", &self.users)
            .field("permits", &self.permits)
            .finish()
    }
}

impl<M: ManageObject> Pool<M> {
    /// Creates a new [`Pool`].
    pub fn new(config: PoolConfig, manager: M) -> Arc<Self> {
        let users = AtomicUsize::new(0);
        let permits = Arc::new(Semaphore::new(config.max_size));
        let slots = Mutex::new(PoolDeque {
            deque: VecDeque::with_capacity(config.max_size),
            current_size: 0,
            max_size: config.max_size,
        });

        Arc::new(Self {
            config,
            manager,
            users,
            permits,
            slots,
        })
    }

    /// Retrieves an [`Object`] from this [`Pool`].
    ///
    /// This method should be called with a pool wrapped in an [`Arc`]. If the pool reaches the
    /// maximum size, this method would block until an object is returned to the pool or an object
    /// is detached from the pool.
    pub async fn get(self: &Arc<Self>) -> Result<Object<M>, M::Error> {
        self.users.fetch_add(1, Ordering::Relaxed);
        let guard = scopeguard::guard((), |()| {
            self.users.fetch_sub(1, Ordering::Relaxed);
        });

        let permit = self.permits.clone().acquire_owned(1).await;

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
                        permit,
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
                        break unready_object.ready(permit);
                    }
                }
            };
        };

        scopeguard::ScopeGuard::into_inner(guard);
        Ok(object)
    }

    /// Retains only the objects that pass the given predicate.
    ///
    /// This function blocks the entire pool. Therefore, the given function should not block.
    ///
    /// The following example starts a background task that runs every 30 seconds and removes
    /// objects from the pool that have not been used for more than one minute. The task will
    /// terminate if the pool is dropped.
    ///
    /// ```rust,ignore
    /// let interval = Duration::from_secs(30);
    /// let max_age = Duration::from_secs(60);
    ///
    /// let weak_pool = Arc::downgrade(&pool);
    /// tokio::spawn(async move {
    ///     loop {
    ///         tokio::time::sleep(interval).await;
    ///         if let Some(pool) = weak_pool.upgrade() {
    ///             pool.retain(|_, status| status.last_used().elapsed() < max_age);
    ///         } else {
    ///             break;
    ///         }
    ///     }
    /// });
    /// ```
    pub fn retain(
        &self,
        f: impl FnMut(&mut M::Object, ObjectStatus) -> bool,
    ) -> RetainResult<M::Object> {
        let mut slots = self.slots.lock();
        let result = retain_spec::do_vec_deque_retain(&mut slots.deque, f);
        slots.current_size -= result.removed.len();
        result
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
        let (current_size, max_size) = (slots.current_size, slots.max_size);
        drop(slots);

        let users = self.users.load(Ordering::Relaxed);
        let (idle_count, wait_count) = if users < current_size {
            (current_size - users, 0)
        } else {
            (0, users - current_size)
        };

        PoolStatus {
            max_size,
            current_size,
            idle_count,
            wait_count,
        }
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

        self.users.fetch_sub(1, Ordering::Relaxed);
    }

    fn detach_object(&self, o: &mut M::Object, ready: bool) {
        let mut slots = self.slots.lock();

        assert!(
            slots.current_size <= slots.max_size,
            "invariant broken: current_size <= max_size (actual: {} <= {})",
            slots.current_size,
            slots.max_size,
        );

        slots.current_size -= 1;
        drop(slots);

        if ready {
            self.users.fetch_sub(1, Ordering::Relaxed);
        } else {
            // if the object is not ready, users count decrement is handled in the caller side,
            // that is, on exiting the `Pool::get` method.
        }
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
pub struct Object<M: ManageObject> {
    state: Option<ObjectState<M::Object>>,
    permit: OwnedSemaphorePermit,
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
            .field("permit", &self.permit)
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

impl<M: ManageObject> Deref for Object<M> {
    type Target = M::Object;
    fn deref(&self) -> &M::Object {
        // SAFETY: `state` is always `Some` when `Object` is owned.
        &self.state.as_ref().unwrap().o
    }
}

impl<M: ManageObject> DerefMut for Object<M> {
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
            pool.detach_object(&mut o, true);
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
                pool.detach_object(&mut state.o, false);
            }
        }
    }
}

impl<M: ManageObject> UnreadyObject<M> {
    fn ready(mut self, permit: OwnedSemaphorePermit) -> Object<M> {
        // SAFETY: `state` is always `Some` when `UnreadyObject` is owned.
        let state = Some(self.state.take().unwrap());
        let pool = self.pool.clone();
        Object {
            state,
            permit,
            pool,
        }
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

impl<T> retain_spec::SealedState for ObjectState<T> {
    type Object = T;

    fn status(&self) -> ObjectStatus {
        self.status
    }

    fn mut_object(&mut self) -> &mut Self::Object {
        &mut self.o
    }

    fn take_object(self) -> Self::Object {
        self.o
    }
}
