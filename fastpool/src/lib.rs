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

#![deny(missing_docs)]

//! Fastpool provides fast and runtime-agnostic object pools for Async Rust.
//!
//! This crate provides two implementations: [bounded pool](bounded::Pool) and
//! [unbounded pool](unbounded::Pool).
//!
//! # Bounded pool
//!
//! A bounded pool creates and recycles objects with full management. You _cannot_ put an object to
//! the pool manually.
//!
//! The pool is bounded by the `max_size` config option of [`PoolConfig`](bounded::PoolConfig). If
//! the pool reaches the maximum size, it will block all the [`Pool::get`](bounded::Pool::get) calls
//! until an object is returned to the pool or an object is detached from the pool.
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
//! use fastpool::bounded::Pool;
//! use fastpool::bounded::PoolConfig;
//! use fastpool::ManageObject;
//! use fastpool::ObjectStatus;
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
//!
//! # Unbounded pool
//!
//! An unbounded pool, on the other hand, allows you to put objects to the pool manually. You can
//! use it like Go's [`sync.Pool`](https://pkg.go.dev/sync#Pool).
//!
//! To configure a factory for creating objects when the pool is empty, like `sync.Pool`'s `New`,
//! you can create the unbounded pool via [`Pool::new`](unbounded::Pool::new) with an
//! implementation of [`ManageObject`].
//!
//! ## Examples
//!
//! Read the following simple demo or more complex examples in the examples directory.
//!
//! ```
//! use fastpool::unbounded::Pool;
//! use fastpool::unbounded::PoolConfig;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let pool = Pool::<Vec<u8>>::from_config(PoolConfig::default());
//!
//! let result = pool.get().await;
//! assert_eq!(result.unwrap_err().to_string(), "unbounded pool is empty");
//!
//! pool.put(Vec::with_capacity(1024));
//! let o = pool.get().await.unwrap();
//! assert_eq!(o.capacity(), 1024);
//! # }
//! ```
//!
//! # FAQ
//!
//! ## Why does fastpool have no timeout config?
//!
//! Many async object pool implementations allow you to configure multiple timeout, like "wait
//! timeout", "create timeout", "recycle timeout", etc.
//!
//! This introduces two major problems:
//!
//! First, to support timeouts, the pool must use with a timer implementation like `tokio::time`.
//! This would prevent the pool from being runtime-agnostic. Theoretically, the pool can depend
//! on a timer trait, but there is no such a standard trait in the Rust ecosystem yet.
//!
//! Second, timeouts options not only add complexity for configuration, but also the value itself
//! cannot be configured properly at all. For example, end users often care about the total time
//! used to obtain an object. This is not solely "wait timeout", "create timeout", or
//! "recycle timeout", but a condition composition of all internal operations.
//!
//! Thus, we propose a caller-side timeout solution:
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! use fastpool::bounded::Object;
//! use fastpool::bounded::Pool;
//!
//! #[derive(Debug, Clone)]
//! pub struct ConnectionPool {
//!     pool: Arc<Pool<ManageConnection>>,
//! }
//!
//! impl ConnectionPool {
//!     pub async fn acquire(&self) -> Result<Object<ManageConnection>, Error> {
//!         const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(60);
//!
//!         // note that users can choose any timer implementation here
//!         let result = tokio::time::timeout(ACQUIRE_TIMEOUT, self.pool.get()).await;
//!
//!         // ... processing the result
//!     }
//! }
//! ```
//!
//! Check out the postgres example in the examples directory for the complete code.
//!
//! ## Why does fastpool have no before/after hooks?
//!
//! Similar to the second point of the previous question, the before/after hooks (closures) are
//! very hard to configure properly. Specific to closures, many Rust code can be easily written
//! in place, but if you'd like to pass a code block as a closure, then you may encounter a lot of
//! lifetime and ownership issues. Besides, how to handle `Result` in the closure is also a
//! headache.
//!
//! Fastpool provides an ordinary interface of object pools, so that you should be able to add
//! any before/after logic in the implementation or using a wrapper.
//!
//! For example, all the "post-create", "pre-recycle", and "post-recycle" hooks can be implemented
//! as:
//!
//! ```
//! use std::future::Future;
//!
//! use fastpool::ManageObject;
//! use fastpool::ObjectStatus;
//!
//! struct Manager;
//! impl ManageObject for Manager {
//!     type Object = i32;
//!     type Error = std::convert::Infallible;
//!
//!     async fn create(&self) -> Result<Self::Object, Self::Error> {
//!         let o = 42;
//!         // any post-create hooks
//!         Ok(o)
//!     }
//!
//!     async fn is_recyclable(
//!         &self,
//!         _o: &mut Self::Object,
//!         _status: &ObjectStatus,
//!     ) -> Result<(), Self::Error> {
//!         // any pre-recycle hooks
//!         // determinate if is_recyclable
//!         // any post-recycle hooks
//!         Ok(())
//!     }
//! }
//! ```

pub use common::ManageObject;
pub use common::ObjectStatus;
pub use common::QueueStrategy;

mod common;
mod mutex;

pub mod bounded;
pub mod unbounded;
