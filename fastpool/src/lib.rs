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

//! A fast object pool for Rust.
//!
//! # Example
//!
//! ```
//! use std::future::Future;
//!
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
//! impl fastpool::ManageObject for Manager {
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
//! let pool = fastpool::Pool::new(fastpool::PoolConfig::new(16), Manager);
//! let o = pool.get().await.unwrap();
//! assert_eq!(o.do_work().await, 42);
//! # }
//! ```

mod manage;
mod mutex;
mod pool;

pub use manage::ManageObject;
pub use manage::ObjectStatus;
pub use pool::Object;
pub use pool::Pool;
pub use pool::PoolConfig;
pub use pool::PoolStatus;
pub use pool::QueueStrategy;
pub use pool::RetainResult;
