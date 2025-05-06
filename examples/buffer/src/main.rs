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

use std::convert::Infallible;
use std::future::Future;
use std::io::Write;
use std::time::Instant;

use fastpool::ObjectStatus;
use fastpool::unbounded::Pool;
use fastpool::unbounded::PoolConfig;

struct ManageBuffer;

impl fastpool::ManageObject for ManageBuffer {
    type Object = Vec<u8>;
    type Error = Infallible;

    fn create(&self) -> impl Future<Output = Result<Self::Object, Self::Error>> + Send {
        std::future::ready(Ok(vec![]))
    }

    fn is_recyclable(
        &self,
        _: &mut Self::Object,
        _: &ObjectStatus,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }
}

#[tokio::main]
async fn main() {
    manual_put_pool().await;
    auto_create_pool().await;
}

async fn manual_put_pool() {
    let pool = Pool::<Vec<u8>>::never_manage(PoolConfig::new());
    pool.put(Vec::with_capacity(1024));

    let mut buf = pool.get().await.unwrap();
    write!(&mut buf, "{:?} key=manual_put_pool_put", Instant::now()).unwrap();
    println!("{}", String::from_utf8_lossy(&buf));

    let mut buf = pool
        .get_or_create(async || Ok::<_, Infallible>(Vec::with_capacity(512)))
        .await
        .unwrap();
    write!(&mut buf, "{:?} key=manual_put_pool_create", Instant::now()).unwrap();
    println!("{}", String::from_utf8_lossy(&buf));
    assert_eq!(buf.capacity(), 512);
}

async fn auto_create_pool() {
    let pool = Pool::new(PoolConfig::new(), ManageBuffer);

    let mut buf = pool.get().await.unwrap();
    write!(&mut buf, "{:?} key=auto_create_pool_0", Instant::now()).unwrap();
    println!("{}", String::from_utf8_lossy(&buf));

    pool.put(Vec::with_capacity(1024));
    let mut buf = pool.get().await.unwrap();
    write!(&mut buf, "{:?} key=auto_create_pool_1", Instant::now()).unwrap();
    println!("{}", String::from_utf8_lossy(&buf));
    assert_eq!(buf.capacity(), 1024);
}
