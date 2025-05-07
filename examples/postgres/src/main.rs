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

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use fastpool::ObjectStatus;
use fastpool::bounded::Object;
use fastpool::bounded::Pool;
use fastpool::bounded::PoolConfig;
use futures::future::BoxFuture;
use sqlx::Acquire;
use sqlx::ConnectOptions;
use sqlx::Connection;
use sqlx::PgConnection;
use sqlx::TransactionManager;
use sqlx::postgres::PgConnectOptions;

#[derive(Debug, Clone)]
pub struct ConnectionPool {
    pool: Arc<Pool<ManageConnection>>,
}

impl ConnectionPool {
    pub fn new(option: PgConnectOptions, max_size: usize) -> Self {
        let pool = Pool::new(PoolConfig::new(max_size), ManageConnection { option });

        let weak_pool = Arc::downgrade(&pool);
        tokio::spawn(async move {
            const REAP_IDLE_INTERVAL: Duration = Duration::from_secs(60);
            const IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

            loop {
                tokio::time::sleep(REAP_IDLE_INTERVAL).await;
                if let Some(pool) = weak_pool.upgrade() {
                    pool.retain(|_, m| m.last_used().elapsed() < IDLE_TIMEOUT);

                    let status = pool.status();
                    let gap = status.max_size - status.current_size;
                    match pool.replenish(gap).await {
                        Ok(n) => println!("Replenished {n} connections"),
                        Err(err) => eprintln!("Failed to replenish connections: {err}"),
                    }
                } else {
                    break;
                }
            }
        });

        Self { pool }
    }

    pub async fn acquire(&self) -> Result<Object<ManageConnection>, sqlx::Error> {
        const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(60);

        tokio::time::timeout(ACQUIRE_TIMEOUT, self.pool.get())
            .await
            .unwrap_or_else(|_| Err(sqlx::Error::PoolTimedOut))
    }

    pub async fn begin(&self) -> Result<PostgresTransaction, sqlx::Error> {
        let mut conn = self.acquire().await?;
        <sqlx::Postgres as sqlx::Database>::TransactionManager::begin(&mut conn).await?;
        Ok(PostgresTransaction { conn, open: true })
    }
}

#[derive(Debug)]
pub struct ManageConnection {
    option: PgConnectOptions,
}

impl fastpool::ManageObject for ManageConnection {
    type Object = PgConnection;
    type Error = sqlx::Error;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        self.option.connect().await
    }

    async fn is_recyclable(
        &self,
        conn: &mut Self::Object,
        _: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        conn.ping().await
    }
}

#[derive(Debug)]
pub struct PostgresTransaction {
    conn: Object<ManageConnection>,
    open: bool,
}

impl Drop for PostgresTransaction {
    fn drop(&mut self) {
        if self.open {
            // starts a rollback operation

            // what this does depend on the database but generally this means we queue a rollback
            // operation that will happen on the next asynchronous invocation of the underlying
            // connection (including if the connection is returned to a pool)

            <sqlx::Postgres as sqlx::Database>::TransactionManager::start_rollback(&mut self.conn);
        }
    }
}

impl std::ops::Deref for PostgresTransaction {
    type Target = PgConnection;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl std::ops::DerefMut for PostgresTransaction {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

impl AsRef<PgConnection> for PostgresTransaction {
    fn as_ref(&self) -> &PgConnection {
        &self.conn
    }
}

impl AsMut<PgConnection> for PostgresTransaction {
    fn as_mut(&mut self) -> &mut PgConnection {
        &mut self.conn
    }
}

impl<'t> Acquire<'t> for &'t mut PostgresTransaction {
    type Database = sqlx::Postgres;

    type Connection = &'t mut PgConnection;

    #[inline]
    fn acquire(self) -> BoxFuture<'t, Result<Self::Connection, sqlx::Error>> {
        Box::pin(futures::future::ok(&mut **self))
    }

    #[inline]
    fn begin(self) -> BoxFuture<'t, Result<sqlx::Transaction<'t, sqlx::Postgres>, sqlx::Error>> {
        sqlx::Transaction::begin(&mut **self)
    }
}

impl PostgresTransaction {
    /// Commits this transaction or savepoint.
    pub async fn commit(mut self) -> Result<(), sqlx::Error> {
        <sqlx::Postgres as sqlx::Database>::TransactionManager::commit(&mut self.conn).await?;
        self.open = false;
        Ok(())
    }

    /// Aborts this transaction or savepoint.
    pub async fn rollback(mut self) -> Result<(), sqlx::Error> {
        <sqlx::Postgres as sqlx::Database>::TransactionManager::rollback(&mut self.conn).await?;
        self.open = false;
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let option = PgConnectOptions::from_str("postgres://localhost:5432/postgres").unwrap();
    let pool = ConnectionPool::new(option, 12);

    let mut txn = pool.begin().await.unwrap();
    let ret: i64 = sqlx::query_scalar("SELECT 1::INT8")
        .fetch_one(&mut *txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    println!("ret: {ret}");
}
