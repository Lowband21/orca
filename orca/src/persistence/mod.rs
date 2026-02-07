/// PostgreSQL persistence implementations for the queue service.
///
/// This module provides `PostgresQueueService`, a PostgreSQL-backed
/// implementation of the [`QueueService`] trait for durable job storage.
pub mod postgres;

pub use postgres::PostgresQueueService;
