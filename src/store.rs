use sea_orm::{ConnectOptions, ConnectionTrait, Database, DatabaseConnection, Schema};

use crate::{
    Result,
    entity::{account, transaction},
};

/// Handle to the database. Cheap to clone (it shares a connection pool), so a
/// server can give every client connection its own copy.
///
/// Backend is chosen by URL scheme: `sqlite://…` or `postgres://…`.
#[derive(Clone, Debug)]
pub struct Store {
    db: DatabaseConnection,
}

impl Store {
    pub async fn connect(url: &str) -> Result<Self> {
        let mut opts = ConnectOptions::new(url);
        opts.sqlx_logging(false);
        if url.starts_with("sqlite") {
            // SQLite has a single writer anyway, and every in-memory
            // connection would otherwise be its own separate database.
            opts.max_connections(1);
        }
        Self::from_connection(Database::connect(opts).await?).await
    }

    /// Throwaway in-memory SQLite database.
    pub async fn in_memory() -> Result<Self> {
        Self::connect("sqlite::memory:").await
    }

    /// Wraps an existing connection and creates the tables if missing.
    pub async fn from_connection(db: DatabaseConnection) -> Result<Self> {
        let backend = db.get_database_backend();
        let schema = Schema::new(backend);
        for mut table in [
            schema.create_table_from_entity(account::Entity),
            schema.create_table_from_entity(transaction::Entity),
        ] {
            db.execute(table.if_not_exists()).await?;
        }
        Ok(Store { db })
    }

    pub fn db(&self) -> &DatabaseConnection {
        &self.db
    }
}
