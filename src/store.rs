use sea_orm::{ConnectOptions, ConnectionTrait, Database, DatabaseConnection, Schema};

use crate::{
    Result,
    entity::{account, transaction},
};

/// Handle to the database.
///
/// Backend is chosen by URL scheme: `sqlite://…` or `postgres://…`.
#[derive(Clone, Debug)]
pub struct Store {
    db: DatabaseConnection,
}

impl Store {
    #[tracing::instrument(skip_all, fields(backend = scheme(url)))]
    pub async fn connect(url: &str) -> Result<Self> {
        let mut opts = ConnectOptions::new(url);

        // Set RUST_LOG=...,sqlx=debug` if you want all SQL calls. I wouldn't.
        opts.sqlx_logging(tracing::enabled!(target: "sqlx::query", tracing::Level::DEBUG))
            .sqlx_logging_level(log::LevelFilter::Debug);

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

    /// Tables are created IF NOT EXISTS. No migrations for now.
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

    pub(crate) fn db(&self) -> &DatabaseConnection {
        &self.db
    }
}

/// URL scheme only (`sqlite`, `postgres`), safe to log.
fn scheme(url: &str) -> &str {
    url.split_once(':').map_or("unknown", |(scheme, _)| scheme)
}

#[cfg(test)]
mod tests {
    use super::scheme;

    #[test]
    fn scheme_hides_credentials() {
        assert_eq!(scheme("postgres://user:secret@host/db"), "postgres");
        assert_eq!(scheme("sqlite::memory:"), "sqlite");
        assert_eq!(scheme("garbage"), "unknown");
    }
}
