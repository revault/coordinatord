use crate::db::maybe_create_db;
use std::{env, str::FromStr};

pub struct CoordinatordTestBuilder {
    pub postgres_config: tokio_postgres::Config,
}

impl CoordinatordTestBuilder {
    pub async fn new() -> Self {
        let postgres_config =
            tokio_postgres::Config::from_str(&env::var("POSTGRES_URI").unwrap_or(
                "postgresql://revault:revault@localhost:5432/fuzz_coordinator".to_string(),
            ))
            .unwrap();

        maybe_create_db(&postgres_config).await.unwrap();

        CoordinatordTestBuilder { postgres_config }
    }
}
