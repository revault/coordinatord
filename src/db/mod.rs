mod schema;
use revault_net::{
    bitcoin::{
        consensus::encode,
        secp256k1::{PublicKey, Signature},
        OutPoint, Transaction as BitcoinTransaction, Txid,
    },
    message::coordinator::Sigs,
};
use schema::SCHEMA;

use std::{collections::BTreeMap, fmt};

use tokio_postgres::{error::SqlState, types::Type, Client, NoTls};

pub const DB_VERSION: i32 = 0;

#[derive(Debug)]
pub enum DbError {
    /// An error originating from the Postgres backend
    Postgres(tokio_postgres::Error),
    /// Trying to insert the same data twice
    Duplicate,
    WrongVersion(i32),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Postgres(e) => write!(f, "{}", e),
            Self::Duplicate => write!(f, "Trying to insert a duplicated entry"),
            Self::WrongVersion(v) => write!(
                f,
                "Unexpected database version, got: {}, expected: {}",
                v, DB_VERSION
            ),
        }
    }
}

impl std::error::Error for DbError {}

impl From<tokio_postgres::Error> for DbError {
    fn from(e: tokio_postgres::Error) -> Self {
        Self::Postgres(e)
    }
}

async fn establish_connection(
    config: &tokio_postgres::Config,
) -> Result<Client, tokio_postgres::Error> {
    let (client, connection) = config.connect(NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            log::error!("Database connection error: {}", e);
        }
    });

    Ok(client)
}

pub async fn maybe_create_db(config: &tokio_postgres::Config) -> Result<(), DbError> {
    let client = establish_connection(config).await?;

    client.batch_execute(SCHEMA).await?;
    let db_version = client.query_opt("SELECT version FROM VERSION", &[]).await?;
    if let Some(db_ver) = db_version {
        let db_ver = db_ver.get(0);
        if db_ver != DB_VERSION {
            return Err(DbError::WrongVersion(db_ver));
        }
    } else {
        // Alright, we just created the db. Let's insert the version...
        let statement = client
            .prepare_typed("INSERT INTO version(version) VALUES ($1)", &[Type::INT4])
            .await?;
        client.execute(&statement, &[&(DB_VERSION)]).await?;
    }

    Ok(())
}

pub async fn store_sig(
    config: &tokio_postgres::Config,
    txid: Txid,
    pubkey: PublicKey,
    signature: Signature,
) -> Result<(), DbError> {
    let client = establish_connection(config).await?;
    let sig = signature.serialize_der();

    let statement = client
        .prepare_typed(
            "INSERT INTO signatures (txid, pubkey, signature) VALUES ($1, $2, $3) \
             ON CONFLICT(txid, pubkey) DO UPDATE SET signature=$3",
            &[Type::BYTEA, Type::BYTEA, Type::BYTEA],
        )
        .await?;

    if let Err(e) = client
        .execute(
            &statement,
            &[&txid.as_ref(), &pubkey.serialize().as_ref(), &sig.as_ref()],
        )
        .await
    {
        log::debug!("We have a statement error in store_sig: {:?}", e);
        if let Some(e) = e.as_db_error() {
            log::debug!(
                "We have a db error {:?} with code {:?}",
                e.clone(),
                e.clone().code()
            );
            if *e.code() == SqlState::UNIQUE_VIOLATION {
                // Ah, it was trying to insert a signature that was there already.
                // Alright.
                return Err(DbError::Duplicate);
            }
        }
        return Err(e.into());
    }

    Ok(())
}

pub async fn fetch_sigs(
    config: &tokio_postgres::Config,
    txid: Txid,
) -> Result<Sigs, tokio_postgres::Error> {
    let client = establish_connection(config).await?;
    let mut signatures: BTreeMap<PublicKey, Signature> = BTreeMap::new();

    let statement = client
        .prepare_typed(
            "SELECT pubkey, signature FROM signatures WHERE txid = $1",
            &[Type::BYTEA],
        )
        .await?;
    for row in client.query(&statement, &[&txid.as_ref()]).await? {
        let pubkey: &[u8] = row.get(0);
        let pubkey = PublicKey::from_slice(&pubkey).expect("We input a compressed pubkey");
        let sig: Vec<u8> = row.get(1);

        signatures.insert(
            pubkey,
            Signature::from_der(&sig).expect("We input to_der()"),
        );
    }

    Ok(Sigs { signatures })
}

pub async fn store_spend_tx(
    config: &tokio_postgres::Config,
    outpoints: &Vec<OutPoint>,
    transaction: BitcoinTransaction,
) -> Result<(), tokio_postgres::Error> {
    let mut client = establish_connection(config).await?;
    let bitcoin_txid = encode::serialize(&transaction.txid());
    let bitcoin_tx = encode::serialize(&transaction);

    // In a single transaction,
    let db_tx = client.transaction().await?;

    // insert the Spend transaction,
    let statement = db_tx
        .prepare_typed(
            "INSERT INTO spend_txs (txid, transaction) VALUES ($1, $2) \
             ON CONFLICT DO NOTHING", // FIXME: we should make the error explicit
            &[Type::BYTEA, Type::BYTEA],
        )
        .await?;
    db_tx
        .execute(&statement, &[&bitcoin_txid, &bitcoin_tx])
        .await?;

    // as well as all vault outpoints it refers to
    for outpoint in outpoints.iter() {
        let statement = db_tx.prepare_typed(
        "INSERT INTO spend_outpoints (deposit_txid, deposit_vout, spend_txid) VALUES ($1, $2, $3) \
         ON CONFLICT (deposit_txid, deposit_vout) DO UPDATE \
         SET deposit_txid = EXCLUDED.deposit_txid, \
             deposit_vout = EXCLUDED.deposit_vout, \
             spend_txid = EXCLUDED.spend_txid",
        &[Type::BYTEA, Type::INT4, Type::BYTEA]
        ).await?;
        db_tx
            .execute(
                &statement,
                &[
                    &outpoint.txid.as_ref(),
                    &(outpoint.vout as i32),
                    &bitcoin_txid,
                ],
            )
            .await?;
    }

    db_tx.commit().await
}

pub async fn fetch_spend_tx(
    config: &tokio_postgres::Config,
    outpoint: &OutPoint,
) -> Result<Option<BitcoinTransaction>, tokio_postgres::Error> {
    let client = establish_connection(config).await?;

    let statement = client
        .prepare_typed(
            "SELECT transaction FROM spend_txs as txs \
             INNER JOIN spend_outpoints as ops ON txs.txid = ops.spend_txid \
             WHERE ops.deposit_txid = $1 AND ops.deposit_vout = $2",
            &[Type::BYTEA, Type::INT4],
        )
        .await?;
    let spend_tx = client
        .query(
            &statement,
            &[&outpoint.txid.as_ref(), &(outpoint.vout as i32)],
        )
        .await?
        .get(0)
        .map(|row| row.get::<_, Vec<u8>>(0));

    Ok(spend_tx.map(|tx| encode::deserialize(&tx).expect("Added to DB with serialize()")))
}

pub async fn fetch_spend_txs_to_broadcast(
    config: &tokio_postgres::Config,
) -> Result<Vec<BitcoinTransaction>, tokio_postgres::Error> {
    let client = establish_connection(config).await?;

    let statement = client
        .prepare_typed(
            "SELECT transaction FROM spend_txs WHERE broadcasted = FALSE",
            &[],
        )
        .await?;
    let spend_txs = client
        .query(&statement, &[])
        .await?
        .into_iter()
        .map(|row| row.get::<_, Vec<u8>>(0))
        .map(|tx| encode::deserialize(&tx).expect("Added to DB with serialize()"))
        .collect();

    Ok(spend_txs)
}

pub async fn mark_broadcasted_spend(
    config: &tokio_postgres::Config,
    txid: &Txid,
) -> Result<(), tokio_postgres::Error> {
    let client = establish_connection(config).await?;

    let statement = client
        .prepare_typed(
            "UPDATE spend_txs SET broadcasted = TRUE WHERE txid = $1",
            &[Type::BYTEA],
        )
        .await?;
    client.execute(&statement, &[&txid.as_ref()]).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db::*;

    use std::str::FromStr;

    use tokio::runtime::Builder as RuntimeBuilder;
    use tokio_postgres::tls::NoTls;

    async fn create_test_db() {
        let conf =
            tokio_postgres::Config::from_str("postgresql://revault:revault@localhost/postgres")
                .unwrap();

        let (client, connection) = conf.connect(NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("Database connection error: {}", e);
            }
        });

        client
            .batch_execute("DROP DATABASE IF EXISTS coordinatord_test_db;")
            .await
            .expect("dropping tables");
        client
            .batch_execute("CREATE DATABASE coordinatord_test_db;")
            .await
            .expect("creating tables");
    }

    async fn postgre_setup() -> tokio_postgres::Config {
        create_test_db().await;
        let conf = tokio_postgres::Config::from_str(
            "postgresql://revault:revault@localhost/coordinatord_test_db",
        )
        .unwrap();

        // Cleanup any leftover
        let (client, connection) = conf.connect(NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("Database connection error: {}", e);
            }
        });
        client
            .batch_execute("DROP TABLE IF EXISTS signatures; DROP TABLE IF EXISTS spend_outpoints; DROP TABLE IF EXISTS spend_txs; DROP TABLE IF EXISTS version;")
            .await
            .expect("dropping tables");

        conf
    }

    async fn postgre_teardown(conf: &tokio_postgres::Config) {
        let (client, connection) = conf.connect(NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("Database connection error: {}", e);
            }
        });
        client
            .batch_execute("DROP TABLE signatures; DROP TABLE spend_outpoints; DROP TABLE spend_txs; DROP TABLE version;")
            .await
            .expect("dropping tables");
    }

    #[test]
    fn db_version_check() {
        let rt = RuntimeBuilder::new_multi_thread()
            .enable_all()
            .thread_name("coordinatord_test_db")
            .build()
            .expect("Creating tokio runtime");

        async fn run_test() {
            let pg_config = postgre_setup().await;
            let client = establish_connection(&pg_config).await.unwrap();

            // The first call will create the DB
            assert!(
                client
                    .query_opt("SELECT version FROM VERSION", &[])
                    .await
                    .is_err(),
                "This relation shouldn't exist yet"
            );
            maybe_create_db(&pg_config).await.unwrap();
            assert!(
                client
                    .query_opt("SELECT version FROM VERSION", &[])
                    .await
                    .unwrap()
                    .is_some(),
                "It should have been populated already!"
            );

            // The second one will sanity check it's right
            maybe_create_db(&pg_config).await.unwrap();

            // We'll refuse to start if the version is from the future
            let statement = client
                .prepare_typed("UPDATE version SET version = 1", &[])
                .await
                .unwrap();
            client.execute(&statement, &[]).await.unwrap();
            assert!(maybe_create_db(&pg_config)
                .await
                .unwrap_err()
                .to_string()
                .contains("Unexpected database version"));

            postgre_teardown(&pg_config).await;
        }

        rt.block_on(run_test());
    }
}
