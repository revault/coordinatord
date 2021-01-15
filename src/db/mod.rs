mod schema;
use revault_net::{
    bitcoin::{
        secp256k1::{PublicKey, Signature},
        OutPoint, Txid,
    },
    message::server::{RevaultSignature, Sigs},
};
use schema::SCHEMA;

use std::collections::HashMap;

use tokio_postgres::{types::Type, Client, NoTls};

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

pub async fn maybe_create_db(config: &tokio_postgres::Config) -> Result<(), tokio_postgres::Error> {
    let client = establish_connection(config).await?;

    client.batch_execute(SCHEMA).await?;

    Ok(())
}

pub async fn store_plaintext_sig(
    config: &tokio_postgres::Config,
    txid: Txid,
    pubkey: PublicKey,
    signature: Signature,
) -> Result<(), tokio_postgres::Error> {
    let client = establish_connection(config).await?;

    let statement = client
        .prepare_typed(
            "INSERT INTO signatures (txid, pubkey, signature) VALUES ($1, $2, $3)",
            &[Type::BYTEA, Type::BYTEA, Type::BYTEA],
        )
        .await?;
    client
        .execute(
            &statement,
            &[
                &txid.as_ref(),
                &pubkey.serialize().as_ref(),
                &signature.serialize_der().as_ref(),
            ],
        )
        .await?;

    Ok(())
}

pub async fn store_encrypted_sig(
    config: &tokio_postgres::Config,
    txid: Txid,
    pubkey: PublicKey,
    signature: Vec<u8>,
    encryption_pubkey: Vec<u8>,
) -> Result<(), tokio_postgres::Error> {
    let client = establish_connection(config).await?;

    let statement = client
        .prepare_typed(
            "INSERT INTO signatures (txid, pubkey, signature, encryption_key) \
             VALUES ($1, $2, $3, $4)",
            &[Type::BYTEA, Type::BYTEA, Type::BYTEA, Type::BYTEA],
        )
        .await?;
    client
        .execute(
            &statement,
            &[
                &txid.as_ref(),
                &pubkey.serialize().as_ref(),
                &signature,
                &encryption_pubkey,
            ],
        )
        .await?;

    Ok(())
}

pub async fn fetch_sigs(
    config: &tokio_postgres::Config,
    txid: Txid,
) -> Result<Sigs, tokio_postgres::Error> {
    let client = establish_connection(config).await?;
    let mut signatures: HashMap<PublicKey, RevaultSignature> = HashMap::new();

    let statement = client
        .prepare_typed(
            "SELECT pubkey, signature, encryption_key FROM signatures WHERE txid = $1",
            &[Type::BYTEA],
        )
        .await?;
    for row in client.query(&statement, &[&txid.as_ref()]).await? {
        let pubkey: Vec<u8> = row.get(0);
        let pubkey = PublicKey::from_slice(&pubkey).expect("We input a compressed pubkey");
        let sig: Vec<u8> = row.get(1);
        let encryption_key: Option<Vec<u8>> = row.get(2);

        if let Some(encryption_key) = encryption_key {
            signatures.insert(
                pubkey,
                RevaultSignature::EncryptedSig {
                    encrypted_signature: sig,
                    pubkey: encryption_key,
                },
            );
        } else {
            signatures.insert(
                pubkey,
                RevaultSignature::PlaintextSig(
                    Signature::from_der(&sig).expect("We input to_der()"),
                ),
            );
        }
    }

    Ok(Sigs { signatures })
}

pub async fn store_spend_tx(
    config: &tokio_postgres::Config,
    outpoint: OutPoint,
    transaction: Vec<u8>,
) -> Result<(), tokio_postgres::Error> {
    let client = establish_connection(config).await?;

    let statement = client
        .prepare_typed(
            "INSERT INTO spend_txs (deposit_txid, deposit_vout, transaction) VALUES ($1, $2, $3)",
            &[Type::BYTEA, Type::INT4, Type::BYTEA],
        )
        .await?;
    client
        .execute(
            &statement,
            &[
                &outpoint.txid.as_ref(),
                &(outpoint.vout as i32),
                &transaction,
            ],
        )
        .await?;

    Ok(())
}

pub async fn fetch_spend_tx(
    config: &tokio_postgres::Config,
    outpoint: OutPoint,
) -> Result<Option<Vec<u8>>, tokio_postgres::Error> {
    let client = establish_connection(config).await?;

    let statement = client
        .prepare_typed(
            "SELECT transaction FROM spend_txs WHERE deposit_txid = $1 AND deposit_vout = $2",
            &[Type::BYTEA, Type::INT4],
        )
        .await?;
    let spend_tx = client
        .query(
            &statement,
            &[&outpoint.txid.as_ref(), &(outpoint.vout as i32)],
        )
        .await?
        .iter()
        .next()
        .map(|row| row.get::<_, Vec<u8>>(0));

    Ok(spend_tx)
}
