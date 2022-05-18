use crate::db::{DbConnection, DbError};
use revault_net::{
    bitcoin::{
        secp256k1::{PublicKey as SecpPubKey, Signature},
        OutPoint, Transaction as BitcoinTransaction, Txid,
    },
    message::{coordinator::*, RequestParams, ResponseResult},
    transport::KKTransport,
};
use std::collections::BTreeMap;

// Fetch the signatures from database for this txid
async fn get_sigs(conn: &DbConnection, params: &GetSigs) -> ResponseResult {
    match conn.fetch_sigs(params.txid).await {
        Ok(sigs) => ResponseResult::Sigs(sigs),
        Err(e) => {
            log::error!(
                "Error fetching signatures for '{:?}': '{}'. Returning an empty set.",
                params,
                e
            );
            ResponseResult::Sigs(Sigs {
                signatures: BTreeMap::new(),
            })
        }
    }
}

// Store this signature in the database
async fn set_sig(
    conn: &DbConnection,
    txid: Txid,
    pubkey: SecpPubKey,
    signature: Signature,
) -> ResponseResult {
    match conn.store_sig(txid, pubkey, signature).await {
        Ok(()) => ResponseResult::Sig(SigResult { ack: true }),
        Err(DbError::Duplicate) => {
            log::info!(
                "Ignoring duplicate (txid {}, pubkey {}, sig {})",
                txid,
                pubkey,
                signature
            );
            ResponseResult::Sig(SigResult { ack: true })
        }
        Err(e) => {
            log::error!("Error while storing Sig: '{}'", e);
            ResponseResult::Sig(SigResult { ack: false })
        }
    }
}

// Fetch the signatures from database for this txid
async fn set_spend_tx(
    conn: &mut DbConnection,
    deposits: &Vec<OutPoint>,
    spend_tx: BitcoinTransaction,
) -> ResponseResult {
    match conn.store_spend_tx(deposits, spend_tx).await {
        Ok(()) => ResponseResult::SetSpend(SetSpendResult { ack: true }),
        Err(e) => {
            log::error!("Error while storing Spend tx: '{}'", e);
            ResponseResult::SetSpend(SetSpendResult { ack: false })
        }
    }
}

// Get this Spend tx from database, and on error log and returns None for now (FIXME!).
async fn get_spend_tx(conn: &DbConnection, deposit: &OutPoint) -> Option<ResponseResult> {
    match conn.fetch_spend_tx(deposit).await {
        Ok(spend_tx) => Some(ResponseResult::SpendTx(SpendTx { spend_tx })),
        Err(e) => {
            log::error!("Error while fetching Spend tx: '{}'", e);
            None
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum MessageSender {
    Manager,
    StakeHolder,
    ManagerStakeholder,
    WatchTower,
}

/// Read a message from this stream, and process it depending on the sender.
pub async fn read_req(
    conn: &mut DbConnection,
    stream: &mut KKTransport,
    sender: MessageSender,
) -> Result<(), revault_net::Error> {
    log::trace!("Reading a new message from '{:?}'", sender);

    stream
        .read_req_async(|req_params| process_request(conn, sender, req_params))
        .await
}

pub async fn process_request(
    conn: &mut DbConnection,
    sender: MessageSender,
    req_params: RequestParams,
) -> Option<ResponseResult> {
    match req_params {
        // The get_sigs message can only be sent by a participant's wallet
        RequestParams::GetSigs(ref msg) => {
            if matches!(
                sender,
                MessageSender::Manager
                    | MessageSender::StakeHolder
                    | MessageSender::ManagerStakeholder
            ) {
                Some(get_sigs(conn, msg).await)
            } else {
                log::error!("Unexpected get_sigs '{:?}' from '{:?}'", req_params, sender);
                None
            }
        }
        // The sig message can only be sent by a stakeholder's wallet
        RequestParams::CoordSig(Sig {
            txid,
            pubkey,
            signature,
        }) => {
            if matches!(
                sender,
                MessageSender::StakeHolder | MessageSender::ManagerStakeholder
            ) {
                Some(set_sig(conn, txid, pubkey, signature).await)
            } else {
                log::error!("Unexpected sig '{:?}' from '{:?}'", req_params, sender);
                None
            }
        }
        // The set_spend_tx message may only be sent by a manager's wallet
        RequestParams::SetSpendTx(msg) => {
            if matches!(
                sender,
                MessageSender::Manager | MessageSender::ManagerStakeholder
            ) {
                Some(set_spend_tx(conn, &msg.deposit_outpoints.clone(), msg.spend_tx()).await)
            } else {
                log::error!("Unexpected set_spend_tx from '{:?}'", sender);
                None
            }
        }
        // The get_spend_tx message may only be sent by a watchtower
        // TODO: we may have to accept it from wallets too, as they may want to inspect it too.
        RequestParams::GetSpendTx(GetSpendTx { deposit_outpoint }) => {
            if matches!(sender, MessageSender::WatchTower) {
                get_spend_tx(conn, &deposit_outpoint).await
            } else {
                log::error!(
                    "Unexpected get_spend_tx '{:?}' from '{:?}'",
                    req_params,
                    sender
                );
                None
            }
        }
        // Any other message is not for us
        _ => {
            log::error!(
                "Received unexpected request from stakeholder: '{:?}'",
                req_params
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::db::*;
    use crate::processing::{get_sigs, get_spend_tx, set_sig, set_spend_tx};

    use revault_net::{
        bitcoin::{
            hashes::hex::FromHex,
            secp256k1::{PublicKey, Signature},
            OutPoint, Txid,
        },
        message::{coordinator::*, ResponseResult},
    };
    use revault_tx::transactions::{RevaultTransaction, SpendTransaction};

    use std::{collections::BTreeMap, str::FromStr};

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
            .batch_execute("DROP DATABASE IF EXISTS coordinatord_test;")
            .await
            .expect("dropping tables");
        client
            .batch_execute("CREATE DATABASE coordinatord_test;")
            .await
            .expect("dropping tables");
    }

    async fn postgre_setup() -> tokio_postgres::Config {
        create_test_db().await;
        let conf = tokio_postgres::Config::from_str(
            "postgresql://revault:revault@localhost/coordinatord_test",
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

        // So this becomes *actually* create DB
        maybe_create_db(&conf).await.expect("Creating tables.");

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

    async fn sig_exchange(pg_config: &tokio_postgres::Config) {
        let db_conn = DbConnection::new(pg_config).await.unwrap();
        let signature_a = Signature::from_compact(&[
            0xdc, 0x4d, 0xc2, 0x64, 0xa9, 0xfe, 0xf1, 0x7a, 0x3f, 0x25, 0x34, 0x49, 0xcf, 0x8c,
            0x39, 0x7a, 0xb6, 0xf1, 0x6f, 0xb3, 0xd6, 0x3d, 0x86, 0x94, 0x0b, 0x55, 0x86, 0x82,
            0x3d, 0xfd, 0x02, 0xae, 0x3b, 0x46, 0x1b, 0xb4, 0x33, 0x6b, 0x5e, 0xcb, 0xae, 0xfd,
            0x66, 0x27, 0xaa, 0x92, 0x2e, 0xfc, 0x04, 0x8f, 0xec, 0x0c, 0x88, 0x1c, 0x10, 0xc4,
            0xc9, 0x42, 0x8f, 0xca, 0x69, 0xc1, 0x32, 0xa2,
        ])
        .unwrap();
        // Inputs aren't checked, we could have fed it garbage
        let pubkey_a = PublicKey::from_slice(&[
            0x02, 0xc6, 0x6e, 0x7d, 0x89, 0x66, 0xb5, 0xc5, 0x55, 0xaf, 0x58, 0x05, 0x98, 0x9d,
            0xa9, 0xfb, 0xf8, 0xdb, 0x95, 0xe1, 0x56, 0x31, 0xce, 0x35, 0x8c, 0x3a, 0x17, 0x10,
            0xc9, 0x62, 0x67, 0x90, 0x63,
        ])
        .unwrap();
        let txid_a =
            Txid::from_hex("264595a4ace1865dfa442bb923320b8f00413711655165ac13a470db2c5384c0")
                .unwrap();

        assert_eq!(
            set_sig(&db_conn, txid_a, pubkey_a, signature_a.clone()).await,
            ResponseResult::Sig(SigResult { ack: true })
        );

        let pubkey_b = PublicKey::from_str(
            "03ffae85b76dd0dd96cbf23348fb398ab93274466759201ecf29d0f68ddd9d1b6c",
        )
        .unwrap();
        let txid_b =
            Txid::from_hex("ead1ff4c948a4993097647b84cd0aa80d3205cc8ddcd19b8aca154743c2e5cec")
                .unwrap();
        let signature_b = Signature::from_str("304402204b0ab8a7d95d5b67d5c1b8584a3075adcac787a315f79a9b52b5a736909c975502206def9036d3d980a7cb66f2baa64ebdcd6648d70b324c6c18c349fa240dd07ca8").unwrap();
        assert_eq!(
            set_sig(&db_conn, txid_b, pubkey_b, signature_b.clone()).await,
            ResponseResult::Sig(SigResult { ack: true })
        );

        // Now fetch the sigs we just stored
        let mut signatures_a = BTreeMap::new();
        signatures_a.insert(pubkey_a, signature_a);
        assert_eq!(
            get_sigs(&db_conn, &GetSigs { txid: txid_a }).await,
            ResponseResult::Sigs(Sigs {
                signatures: signatures_a.clone()
            })
        );

        let mut signatures_b = BTreeMap::new();
        signatures_b.insert(pubkey_b, signature_b);
        assert_eq!(
            get_sigs(&db_conn, &GetSigs { txid: txid_b }).await,
            ResponseResult::Sigs(Sigs {
                signatures: signatures_b.clone()
            })
        );

        // We can store a different signature for the same pubkey...
        let another_sig_a = Signature::from_str("304402201a3109a4a6445c1e56416bc39520aada5c8ad089e69ee4f1a40a0901de1a435302204b281ba97da2ab2e40eb65943ae414cc4307406c5eb177b1c646606839a2e99d").unwrap();
        assert_eq!(
            set_sig(&db_conn, txid_a, pubkey_a, another_sig_a.clone()).await,
            ResponseResult::Sig(SigResult { ack: true })
        );

        // ...the db will just update it
        let mut signatures_a = BTreeMap::new();
        signatures_a.insert(pubkey_a, another_sig_a);
        assert_eq!(
            get_sigs(&db_conn, &GetSigs { txid: txid_a }).await,
            ResponseResult::Sigs(Sigs {
                signatures: signatures_a.clone()
            })
        );

        // We can add more sigs for the same txid, and we'll get all of them
        let pubkey_c = PublicKey::from_str(
            "028c887a4a78211ff320802134046cb1db92215614ac0a078c261ed860f3067f0f",
        )
        .unwrap();
        let signature_c = Signature::from_str("304402201fbe986a41b69ea65bbb94a042cb6a5edacb898f290c76d76deb5d74241d0309022065d5ad54a36962b75857ce22ddf2189e71e5a0fe6df6e6d5d0c8acdb59e16374").unwrap();
        assert_eq!(
            set_sig(&db_conn, txid_b, pubkey_c, signature_c.clone()).await,
            ResponseResult::Sig(SigResult { ack: true })
        );

        let pubkey_d = PublicKey::from_str(
            "02a6d7ef3ffce87bec86fbd80ca510a72abe2c5ac4842fa6308eec8ba457dc66ae",
        )
        .unwrap();
        let signature_d = Signature::from_str("30440220197a312ee648b762ed795c686217f79b1b825d80bfb87f7c5e387cd713e3a026022077fb9114caafcd6a0d2362cb4eaddb19b5ea8c0fb37b257338d4dfbce239ee9e").unwrap();
        assert_eq!(
            set_sig(&db_conn, txid_b, pubkey_d, signature_d.clone()).await,
            ResponseResult::Sig(SigResult { ack: true })
        );

        signatures_b.insert(pubkey_c, signature_c);
        signatures_b.insert(pubkey_d, signature_d);
        assert_eq!(
            get_sigs(&db_conn, &GetSigs { txid: txid_b }).await,
            ResponseResult::Sigs(Sigs {
                signatures: signatures_b.clone()
            })
        );
    }

    async fn spend_tx_exchange(pg_config: &tokio_postgres::Config) {
        let mut db_conn = DbConnection::new(pg_config).await.unwrap();
        let spend_psbt_str = "\"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\"";
        let spend_tx: SpendTransaction = serde_json::from_str(&spend_psbt_str).unwrap();
        let spend_tx = spend_tx.into_psbt().extract_tx();
        let deposit_outpoints = vec![OutPoint::from_str(
            "4e37824b0bd0843bb94c290956374ffa1752d4c6bc9089fcbd20e1e63518b25e:0",
        )
        .unwrap()];

        // We still haven't stored the spend
        let deposit_outpoint = deposit_outpoints[0];
        let received = get_spend_tx(&db_conn, &deposit_outpoint).await.unwrap();
        assert_eq!(
            received,
            ResponseResult::SpendTx(SpendTx { spend_tx: None })
        );

        assert_eq!(
            set_spend_tx(&mut db_conn, &deposit_outpoints, spend_tx.clone()).await,
            ResponseResult::SetSpend(SetSpendResult { ack: true })
        );

        let received = get_spend_tx(&db_conn, &deposit_outpoint).await.unwrap();
        assert_eq!(
            received,
            ResponseResult::SpendTx(SpendTx {
                spend_tx: Some(spend_tx)
            })
        );

        // If a new one is set with a conflicting outpoint, it'll just get overriden
        let second_spend_tx = SpendTransaction::from_psbt_str("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").unwrap();
        let second_spend_tx = second_spend_tx.into_psbt().extract_tx();
        let conflicting_deposit_outpoints = vec![
            deposit_outpoint,
            OutPoint::from_str(
                "dbf7040be3ce465638373f48fb681bf3ae334691c328294f908baadfb927e942:1",
            )
            .unwrap(),
        ];
        assert_eq!(
            set_spend_tx(
                &mut db_conn,
                &conflicting_deposit_outpoints,
                second_spend_tx.clone()
            )
            .await,
            ResponseResult::SetSpend(SetSpendResult { ack: true })
        );
        let received = get_spend_tx(&db_conn, &deposit_outpoint).await.unwrap();
        assert_eq!(
            received,
            ResponseResult::SpendTx(SpendTx {
                spend_tx: Some(second_spend_tx.clone())
            })
        );

        // And we can even set the same Spend again, it will just do nothing
        // FIXME: it should err explicitly
        assert_eq!(
            set_spend_tx(
                &mut db_conn,
                &conflicting_deposit_outpoints,
                second_spend_tx.clone()
            )
            .await,
            ResponseResult::SetSpend(SetSpendResult { ack: true })
        );
        let received = get_spend_tx(&db_conn, &deposit_outpoint).await.unwrap();
        assert_eq!(
            received,
            ResponseResult::SpendTx(SpendTx {
                spend_tx: Some(second_spend_tx)
            })
        );
    }

    #[test]
    pub fn test_message_processing() {
        let rt = RuntimeBuilder::new_multi_thread()
            .enable_all()
            .thread_name("coordinatord_test")
            .build()
            .expect("Creating tokio runtime");

        async fn run_tests() {
            let pg_config = postgre_setup().await;
            sig_exchange(&pg_config).await;
            postgre_teardown(&pg_config).await;

            let pg_config = postgre_setup().await;
            spend_tx_exchange(&pg_config).await;
            postgre_teardown(&pg_config).await;
        }

        rt.block_on(run_tests());
    }
}
