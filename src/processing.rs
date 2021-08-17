use crate::db::{fetch_sigs, fetch_spend_tx, store_sig, store_spend_tx};
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
async fn get_sigs(pg_config: &tokio_postgres::Config, params: &GetSigs) -> ResponseResult {
    match fetch_sigs(pg_config, params.id).await {
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
    pg_config: &tokio_postgres::Config,
    txid: Txid,
    pubkey: SecpPubKey,
    signature: Signature,
) -> ResponseResult {
    match store_sig(&pg_config, txid, pubkey, signature).await {
        Ok(()) => ResponseResult::Sig(SigResult { ack: true }),
        Err(e) => {
            log::error!("Error while storing Sig: '{}'", e);
            ResponseResult::Sig(SigResult { ack: false })
        }
    }
}

// Fetch the signatures from database for this txid
async fn set_spend_tx(
    pg_config: &tokio_postgres::Config,
    deposits: &Vec<OutPoint>,
    spend_tx: BitcoinTransaction,
) -> ResponseResult {
    match store_spend_tx(pg_config, deposits, spend_tx).await {
        Ok(()) => ResponseResult::SetSpend(SetSpendResult { ack: true }),
        Err(e) => {
            log::error!("Error while storing Spend tx: '{}'", e);
            ResponseResult::SetSpend(SetSpendResult { ack: false })
        }
    }
}

// Get this Spend tx from database, and on error log and returns None for now (FIXME!).
async fn get_spend_tx(
    pg_config: &tokio_postgres::Config,
    deposit: &OutPoint,
) -> Option<ResponseResult> {
    match fetch_spend_tx(pg_config, deposit).await {
        Ok(transaction) => Some(ResponseResult::SpendTx(SpendTx { transaction })),
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
    pg_config: &tokio_postgres::Config,
    stream: &mut KKTransport,
    sender: MessageSender,
) -> Result<(), revault_net::Error> {
    log::trace!("Reading a new message from '{:?}'", sender);

    stream
        .read_req_async(|req_params| async {
            match req_params {
                // The get_sigs message can only be sent by a participant's wallet
                RequestParams::GetSigs(ref msg) => {
                    if matches!(
                        sender,
                        MessageSender::Manager
                            | MessageSender::StakeHolder
                            | MessageSender::ManagerStakeholder
                    ) {
                        Some(get_sigs(pg_config, msg).await)
                    } else {
                        log::error!("Unexpected get_sigs '{:?}' from '{:?}'", req_params, sender);
                        None
                    }
                }
                // The sig message can only be sent by a stakeholder's wallet
                RequestParams::CoordSig(Sig {
                    id,
                    pubkey,
                    signature,
                }) => {
                    if matches!(
                        sender,
                        MessageSender::StakeHolder | MessageSender::ManagerStakeholder
                    ) {
                        Some(set_sig(pg_config, id, pubkey, signature).await)
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
                        Some(
                            set_spend_tx(pg_config, &msg.deposit_outpoints.clone(), msg.spend_tx())
                                .await,
                        )
                    } else {
                        log::error!("Unexpected set_spend_tx from '{:?}'", sender);
                        None
                    }
                }
                // The get_spend_tx message may only be sent by a watchtower
                // TODO: we may have to accept it from wallets too, as they may want to inspect it too.
                RequestParams::GetSpendTx(GetSpendTx { deposit_outpoint }) => {
                    if matches!(sender, MessageSender::WatchTower) {
                        get_spend_tx(pg_config, &deposit_outpoint).await
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
        })
        .await
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
            set_sig(pg_config, txid_a, pubkey_a, signature_a.clone()).await,
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
            set_sig(pg_config, txid_b, pubkey_b, signature_b.clone()).await,
            ResponseResult::Sig(SigResult { ack: true })
        );

        // Now fetch the sigs we just stored
        let mut signatures_a = BTreeMap::new();
        signatures_a.insert(pubkey_a, signature_a);
        assert_eq!(
            get_sigs(pg_config, &GetSigs { id: txid_a }).await,
            ResponseResult::Sigs(Sigs {
                signatures: signatures_a.clone()
            })
        );

        let mut signatures_b = BTreeMap::new();
        signatures_b.insert(pubkey_b, signature_b);
        assert_eq!(
            get_sigs(pg_config, &GetSigs { id: txid_b }).await,
            ResponseResult::Sigs(Sigs {
                signatures: signatures_b.clone()
            })
        );

        // We can add more sigs for the same txid, and we'll get all of them
        let pubkey_c = PublicKey::from_str(
            "028c887a4a78211ff320802134046cb1db92215614ac0a078c261ed860f3067f0f",
        )
        .unwrap();
        let signature_c = Signature::from_str("304402201fbe986a41b69ea65bbb94a042cb6a5edacb898f290c76d76deb5d74241d0309022065d5ad54a36962b75857ce22ddf2189e71e5a0fe6df6e6d5d0c8acdb59e16374").unwrap();
        assert_eq!(
            set_sig(pg_config, txid_b, pubkey_c, signature_c.clone()).await,
            ResponseResult::Sig(SigResult { ack: true })
        );

        let pubkey_d = PublicKey::from_str(
            "02a6d7ef3ffce87bec86fbd80ca510a72abe2c5ac4842fa6308eec8ba457dc66ae",
        )
        .unwrap();
        let signature_d = Signature::from_str("30440220197a312ee648b762ed795c686217f79b1b825d80bfb87f7c5e387cd713e3a026022077fb9114caafcd6a0d2362cb4eaddb19b5ea8c0fb37b257338d4dfbce239ee9e").unwrap();
        assert_eq!(
            set_sig(pg_config, txid_b, pubkey_d, signature_d.clone()).await,
            ResponseResult::Sig(SigResult { ack: true })
        );

        signatures_b.insert(pubkey_c, signature_c);
        signatures_b.insert(pubkey_d, signature_d);
        assert_eq!(
            get_sigs(pg_config, &GetSigs { id: txid_b }).await,
            ResponseResult::Sigs(Sigs {
                signatures: signatures_b.clone()
            })
        );
    }

    async fn spend_tx_exchange(pg_config: &tokio_postgres::Config) {
        let spend_psbt_str = "\"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\"";
        let spend_tx: SpendTransaction = serde_json::from_str(&spend_psbt_str).unwrap();
        let spend_tx = spend_tx.into_psbt().extract_tx();
        let deposit_outpoints = vec![OutPoint::from_str(
            "4e37824b0bd0843bb94c290956374ffa1752d4c6bc9089fcbd20e1e63518b25e:0",
        )
        .unwrap()];

        // We still haven't stored the spend
        let deposit_outpoint = deposit_outpoints[0];
        let received = get_spend_tx(pg_config, &deposit_outpoint).await.unwrap();
        assert_eq!(
            received,
            ResponseResult::SpendTx(SpendTx {
                transaction: None,
            })
        );

        assert_eq!(
            set_spend_tx(pg_config, &deposit_outpoints, spend_tx.clone()).await,
            ResponseResult::SetSpend(SetSpendResult { ack: true })
        );

        let received = get_spend_tx(pg_config, &deposit_outpoint).await.unwrap();
        assert_eq!(
            received,
            ResponseResult::SpendTx(SpendTx {
                transaction: Some(spend_tx)
            })
        );

        // If a new one is set with a conflicting outpoint, it'll just get overriden
        let second_spend_tx = SpendTransaction::from_psbt_str("cHNidP8BAGcCAAAAATJj+J05C8NjU6aFkbjH+AlpaAqUSHqsYmvdXXsC6k0XAAAAAADOYAAAAoAyAAAAAAAAIgAgS4/3QaTXSQuvlpDk4z6xdM4cKh4nMpTnhF0HmaQWsu+gjAIAAAAAAAAAAAAAAAEBK0ANAwAAAAAAIgAg3GSr/0q6qUaIuNJEdndSJ2sKFlDccx5CFx4SZ2spL3wBCP2GAQUASDBFAiEApjf0AqotFH4ffzLCB3JKsbda8Ni3v+oad/gHQCUQy5UCIF9IIaPpmwl3uQT6A5CCBeqUW+fwWL0DLEb3Yke/+G8wAUYwQwIfAXs8XkbDD0WccmcLL7lHdezsQjo40ILZHeiI+zn6nwIgdIjHwGU3bMhFSzk23A21zaQQQfcoRpaLqAwEot7jshYBSDBFAiEA6RwcVU0HdHIXy+/Wh7vXGsSbbUsJ3lXqC3AjApSFcAQCIAqwY2ZnRwXcZA53HWYhKpUUwlPVlhHnMZHREccAx4+UAaohA8ujblABMfWi8DaUwzeN+ttu2AppH8zdsD1K/WY8bMnUrFGHZHapFEEQ586S5hPnp11w9epOlCzJEz84iKxrdqkUUwKW1Yzw4enIBR/m4J62xDYUTI6IrGyTUodnUiEDcMBgveHhyiayIeeNy0b54/FpAEo54BLxJK8GHTVomi0hA2LsGliO85N/vTQGAUbHRf6D0D72NbUQPhznA+1bfyNKUq8CzmCyaAABASUhA8ujblABMfWi8DaUwzeN+ttu2AppH8zdsD1K/WY8bMnUrFGHAAA=").unwrap();
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
                pg_config,
                &conflicting_deposit_outpoints,
                second_spend_tx.clone()
            )
            .await,
            ResponseResult::SetSpend(SetSpendResult { ack: true })
        );
        let received = get_spend_tx(pg_config, &deposit_outpoint).await.unwrap();
        assert_eq!(
            received,
            ResponseResult::SpendTx(SpendTx {
                transaction: Some(second_spend_tx.clone())
            })
        );

        // And we can even set the same Spend again, it will just do nothing
        // FIXME: it should err explicitly
        assert_eq!(
            set_spend_tx(
                pg_config,
                &conflicting_deposit_outpoints,
                second_spend_tx.clone()
            )
            .await,
            ResponseResult::SetSpend(SetSpendResult { ack: true })
        );
        let received = get_spend_tx(pg_config, &deposit_outpoint).await.unwrap();
        assert_eq!(
            received,
            ResponseResult::SpendTx(SpendTx {
                transaction: Some(second_spend_tx)
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
