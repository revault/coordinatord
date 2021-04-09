use crate::db::{fetch_sigs, fetch_spend_tx, store_sig, store_spend_tx};
use revault_net::message::server::*;

// Watchtowers only fetch spend transactions from us, so in reality it is
// process_getspendtx_message() here.
pub async fn process_watchtower_message(
    pg_config: &tokio_postgres::Config,
    msg: Vec<u8>,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let GetSpendTx { deposit_outpoint } = serde_json::from_slice::<GetSpendTx>(&msg)?;
    let response = if let Some(transaction) = fetch_spend_tx(pg_config, deposit_outpoint).await? {
        serde_json::to_vec(&SpendTx { transaction })?
    } else {
        // FIXME: make it an Option!!
        vec![]
    };

    Ok(Some(response))
}

async fn answer_getsigs(
    pg_config: &tokio_postgres::Config,
    msg: GetSigs,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    serde_json::to_vec(&fetch_sigs(pg_config, msg.id).await?).map_err(|e| Box::from(e))
}

// Managers can poll pre-signed transaction signatures and set a spend transaction
// for a given set of vaults so watchtowers can poll it.
pub async fn process_manager_message(
    pg_config: &tokio_postgres::Config,
    msg: Vec<u8>,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    log::trace!("Processing manager message");

    match serde_json::from_slice::<FromManager>(&msg)? {
        FromManager::GetSigs(msg) => answer_getsigs(pg_config, msg).await.map(|x| Some(x)),
        FromManager::SetSpend(msg) => {
            // FIXME: return an ACK on success and an error if already present
            store_spend_tx(pg_config, &msg.deposit_outpoints.clone(), msg.spend_tx()).await?;
            Ok(None)
        }
    }
}

// Stakeholders only send us signatures, so we juste store and serve signatures
// idntified by txids.
pub async fn process_stakeholder_message(
    pg_config: &tokio_postgres::Config,
    msg: Vec<u8>,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    log::trace!("Processing stakeholder message");

    match serde_json::from_slice::<FromStakeholder>(&msg)? {
        // We got a new signature for a pre-signed transaction. Just store it. If we can't
        // trust our own stakeholders, who can we trust?
        FromStakeholder::Sig(Sig {
            id,
            pubkey,
            signature,
        }) => {
            store_sig(&pg_config, id, pubkey, signature).await?;
            // FIXME: should we send an explicit response to the sender?
            Ok(None)
        }
        // If we got some sigs, send them
        FromStakeholder::GetSigs(msg) => answer_getsigs(pg_config, msg).await.map(|x| Some(x)),
    }
}

// Stakeholders-managers can send us both what the above process_*_message() handle, so direct it
// to the right one
pub async fn process_stakeholdermanager_message(
    pg_config: &tokio_postgres::Config,
    msg: Vec<u8>,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    log::trace!("Processing manager-stakeholder message");

    match serde_json::from_slice::<FromParticipant>(&msg)? {
        FromParticipant::GetSigs(_) => process_stakeholder_message(pg_config, msg).await,
        FromParticipant::Sig(_) => process_stakeholder_message(pg_config, msg).await,
        FromParticipant::SetSpend(_) => process_manager_message(pg_config, msg).await,
    }
}

#[cfg(test)]
mod tests {
    use crate::db::*;
    use crate::processing::{
        process_manager_message, process_stakeholder_message, process_stakeholdermanager_message,
        process_watchtower_message,
    };

    use revault_net::{
        bitcoin::{
            hashes::hex::FromHex,
            secp256k1::{PublicKey, Signature},
            OutPoint, Txid,
        },
        message::server::*,
    };
    use revault_tx::transactions::{RevaultTransaction, SpendTransaction};

    use std::{str::FromStr, collections::BTreeMap};

    use tokio::runtime::Builder as RuntimeBuilder;
    use tokio_postgres::tls::NoTls;

    async fn postgre_setup() -> tokio_postgres::Config {
        let conf =
            tokio_postgres::Config::from_str("postgresql://test:test@localhost/coordinatord_test")
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

    async fn sig_exchange() {
        let pg_config = postgre_setup().await;

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
        let txid_a = Txid::from_hex("264595a4ace1865dfa442bb923320b8f00413711655165ac13a470db2c5384c0")
            .unwrap();
        let sig = FromStakeholder::Sig(Sig {
            id: txid_a,
            pubkey: pubkey_a,
            signature: signature_a.clone(),
        });
        assert!(
            process_stakeholder_message(&pg_config, serde_json::to_vec(&sig).unwrap())
                .await
                .unwrap()
                .is_none()
        );

        let pubkey_b = PublicKey::from_str("03ffae85b76dd0dd96cbf23348fb398ab93274466759201ecf29d0f68ddd9d1b6c").unwrap();
        let txid_b = Txid::from_hex("ead1ff4c948a4993097647b84cd0aa80d3205cc8ddcd19b8aca154743c2e5cec").unwrap();
        let signature_b = Signature::from_str("304402204b0ab8a7d95d5b67d5c1b8584a3075adcac787a315f79a9b52b5a736909c975502206def9036d3d980a7cb66f2baa64ebdcd6648d70b324c6c18c349fa240dd07ca8").unwrap();
        let sig = FromStakeholder::Sig(Sig {
            id: txid_b,
            pubkey: pubkey_b,
            signature: signature_b,
        });
        assert!(
            process_stakeholdermanager_message(&pg_config, serde_json::to_vec(&sig).unwrap())
                .await
                .unwrap()
                .is_none()
        );

        // Now fetch the sigs we just stored
        let mut signatures_a = BTreeMap::new();
        signatures_a.insert(pubkey_a, signature_a);
        assert_eq!(
            serde_json::from_slice::<Sigs>(
                &process_stakeholder_message(
                    &pg_config,
                    serde_json::to_vec(&GetSigs { id: txid_a }).unwrap()
               )
                .await
                .unwrap()
                .unwrap()
            )
           .unwrap(),
            Sigs {
                signatures: signatures_a.clone()
            }
        );
        assert_eq!(
            serde_json::from_slice::<Sigs>(
                &process_manager_message(
                    &pg_config,
                    serde_json::to_vec(&GetSigs { id: txid_a }).unwrap()
               )
                .await
                .unwrap()
                .unwrap()
            )
           .unwrap(),
            Sigs {
                signatures: signatures_a.clone()
            }
        );
        assert_eq!(
            serde_json::from_slice::<Sigs>(
                &process_stakeholdermanager_message(
                    &pg_config,
                    serde_json::to_vec(&GetSigs { id: txid_a }).unwrap()
               )
                .await
                .unwrap()
                .unwrap()
            )
           .unwrap(),
            Sigs {
                signatures: signatures_a.clone()
            }
        );

        let mut signatures_b = BTreeMap::new();
        signatures_b.insert(pubkey_b, signature_b);
        assert_eq!(
            serde_json::from_slice::<Sigs>(
                &process_stakeholder_message(
                    &pg_config,
                    serde_json::to_vec(&GetSigs { id: txid_b }).unwrap()
               )
                .await
                .unwrap()
                .unwrap()
            )
           .unwrap(),
            Sigs {
                signatures: signatures_b.clone()
            }
        );
        assert_eq!(
            serde_json::from_slice::<Sigs>(
                &process_manager_message(
                    &pg_config,
                    serde_json::to_vec(&GetSigs { id: txid_b }).unwrap()
               )
                .await
                .unwrap()
                .unwrap()
            )
           .unwrap(),
            Sigs {
                signatures: signatures_b.clone()
            }
        );
        assert_eq!(
            serde_json::from_slice::<Sigs>(
                &process_stakeholdermanager_message(
                    &pg_config,
                    serde_json::to_vec(&GetSigs { id: txid_b }).unwrap()
               )
                .await
                .unwrap()
                .unwrap()
            )
           .unwrap(),
            Sigs {
                signatures: signatures_b.clone()
            }
        );

        postgre_teardown(&pg_config).await;
    }

    async fn spend_tx_exchange() {
        let pg_config = postgre_setup().await;

        let spend_psbt_str = "\"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\"";
        let spend_tx: SpendTransaction = serde_json::from_str(&spend_psbt_str).unwrap();
        let deposit_outpoints = vec![OutPoint::from_str(
            "4e37824b0bd0843bb94c290956374ffa1752d4c6bc9089fcbd20e1e63518b25e:0",
        )
        .unwrap()];
        let setspend_msg = SetSpendTx::from_spend_tx(deposit_outpoints.clone(), spend_tx.clone());
        assert!(
            process_manager_message(&pg_config, serde_json::to_vec(&setspend_msg).unwrap())
                .await
                .unwrap()
                .is_none()
        );

        let deposit_outpoint = deposit_outpoints[0];
        let getspend_msg = GetSpendTx { deposit_outpoint };
        let received =
            process_watchtower_message(&pg_config, serde_json::to_vec(&getspend_msg).unwrap())
                .await
                .unwrap()
                .unwrap();
        let received_msg: SpendTx = serde_json::from_slice(&received).unwrap();
        assert_eq!(received_msg.transaction, spend_tx.into_psbt().extract_tx());

        // If a new one is set with a conflicting outpoint, it'll just get overriden
        let second_spend_tx = SpendTransaction::from_psbt_str("cHNidP8BAGcCAAAAATJj+J05C8NjU6aFkbjH+AlpaAqUSHqsYmvdXXsC6k0XAAAAAADOYAAAAoAyAAAAAAAAIgAgS4/3QaTXSQuvlpDk4z6xdM4cKh4nMpTnhF0HmaQWsu+gjAIAAAAAAAAAAAAAAAEBK0ANAwAAAAAAIgAg3GSr/0q6qUaIuNJEdndSJ2sKFlDccx5CFx4SZ2spL3wBCP2GAQUASDBFAiEApjf0AqotFH4ffzLCB3JKsbda8Ni3v+oad/gHQCUQy5UCIF9IIaPpmwl3uQT6A5CCBeqUW+fwWL0DLEb3Yke/+G8wAUYwQwIfAXs8XkbDD0WccmcLL7lHdezsQjo40ILZHeiI+zn6nwIgdIjHwGU3bMhFSzk23A21zaQQQfcoRpaLqAwEot7jshYBSDBFAiEA6RwcVU0HdHIXy+/Wh7vXGsSbbUsJ3lXqC3AjApSFcAQCIAqwY2ZnRwXcZA53HWYhKpUUwlPVlhHnMZHREccAx4+UAaohA8ujblABMfWi8DaUwzeN+ttu2AppH8zdsD1K/WY8bMnUrFGHZHapFEEQ586S5hPnp11w9epOlCzJEz84iKxrdqkUUwKW1Yzw4enIBR/m4J62xDYUTI6IrGyTUodnUiEDcMBgveHhyiayIeeNy0b54/FpAEo54BLxJK8GHTVomi0hA2LsGliO85N/vTQGAUbHRf6D0D72NbUQPhznA+1bfyNKUq8CzmCyaAABASUhA8ujblABMfWi8DaUwzeN+ttu2AppH8zdsD1K/WY8bMnUrFGHAAA=").unwrap();
        let conflicting_deposit_outpoints = vec![
            deposit_outpoint,
            OutPoint::from_str(
                "dbf7040be3ce465638373f48fb681bf3ae334691c328294f908baadfb927e942:1",
            )
            .unwrap(),
        ];
        let setspend_msg = SetSpendTx::from_spend_tx(
            conflicting_deposit_outpoints.clone(),
            second_spend_tx.clone(),
        );
        assert!(
            process_manager_message(&pg_config, serde_json::to_vec(&setspend_msg).unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            process_manager_message(&pg_config, serde_json::to_vec(&setspend_msg).unwrap()).await.unwrap(),
            process_stakeholdermanager_message(
                &pg_config,
                serde_json::to_vec(&setspend_msg).unwrap()
            )
            .await.unwrap()
        );
        let getspend_msg = GetSpendTx { deposit_outpoint };
        let received =
            process_watchtower_message(&pg_config, serde_json::to_vec(&getspend_msg).unwrap())
                .await
                .unwrap()
                .unwrap();
        let received_msg: SpendTx = serde_json::from_slice(&received).unwrap();
        assert_eq!(
            received_msg.transaction,
            second_spend_tx.into_psbt().extract_tx()
        );

        // And we can even set the same Spend again, it will just do nothing
        // FIXME: it should err explicitly
        assert!(
            process_manager_message(&pg_config, serde_json::to_vec(&setspend_msg).unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            process_manager_message(&pg_config, serde_json::to_vec(&setspend_msg).unwrap()).await.unwrap(),
            process_stakeholdermanager_message(
                &pg_config,
                serde_json::to_vec(&setspend_msg).unwrap()
            )
            .await.unwrap()
        );

        postgre_teardown(&pg_config).await;
    }

    #[test]
    pub fn test_message_processing() {
        let rt = RuntimeBuilder::new_multi_thread()
            .enable_all()
            .thread_name("coordinatord_test")
            .build()
            .expect("Creating tokio runtime");
        rt.block_on(sig_exchange());
        rt.block_on(spend_tx_exchange());
    }
}
