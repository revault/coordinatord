use crate::db::{
    fetch_sigs, fetch_spend_tx, store_encrypted_sig, store_plaintext_sig, store_spend_tx,
};
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
        // If we don't have it, return an empty array. At this point the watchtower
        // will probably revault..
        serde_json::to_vec(&SpendTx {
            transaction: vec![],
        })?
    };

    Ok(Some(response))
}

// Managers can poll pre-signed transaction signatures and set a spend transaction
// for a given vault so watchtowers can poll it.
pub async fn process_manager_message(
    pg_config: &tokio_postgres::Config,
    msg: Vec<u8>,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    match serde_json::from_slice::<FromManager>(&msg)? {
        FromManager::GetSigs(msg) => Ok(Some(serde_json::to_vec(
            &fetch_sigs(pg_config, msg.id).await?,
        )?)),
        FromManager::SetSpend(msg) => {
            // FIXME: return an ACK on success and an error if already present
            store_spend_tx(pg_config, msg.deposit_outpoint, msg.spend_tx()).await?;
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
            match signature {
                RevaultSignature::PlaintextSig(sig) => {
                    store_plaintext_sig(&pg_config, id, pubkey, sig).await?;
                }
                RevaultSignature::EncryptedSig {
                    encryption_key,
                    encrypted_signature,
                } => {
                    store_encrypted_sig(
                        &pg_config,
                        id,
                        pubkey,
                        encrypted_signature,
                        encryption_key,
                    )
                    .await?;
                }
            };

            // FIXME: should we send an explicit response to the sender?
            Ok(None)
        }
        // If we got some sigs, send them
        FromStakeholder::GetSigs(msg) => Ok(Some(serde_json::to_vec(
            &fetch_sigs(&pg_config, msg.id).await?,
        )?)),
    }
}

#[cfg(test)]
mod tests {
    use crate::db::*;
    use crate::processing::{
        process_manager_message, process_stakeholder_message, process_watchtower_message,
    };

    use revault_net::{
        bitcoin::{
            hashes::hex::FromHex,
            secp256k1::{PublicKey, Signature},
            OutPoint, Txid,
        },
        message::server::*,
        sodiumoxide::crypto::box_::gen_keypair,
    };
    use revault_tx::transactions::{RevaultTransaction, SpendTransaction};

    use std::{collections::BTreeMap, str::FromStr};

    use tokio::runtime::Builder as RuntimeBuilder;
    use tokio_postgres::tls::NoTls;

    async fn postgre_setup() -> tokio_postgres::Config {
        let conf =
            tokio_postgres::Config::from_str("postgresql://test:test@localhost/coordinatord_test")
                .unwrap();
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
            .batch_execute("DROP TABLE signatures; DROP TABLE spend_txs; DROP TABLE version;")
            .await
            .expect("dropping tables");
    }

    async fn sig_exchange() {
        let pg_config = postgre_setup().await;

        let signature_1 = RevaultSignature::PlaintextSig(
            Signature::from_compact(&[
                0xdc, 0x4d, 0xc2, 0x64, 0xa9, 0xfe, 0xf1, 0x7a, 0x3f, 0x25, 0x34, 0x49, 0xcf, 0x8c,
                0x39, 0x7a, 0xb6, 0xf1, 0x6f, 0xb3, 0xd6, 0x3d, 0x86, 0x94, 0x0b, 0x55, 0x86, 0x82,
                0x3d, 0xfd, 0x02, 0xae, 0x3b, 0x46, 0x1b, 0xb4, 0x33, 0x6b, 0x5e, 0xcb, 0xae, 0xfd,
                0x66, 0x27, 0xaa, 0x92, 0x2e, 0xfc, 0x04, 0x8f, 0xec, 0x0c, 0x88, 0x1c, 0x10, 0xc4,
                0xc9, 0x42, 0x8f, 0xca, 0x69, 0xc1, 0x32, 0xa2,
            ])
            .unwrap(),
        );
        // Inputs aren't checked, we could have fed it garbage
        let pubkey = PublicKey::from_slice(&[
            0x02, 0xc6, 0x6e, 0x7d, 0x89, 0x66, 0xb5, 0xc5, 0x55, 0xaf, 0x58, 0x05, 0x98, 0x9d,
            0xa9, 0xfb, 0xf8, 0xdb, 0x95, 0xe1, 0x56, 0x31, 0xce, 0x35, 0x8c, 0x3a, 0x17, 0x10,
            0xc9, 0x62, 0x67, 0x90, 0x63,
        ])
        .unwrap();
        let id = Txid::from_hex("264595a4ace1865dfa442bb923320b8f00413711655165ac13a470db2c5384c0")
            .unwrap();
        let plaintext_sig = FromStakeholder::Sig(Sig {
            id,
            pubkey,
            signature: signature_1.clone(),
        });
        assert!(process_stakeholder_message(
            &pg_config,
            serde_json::to_vec(&plaintext_sig).unwrap()
        )
        .await
        .unwrap()
        .is_none());

        let id2 =
            Txid::from_hex("6a276a96807dd45ceed9cbd6fd48b5edf185623b23339a1643e19e8dcbf2e474")
                .unwrap();
        let (encryption_key, _) = gen_keypair();
        let signature_2 = RevaultSignature::EncryptedSig {
            encryption_key,
            encrypted_signature: vec![1u8; 71],
        };
        let encrypted_signature_1 = FromStakeholder::Sig(Sig {
            id: id2,
            pubkey,
            signature: signature_2.clone(),
        });
        assert!(process_stakeholder_message(
            &pg_config,
            serde_json::to_vec(&encrypted_signature_1).unwrap()
        )
        .await
        .unwrap()
        .is_none());

        // Another signature for the second txid
        let (encryption_key, _) = gen_keypair();
        let signature_3 = RevaultSignature::EncryptedSig {
            encryption_key,
            encrypted_signature: vec![4u8; 71],
        };
        let encrypted_signature_2 = FromStakeholder::Sig(Sig {
            id: id2,
            pubkey,
            signature: signature_3.clone(),
        });
        assert!(process_stakeholder_message(
            &pg_config,
            serde_json::to_vec(&encrypted_signature_2).unwrap()
        )
        .await
        .unwrap()
        .is_none());

        // Now fetch the sigs we just stored
        let mut signatures = BTreeMap::new();
        signatures.insert(pubkey, signature_1);
        assert_eq!(
            serde_json::from_slice::<Sigs>(
                &process_stakeholder_message(
                    &pg_config,
                    serde_json::to_vec(&GetSigs { id }).unwrap()
                )
                .await
                .unwrap()
                .unwrap()
            )
            .unwrap(),
            Sigs {
                signatures: signatures.clone()
            }
        );
        assert_eq!(
            serde_json::from_slice::<Sigs>(
                &process_manager_message(&pg_config, serde_json::to_vec(&GetSigs { id }).unwrap())
                    .await
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            Sigs { signatures }
        );

        let mut signatures = BTreeMap::new();
        signatures.insert(pubkey, signature_2);
        signatures.insert(pubkey, signature_3);
        assert_eq!(
            serde_json::from_slice::<Sigs>(
                &process_stakeholder_message(
                    &pg_config,
                    serde_json::to_vec(&GetSigs { id: id2 }).unwrap()
                )
                .await
                .unwrap()
                .unwrap()
            )
            .unwrap(),
            Sigs {
                signatures: signatures.clone()
            }
        );
        assert_eq!(
            serde_json::from_slice::<Sigs>(
                &process_manager_message(
                    &pg_config,
                    serde_json::to_vec(&GetSigs { id: id2 }).unwrap()
                )
                .await
                .unwrap()
                .unwrap()
            )
            .unwrap(),
            Sigs { signatures }
        );

        postgre_teardown(&pg_config).await;
    }

    async fn spend_tx_exchange() {
        let pg_config = postgre_setup().await;

        let spend_psbt_str = "\"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\"";
        let spend_tx: SpendTransaction = serde_json::from_str(&spend_psbt_str).unwrap();
        let deposit_outpoint = OutPoint::from_str(
            "4e37824b0bd0843bb94c290956374ffa1752d4c6bc9089fcbd20e1e63518b25e:0",
        )
        .unwrap();
        let setspend_msg = SetSpendTx::from_spend_tx(deposit_outpoint, spend_tx.clone()).unwrap();
        assert!(
            process_manager_message(&pg_config, serde_json::to_vec(&setspend_msg).unwrap())
                .await
                .unwrap()
                .is_none()
        );

        let getspend_msg = GetSpendTx { deposit_outpoint };
        let received =
            process_watchtower_message(&pg_config, serde_json::to_vec(&getspend_msg).unwrap())
                .await
                .unwrap()
                .unwrap();
        let received_msg: SpendTx = serde_json::from_slice(&received).unwrap();
        let spend_tx_bytes = spend_tx.as_bitcoin_serialized().unwrap();
        assert_eq!(received_msg.transaction, spend_tx_bytes);

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
