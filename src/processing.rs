use crate::coordinatord::{SigCache, SpendTxCache};
use revault_net::message::server::*;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

fn get_sigs(shared_signature: &Arc<RwLock<SigCache>>, getsigs_msg: GetSigs) -> Sigs {
    Sigs {
        signatures: shared_signature
            .read()
            .unwrap()
            .get(&getsigs_msg.id)
            .cloned()
            .unwrap_or_else(|| HashMap::new()),
    }
}

// Managers can poll pre-signed transaction signatures and set a spend transaction
// for a given vault so watchtowers can poll it.
pub fn process_manager_message(
    shared_signature: &Arc<RwLock<SigCache>>,
    spend_txs: &Arc<RwLock<SpendTxCache>>,
    msg: Vec<u8>,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    match serde_json::from_slice::<FromManager>(&msg)? {
        FromManager::GetSigs(msg) => {
            Ok(Some(serde_json::to_vec(&get_sigs(shared_signature, msg))?))
        }
        FromManager::SetSpend(msg) => {
            // FIXME: return an ACK on success and an error if already present
            spend_txs
                .write()
                .unwrap()
                .insert(msg.deposit_outpoint, msg.spend_tx());
            Ok(None)
        }
    }
}

// Stakeholders only send us signatures, so we juste store and serve signatures
// idntified by txids.
pub fn process_stakeholder_message(
    shared_signature: &Arc<RwLock<SigCache>>,
    msg: Vec<u8>,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    match serde_json::from_slice::<FromStakeholder>(&msg)? {
        // We got a new signature for a pre-signed transaction. Just store it. If we can't
        // trust our own stakeholders, who can we trust?
        FromStakeholder::Sig(Sig {
            id,
            pubkey,
            signature,
        }) => {
            let mut txid_map = shared_signature.write().unwrap();
            if let Some(pubkey_map) = txid_map.get_mut(&id) {
                pubkey_map.insert(pubkey, signature);
            } else {
                let mut pubkey_map = HashMap::with_capacity(1);
                pubkey_map.insert(pubkey, signature);
                txid_map.insert(id, pubkey_map);
            }

            // FIXME: should we send an explicit response to the sender?
            Ok(None)
        }
        // If we got some sigs, send them
        FromStakeholder::GetSigs(msg) => {
            Ok(Some(serde_json::to_vec(&get_sigs(shared_signature, msg))?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use revault_net::bitcoin::{
        hashes::hex::FromHex,
        secp256k1::{PublicKey, Signature},
        Txid,
    };

    #[test]
    pub fn msg_process_sanity_checks() {
        let sigcache = Arc::new(RwLock::new(SigCache::new()));

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
            &sigcache,
            serde_json::to_vec(&plaintext_sig).unwrap()
        )
        .unwrap()
        .is_none());
        assert!(sigcache.read().unwrap().get(&id).is_some());

        let id2 =
            Txid::from_hex("6a276a96807dd45ceed9cbd6fd48b5edf185623b23339a1643e19e8dcbf2e474")
                .unwrap();
        let signature_2 = RevaultSignature::EncryptedSig {
            pubkey: vec![2u8, 32],
            encrypted_signature: vec![1u8; 71],
        };
        let encrypted_signature_1 = FromStakeholder::Sig(Sig {
            id: id2,
            pubkey,
            signature: signature_2.clone(),
        });
        assert!(process_stakeholder_message(
            &sigcache,
            serde_json::to_vec(&encrypted_signature_1).unwrap()
        )
        .unwrap()
        .is_none());
        assert!(sigcache.read().unwrap().get(&id2).is_some());
        // A second sig for this one
        let signature_3 = RevaultSignature::EncryptedSig {
            pubkey: vec![0u8, 32],
            encrypted_signature: vec![4u8; 71],
        };
        let encrypted_signature_2 = FromStakeholder::Sig(Sig {
            id: id2,
            pubkey,
            signature: signature_3.clone(),
        });
        assert!(process_stakeholder_message(
            &sigcache,
            serde_json::to_vec(&encrypted_signature_2).unwrap()
        )
        .unwrap()
        .is_none());

        let getsigs_1 = serde_json::to_vec(&FromStakeholder::GetSigs(GetSigs { id })).unwrap();
        let mut expected_sigs_1 = HashMap::with_capacity(1);
        expected_sigs_1.insert(pubkey, signature_1);
        assert_eq!(
            serde_json::from_slice::<Sigs>(
                &process_stakeholder_message(&sigcache, getsigs_1)
                    .unwrap()
                    .unwrap(),
            )
            .unwrap(),
            Sigs {
                signatures: expected_sigs_1
            }
        );

        let getsigs_2 = serde_json::to_vec(&FromStakeholder::GetSigs(GetSigs { id: id2 })).unwrap();
        let mut expected_sigs_2 = HashMap::with_capacity(1);
        expected_sigs_2.insert(pubkey, signature_2);
        expected_sigs_2.insert(pubkey, signature_3);
        assert_eq!(
            serde_json::from_slice::<Sigs>(
                &process_stakeholder_message(&sigcache, getsigs_2.clone())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap(),
            Sigs {
                signatures: expected_sigs_2.clone()
            }
        );

        let spend_cache = Arc::new(RwLock::new(HashMap::new()));
        assert_eq!(
            serde_json::from_slice::<Sigs>(
                &process_manager_message(&sigcache, &spend_cache, getsigs_2)
                    .unwrap()
                    .unwrap(),
            )
            .unwrap(),
            Sigs {
                signatures: expected_sigs_2
            }
        );
    }
}
