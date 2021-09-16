use honggfuzz::fuzz;
use revault_coordinatord::{
    fuzz::builder::CoordinatordTestBuilder,
    processing::{process_request, MessageSender},
};
use revault_net::message::{
    coordinator::{GetSigs, GetSpendTx},
    RequestParams, ResponseResult,
};
use tokio::runtime::Runtime;

fn main() {
    let current_runtime = Runtime::new().unwrap();
    let builder = current_runtime.block_on(async { CoordinatordTestBuilder::new().await });

    loop {
        fuzz!(|data: &[u8]| {
            let msg: RequestParams = match serde_json::from_slice(data) {
                Ok(msg) => msg,
                Err(_) => return,
            };

            let sender = match &msg {
                RequestParams::CoordSig(_) => MessageSender::StakeHolder,
                RequestParams::GetSigs(_) => MessageSender::Manager,
                // TODO: We still don't have corpus for getspendtx
                RequestParams::GetSpendTx(_) => MessageSender::WatchTower,
                RequestParams::SetSpendTx(_) => MessageSender::Manager,
                _ => return,
            };

            let res = current_runtime.block_on(process_request(
                &builder.postgres_config,
                sender,
                msg.clone(),
            ));
            match msg {
                RequestParams::CoordSig(sigreq) => {
                    match res {
                        None => {}
                        Some(resp) => {
                            match resp {
                                ResponseResult::Sig(sigres) => {
                                    // If it was stored and accepted it must return it now.
                                    if sigres.ack {
                                        let id = sigreq.id;
                                        let getsigs_req = RequestParams::GetSigs(GetSigs { id });
                                        let res = current_runtime
                                            .block_on(process_request(
                                                &builder.postgres_config,
                                                MessageSender::StakeHolder,
                                                getsigs_req,
                                            ))
                                            .unwrap();
                                        let sigs = match res {
                                            ResponseResult::Sigs(sigs) => sigs,
                                            _ => unreachable!("Must have answered with a sigs msg"),
                                        };
                                        assert_eq!(
                                            sigs.signatures
                                                .get(&sigreq.pubkey)
                                                .unwrap()
                                                .serialize_der()
                                                .to_vec(),
                                            sigreq.signature.serialize_der().to_vec()
                                        );
                                    }
                                }
                                _ => unreachable!("Must have responded with a sig_ack, or nothing"),
                            }
                        }
                    }
                }
                RequestParams::GetSigs(_) => {}
                RequestParams::GetSpendTx(_) => {}
                RequestParams::SetSpendTx(setspendreq) => {
                    match res {
                        None => {}
                        Some(resp) => {
                            match resp {
                                ResponseResult::SetSpend(setspendres) => {
                                    // If it was stored and accepted it must return it now.
                                    if setspendres.ack {
                                        let deposit_outpoint = setspendreq.deposit_outpoints[0];
                                        let getspend_req = RequestParams::GetSpendTx(GetSpendTx {
                                            deposit_outpoint,
                                        });
                                        let res = current_runtime
                                            .block_on(process_request(
                                                &builder.postgres_config,
                                                MessageSender::WatchTower,
                                                getspend_req,
                                            ))
                                            .unwrap();
                                        // NOTE: we must have *some* transaction, but the actual
                                        // transaction we sent might have been replaced. Therefore
                                        // we can't assert it's equal to the req's tx.
                                        // TODO: we should probably have a DB per thread.
                                        match res {
                                            ResponseResult::SpendTx(spend_tx) => {
                                                spend_tx.transaction.expect("We just stored it")
                                            }
                                            _ => unreachable!(
                                                "Must have answered with a spend_tx msg"
                                            ),
                                        };
                                    }
                                }
                                _ => unreachable!("Must have responded with a sig_ack, or nothing"),
                            }
                        }
                    }
                }
                _ => {
                    assert!(res.is_none(), "Message was unexpected, must not respond");
                }
            };
        });
    }
}
