use honggfuzz::fuzz;
use revault_coordinatord::{
    fuzz::builder::CoordinatordTestBuilder,
    processing::{process_request, MessageSender},
};
use revault_net::message::{RequestParams, ResponseResult};
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

            let res =
                current_runtime.block_on(process_request(&builder.postgres_config, sender, msg.clone()));
            match (msg, res) {
                (RequestParams::CoordSig(_), Some(ResponseResult::Sig(_)))
                | (RequestParams::GetSigs(_), Some(ResponseResult::Sigs(_)))
                | (RequestParams::GetSpendTx(_), Some(ResponseResult::SpendTx(_)))
                | (RequestParams::SetSpendTx(_), Some(ResponseResult::SetSpend(_))) => {}
                _ => return,
            };
        });
    }
}
