pub const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS version (
    version INTEGER UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS signatures (
    txid BYTEA NOT NULL,
    pubkey BYTEA NOT NULL,
    signature BYTEA UNIQUE NOT NULL,
    UNIQUE (txid, pubkey)
);

CREATE TABLE IF NOT EXISTS spend_txs (
    txid BYTEA UNIQUE NOT NULL,
    transaction BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS spend_outpoints (
    deposit_txid BYTEA NOT NULL,
    deposit_vout INTEGER NOT NULL,
    spend_txid BYTEA REFERENCES spend_txs (txid) ON DELETE CASCADE,
    UNIQUE (deposit_txid, deposit_vout)
);
";
