pub const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS version (
    version INTEGER UNIQUE NOT NULL
);

-- If the encryption_key is not NULL, then the signature is encrypted.
CREATE TABLE IF NOT EXISTS signatures (
    txid BYTEA NOT NULL,
    pubkey BYTEA NOT NULL,
    signature BYTEA UNIQUE NOT NULL,
    encryption_key BYTEA
);

CREATE TABLE IF NOT EXISTS spend_txs (
    deposit_txid BYTEA UNIQUE NOT NULL,
    deposit_vout INTEGER NOT NULL,
    transaction BYTEA UNIQUE NOT NULL
);
";
