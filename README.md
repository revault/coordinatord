# Revault coordinator

This is the implementation of the synchronisation server -or coordinator- for the version
1 of the deployment of the Revault architecture, as specified in [practical-revault](https://github.com/re-vault/practical-revault).

The coordinator is a (pretty simple) server used by Managers to advertise
[Spend transactions](https://github.com/re-vault/practical-revault/blob/master/transactions.md#spend_tx)
to the Stakeholders' watchtowers before processing an Unvault.  
It is also used by Stakeholders to exchange signatures for the pre-signed transactions
(Emergency transaction, Cancel transaction, Unvault Emergency transaction and Unvault
transaction).

Communication is done over Noise KK channels. Each participant's static public key must be
initially configured on the Coordinator.  

**The coordinator is never trusted with funds, it is just a convenient mean to replace
direct connections between participants.**

## Usage

The coordinator will need access to a PostgreSQL database, set in the configuration file as:
```
postgres_uri = "postgresql://user:password@localhost:5432/database_name"
```

An easy way to try it out without having to configure Postgres on your system is by using Docker:
```
docker run --rm -d -p 5432:5432 --name postgres-coordinatord -e POSTGRES_PASSWORD=revault -e POSTGRES_USER=revault -e POSTGRES_DB=coordinator_db postgres:alpine
cargo run -- --conf contrib/config.toml
```

For a more complete guide for setting up a demo Revault deployment, check out the tutorial in 
[`revaultd`'s repository](https://github.com/revault/revaultd/)!


## Contributing

Any contribution of any form (patches, bug reports, documentation, thoughts) is very
welcome.

See [CONTRIBUTING](CONTRIBUTING.md) for general guidelines.

## License

Released under the BSD 3-Clause Licence. See the [LICENCE](LICENCE) file.
