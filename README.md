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

## Contributing

Any contribution of any form (patches, bug reports, documentation, thoughts) is very
welcome.

See [CONTRIBUTING](CONTRIBUTING.md) for general guidelines.

## License

Released under the BSD 3-Clause Licence. See the [LICENCE](LICENCE) file.
