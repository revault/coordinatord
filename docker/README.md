# Use Revault Coordinatord with docker.

# Prerequisites

* `docker`
* `docker-compose`

# Run [coordinatord](https://github.com/revault/coordinatord)

Add the participants noise keys to the `docker/config.toml`.

```
cd docker
docker-compose build & docker-compose run coordinatord
```

It creates a volume `${PWD}/coordinatord_volume` which stores the
postgres db and the coordinatord datadir.
