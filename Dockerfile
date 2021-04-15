FROM rust:latest as builder

COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo install --path .

COPY "docker/entrypoint.sh" .
COPY "docker/config.toml" .

EXPOSE 8383

RUN /bin/sh -c "chmod +x entrypoint.sh"
