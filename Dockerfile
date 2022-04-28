FROM rust:1.59 AS builder
WORKDIR /app

RUN rustup component add rustfmt

COPY Cargo.* ./
COPY ./src ./src
COPY ./migrations ./migrations

RUN cargo build --release


FROM debian:buster-slim as runtime
WORKDIR /usr/www/app

RUN apt-get update && apt-get install -y curl openssl libssl-dev libpq-dev
RUN /usr/sbin/update-ca-certificates

COPY --from=builder /usr/local/cargo/bin/* ./
COPY --from=builder /app/migrations ./migrations/ 

CMD ['./api']