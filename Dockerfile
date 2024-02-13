FROM rust:1.75 as builder
WORKDIR /app

RUN apt-get update && apt-get install -y protobuf-compiler
RUN rustup component add rustfmt

COPY Cargo.* ./
COPY ./src ./src
COPY ./migrations ./migrations

RUN cargo build --release


FROM debian:12 as runtime
WORKDIR /app

RUN apt-get update && apt-get install -y curl openssl libssl-dev libpq-dev postgresql-client
RUN /usr/sbin/update-ca-certificates

COPY --from=builder /app/target/release/api ./api
COPY --from=builder /app/target/release/consumer ./consumer
COPY --from=builder /app/target/release/migration ./migration
COPY --from=builder /app/target/release/invalidate_cache ./invalidate_cache
COPY --from=builder /app/migrations ./migrations/

CMD ['./api']
