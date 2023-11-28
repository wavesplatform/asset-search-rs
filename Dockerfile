FROM rust:1.69 as builder
WORKDIR /app

RUN apt-get update && apt-get install -y protobuf-compiler
RUN rustup component add rustfmt

COPY Cargo.* ./
COPY ./src ./src
COPY ./migrations ./migrations

RUN cargo install --path .


FROM debian:11 as runtime
WORKDIR /app

RUN apt-get update && apt-get install -y curl openssl libssl-dev libpq-dev postgresql-client
RUN /usr/sbin/update-ca-certificates

COPY --from=builder /usr/local/cargo/bin/* ./
COPY --from=builder /app/migrations ./migrations/ 

CMD ['./api']
