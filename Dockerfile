FROM rust:1-alpine AS build

RUN apk add --no-cache build-base cmake perl
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM alpine:3

RUN apk add --no-cache ca-certificates
ENV LISTEN_ADDR=0.0.0.0:3000 DB_PATH=/data/lake.sqlite3
WORKDIR /app
COPY --from=build /app/target/release/lake /usr/local/bin/lake
VOLUME ["/data"]
EXPOSE 3000
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 CMD wget -q -O /dev/null http://127.0.0.1:3000/health || exit 1
CMD ["lake"]
