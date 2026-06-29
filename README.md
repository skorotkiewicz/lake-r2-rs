# lake

Minimal file sharing on Cloudflare R2.

Upload a file with one `curl` command. Get a link back. No sign-up. Links expire after 7 days or 20 downloads.

## Quick Start

```sh
docker pull ghcr.io/skorotkiewicz/lake-r2-rs
```

```sh
cp .env.example .env
docker compose up
```

## Run

```sh
R2_ACCOUNT_ID=... \
R2_ACCESS_KEY_ID=... \
R2_SECRET_ACCESS_KEY=... \
R2_BUCKET=... \
BASE_URL=https://share.example \
cargo run
```

## Upload

```sh
curl -fsS -H "X-Filename: file" --data-binary @file https://share.example/upload
```

## Limits

Uploads stop before the Cloudflare R2 free-tier limits:

- Storage: 8 GB guard on a 10 GB limit
- Class A: 800k guard on a 1M limit
- Class B: 8M guard on a 10M limit

Defaults: 100 MB max upload, 7-day TTL, 20 downloads.

## Check

```sh
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```
