# TGTime

TGTime updates an existing Telegram user profile every minute. The first
name displays Moscow time. The bio includes Moscow, Tokyo and Paris
clocks, CoinGecko crypto prices and CBR exchange rates. The Paris clock
uses the same fixed UTC+02:00 offset as the Python service.

## Run

1. Copy `.env.example` to `.env`. Set `API_ID` to the Telegram application
   used to create the session, and set `PROFILE_PREFIX` to the desired name.
2. Stop the old user client. Put its existing Pyrogram
   `my_account.session` in the private `data/` directory. The file contains
   an account credential. Keep `.env`, `data/` and backups off GitHub.
3. Build the Linux image on a workstation with Rust and Docker.

   ```sh
   cargo build --release --locked
   context="$(mktemp -d)"
   cp target/release/tgtime Dockerfile "$context/"
   docker build -t tgtime-rust:local "$context"
   ```

4. Run `docker compose run --rm tgtime --check` to verify the existing
   Telegram identity and connection without updating the profile.
   Start normal updates with `docker compose up -d`.

The program reads the existing SQLite credential in read-only mode.
It uses MTProto because a Bot API token cannot update a user profile.
It does not initiate enrollment or change the existing session database.
`PROXY_POOL_URLS` is an ordered comma-separated SOCKS5 pool. `PROXY_URL`
is appended if set. An empty pool and empty legacy proxy mean direct
access. Attach the container to the same Docker network as any proxies
named by hostname.

## Operations

- `prices.json` retains the last valid quotes through source outages.
  Crypto is fresh for five minutes and fiat for one hour.
- `health.json` records successful Telegram profile updates. The
  `--health` command checks that the last success is no more than three
  minutes old. A Telegram flood wait is honored even across restarts.
- Run one writer per Telegram session. To roll back, stop Rust before
  starting the retained Python service with the same data directory.
- Use `docker save tgtime-rust:local | gzip` to transfer a workstation-built
  image to a small server without a build cache there. Retain the
  rollback image and check `df -h /` before and after loading. Container
  logs are capped at two 5 MiB files by the included Compose file.
- The Python history is in [Telegram-Profile-AutoUpdate](https://github.com/Laur1dor/Telegram-Profile-AutoUpdate).

Run formatting, Clippy and the tests before release. The CI workflow
also assembles and starts the Linux runtime image with `--help`.
