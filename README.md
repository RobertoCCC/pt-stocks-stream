# pt-stocks-stream

Real-time PSI-20 ticker built around the pattern most production trading and analytics platforms use: **one publisher → message bus → N stateless fan-out workers → browser clients**. The repo contains two Go binaries (`poller`, `wsserver`) that compose with Redis pub/sub and ship through Docker. A companion [`pt-stocks-stream-web`](https://github.com/RobertoCCC/pt-stocks-stream-web) repo holds the Next.js frontend.

## Live demo

- **Web app**: <https://pt-stocks-stream-web.vercel.app>
- **WebSocket endpoint**: `wss://pt-stocks-stream-ws.onrender.com/ws`
- **Health**: <https://pt-stocks-stream-ws.onrender.com/healthz>

> The Render free tier sleeps after ~15 minutes of inactivity. The first request after a sleep takes a few seconds to wake the service up; subsequent ticks arrive every 15 seconds.

## Architecture

```
┌─────────────┐    JSON envelope    ┌───────────┐   pub/sub   ┌─────────────┐    WebSocket   ┌──────────┐
│   poller    │ ──────────────────▶ │   Redis   │ ──────────▶ │  wsserver   │ ─────────────▶ │ browsers │
│ (one proc)  │  every -interval    │  channel  │             │  (N replicas)│   fan-out      │   (Web)  │
└─────────────┘                     └───────────┘             └─────────────┘                └──────────┘
       │
       └── fetcher: yahoo | synthetic (flag-driven)
```

- The **poller** is the only process that talks to upstream (Yahoo Finance v7 or the in-process synthetic walk). It fetches a batch of quotes on a fixed interval and publishes the envelope to a Redis channel. Retries use **bounded exponential backoff with full jitter** and fail fast on permanent 4xx responses (anything other than 429).
- **Redis pub/sub** is the seam that lets us run more than one wsserver replica without coordination. Pub/sub is fire-and-forget, which is the right durability profile here: the next tick arrives in 15 seconds anyway, and we never want a slow consumer to backpressure the publisher.
- The **wsserver** owns the WebSocket lifecycle. Each connected client gets a **bounded send queue** (16 frames). If a client falls behind, we **drop frames for that one client** and increment a counter — we never block the hub goroutine on a slow consumer. New clients receive the most recent broadcast as a snapshot so they don't wait for the next tick to see prices.
- The **frontend** maintains a single WebSocket connection and reconnects with full-jitter backoff. Latest prices and a 30-point ring buffer per symbol live in a `useReducer`; rows re-render only when their quote changes.

## Repository layout

```
cmd/
  poller/          # produces ticks, writes to stdout or Redis
  wsserver/        # subscribes to Redis, exposes /ws and /healthz
internal/
  quote/           # wire types shared by all components
  tickers/         # PSI-20 ticker list and helpers
  yahoo/           # Yahoo Finance v7 client (HTTP + retries)
  synthetic/       # geometric Brownian motion price walk for CI/dev
  redisbus/        # thin wrapper around go-redis pub/sub
  wsserver/        # hub, client lifecycle, HTTP handler
Dockerfile         # single image, both binaries
docker-compose.yml # redis + poller + wsserver, ready for local hacking
```

## Quickstart (Docker)

```bash
docker compose up --build
# wsserver listening on :8080
# - WebSocket:   ws://localhost:8080/ws
# - Health:      http://localhost:8080/healthz
```

Try it from the shell:

```bash
# In one terminal:
docker compose logs -f poller | head -n 5
# Each log line shows the envelope sent to Redis.

# In another, connect a CLI WebSocket client:
websocat ws://localhost:8080/ws | head -n 3
```

To run the frontend against the local backend, clone [pt-stocks-stream-web](https://github.com/RobertoCCC/pt-stocks-stream-web) and set `NEXT_PUBLIC_WS_URL=ws://localhost:8080/ws` in `.env.local`.

## Running directly (no Docker)

```bash
# 1. Redis
docker run --rm -p 6379:6379 redis:7-alpine

# 2. Poller (synthetic source, no external API needed)
go run ./cmd/poller \
  -source=synthetic \
  -sink=redis \
  -redis-url=redis://localhost:6379 \
  -interval=2s -timeout=1500ms -log-format=text

# 3. wsserver
go run ./cmd/wsserver \
  -addr=:8080 \
  -redis-url=redis://localhost:6379 \
  -log-format=text
```

## Configuration

### `poller`

| Flag / env             | Default          | Purpose                                                |
| ---------------------- | ---------------- | ------------------------------------------------------ |
| `-source`              | `synthetic`      | `yahoo` (Yahoo Finance v7) or `synthetic` (CI/dev).    |
| `-interval`            | `15s`            | How often to fetch a new batch.                        |
| `-timeout`             | `8s`             | Per-fetch deadline. Must be `< interval`.              |
| `-max-retry`           | `5`              | Retries per tick before logging and moving on.         |
| `-sink`                | `stdout`         | `stdout` or `redis`.                                   |
| `-redis-url`           | _(env REDIS_URL)_| Required when `-sink=redis`.                           |
| `-redis-channel`       | `psi20.ticks`    | Pub/sub channel name.                                  |
| `-log-format`          | `json`           | `json` (production) or `text` (humans).                |

### `wsserver`

| Flag / env              | Default          | Purpose                                                 |
| ----------------------- | ---------------- | ------------------------------------------------------- |
| `-addr`                 | `:8080`          | HTTP listen address.                                    |
| `-redis-url`            | _(env REDIS_URL)_| Required.                                               |
| `-redis-channel`        | `psi20.ticks`    | Channel the publisher writes to.                        |
| `-allowed-origins`      | _(empty)_        | Comma-separated host patterns allowed to upgrade.       |
| `-log-format`           | `json`           |                                                         |

## Design notes

- **One interface, defined where it's used.** The `fetcher` interface lives in `cmd/poller` rather than in a shared `internal/fetcher` package — the consumer defines what it needs. Idiomatic Go: "accept interfaces, return concrete types".
- **Full-jitter backoff.** Following AWS's [Exponential Backoff And Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/) recommendation, the poller's retry delay is uniform in `[0, 2^attempt * base)`. Many pollers retrying against the same upstream do not all align.
- **Snapshot-on-connect.** The hub keeps a single-frame snapshot of the most recent broadcast. A new tab sees prices instantly without waiting up to 15s for the next tick — a small touch that materially improves the perceived liveness.
- **No third-party WebSocket library in the browser.** The frontend uses the native `WebSocket` and a single `useReducer`. Smaller bundle, fewer dependencies, behaviour easier to reason about.
- **Pub/sub instead of streams.** Redis Streams would give durability but at the cost of more complex consumer-group bookkeeping. For a live ticker, dropping a missed tick is correct — the next one is along in 15 seconds.

## Testing

```bash
go test ./...
```

The suite covers:
- HTTP roundtrips against `httptest.Server` (yahoo client) — ordering, empty responses, 429 vs 401 handling, base URL override.
- Deterministic random walks (synthetic) — seed reproducibility and price floor.
- Retry behaviour (poller) — flaky fetcher + jitter stubbed for fast tests, ctx cancellation respected, fast-fail on 4xx except 429.
- Real pub/sub roundtrip via [miniredis](https://github.com/alicebob/miniredis) — no Docker required in CI.
- Hub fan-out, snapshot replay, backpressure drop counting.
- Full HTTP+WebSocket integration via `httptest.NewServer` + `coder/websocket`.

## License

MIT — see [LICENSE](./LICENSE).
