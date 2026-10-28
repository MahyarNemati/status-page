# status-page

[![CI](https://github.com/MahyarNemati/status-page/actions/workflows/ci.yml/badge.svg)](https://github.com/MahyarNemati/status-page/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/MahyarNemati/status-page)](https://goreportcard.com/report/github.com/MahyarNemati/status-page)

A self-hosted uptime monitor and public status page in a single static binary.
HTTP, TCP, DNS and ICMP probes; incidents with hysteresis and flap protection;
Slack, Discord, webhook and email alerts; error-budget SLOs with multi-window
burn-rate alerting; Prometheus metrics.

No JavaScript on the page. No agent to install. SQLite by default, so there is
nothing else to run.

## Quickstart

```bash
cp config.example.yaml config.yaml     # edit services and alert channels
export STATUSPAGE_ADMIN_TOKEN=$(openssl rand -hex 16)

go run ./cmd/statuspage -config config.yaml -check-config   # validate first
go run ./cmd/statuspage -config config.yaml
```

Open <http://localhost:8080>.

With Docker:

```bash
docker compose up --build
```

## Why this exists

Hosted status pages charge per service and put your incident communications on
someone else's infrastructure — which is precisely the dependency you do not
want while you are having an outage. Most self-hosted alternatives want a
database, a queue, a worker and a frontend build.

This is one binary and one YAML file.

## Architecture

```
  config.yaml ──▶ config  (env expansion, defaults, strict validation)
                    │
                    ▼
                  app  (wiring, lifecycle, graceful shutdown)
                    │
    ┌───────────────┼────────────────┬──────────────┐
    ▼               ▼                ▼              ▼
 monitor        incident           slo            web
 (scheduler)    (manager)      (budgets,       (page, API,
    │               │           burn rates)     admin API)
    │ probes        │ open/resolve  │              │
    ▼               └───────┬───────┘              │
  check                     ▼                      │
 http·tcp·            alert (dispatcher)            │
 dns·icmp         slack·discord·webhook·email       │
    │                                              │
    └──────────────▶ store ◀───────────────────────┘
                 sqlite | postgres
            raw results, hourly/daily rollups,
              incidents, notes, retention
                        │
                        ▼
                     metrics  (Prometheus)
```

| Package | Responsibility |
|---|---|
| `internal/config` | YAML + environment expansion, defaults, validation |
| `internal/check` | the four probe types |
| `internal/store` | SQLite/Postgres persistence, rollups, retention |
| `internal/monitor` | jittered scheduler, bounded worker pool, retries |
| `internal/incident` | thresholds, flap protection, incident lifecycle |
| `internal/slo` | error budgets and multi-window burn-rate rules |
| `internal/alert` | Slack, Discord, webhook and email delivery |
| `internal/metrics` | Prometheus instrumentation |
| `internal/web` | status page, JSON API, admin API, static assets |
| `internal/app` | composition root and shutdown ordering |
| `internal/clock` | injectable time, so tests never sleep |

## The design decisions worth explaining

### One failed probe is not an outage

Every monitor that pages on the first failed request gets muted within a month.
Two separate mechanisms prevent that:

- **Retries inside a probe.** A probe reports down only after every attempt
  failed, so one dropped packet is not a data point.
- **Hysteresis across probes.** An incident opens after `failure_threshold`
  *consecutive* confirmed failures and resolves after `recovery_threshold`
  consecutive successes. The two are configured independently on purpose —
  you usually want to confirm a recovery more slowly than a failure.

### Flap protection

A service bouncing up and down every 30 seconds would otherwise open and
resolve an incident — and send two alerts — every 30 seconds. After
`incidents.flap_threshold` confirmed transitions inside
`incidents.flap_window`, the service is marked flapping and incident churn
stops. Probing and recording continue: you keep the data, you just stop being
paged by it.

### Degraded counts as available

`degraded_latency` grades a slow-but-working service as degraded rather than
down, and degraded counts toward availability. A service answering in 900 ms is
serving traffic; calling that an outage makes the uptime number meaningless.

### Burn-rate alerting instead of threshold alerting

"Alert when availability drops below 99.9%" is a bad alert. Over a 30-day
window it fires long after the damage and then keeps firing for weeks.

Instead, each objective has an **error budget** — 99.9% over 30 days permits
0.1% of probes to fail — and alerts fire on *how fast that budget is being
spent*. Each rule requires both a long and a short window above the threshold:

| Rule | Long | Short | Factor | Meaning |
|---|---|---|---|---|
| fast | 1h | 5m | 14.4 | exhausts a 30-day budget in ~2 days — page now |
| slow | 6h | 30m | 6 | exhausts it in ~5 days — look today |

The long window supplies significance, so a 90-second blip does not page. The
short window supplies freshness, so when the outage ends the alert stops
instead of echoing for the rest of the long window.

A service with no probe data is explicitly **not** breaching. Reporting a
newly added service as failing is how monitoring loses credibility on day one.

### The page caches itself

The status page is the one page guaranteed to receive traffic during an outage.
`server.page_cache_ttl` serves a rendered snapshot so a traffic spike cannot
turn the thing reporting the outage into part of it.

### The admin API fails closed

Declaring incidents by hand requires `Authorization: Bearer
$STATUSPAGE_ADMIN_TOKEN`, compared in constant time. An **unset** token
disables the admin API rather than leaving it open — "no token configured" must
never mean "anyone may declare an outage on your status page". Validation also
rejects a token shorter than 16 characters.

## HTTP API

Public, no auth:

| Method | Path | Returns |
|---|---|---|
| `GET` | `/` | the status page |
| `GET` | `/incidents/{id}` | incident detail page |
| `GET` | `/api/v1/status` | current status of every service |
| `GET` | `/api/v1/services/{id}` | one service |
| `GET` | `/api/v1/services/{id}/uptime` | uptime history |
| `GET` | `/api/v1/incidents` | incident list |
| `GET` | `/api/v1/incidents/{id}` | one incident with notes |
| `GET` | `/api/v1/slo` | objectives, budgets and burn rates |
| `GET` | `/healthz` | liveness |
| `GET` | `/readyz` | readiness — checks the datastore |
| `GET` | `/metrics` | Prometheus (when enabled) |

Admin, requires the bearer token:

```bash
TOKEN=$STATUSPAGE_ADMIN_TOKEN

curl -s localhost:8080/api/v1/admin/services -H "Authorization: Bearer $TOKEN"

curl -s -X POST localhost:8080/api/v1/admin/incidents \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"service_id":"api","title":"Degraded search","severity":"warning"}'

curl -s -X POST localhost:8080/api/v1/admin/incidents/1/notes \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"author":"mahyar","body":"Root cause found, deploying a fix."}'

curl -s -X POST localhost:8080/api/v1/admin/incidents/1/resolve \
  -H "Authorization: Bearer $TOKEN"
```

Manual incidents exist because the useful ones are often invisible to a probe:
planned maintenance, a partner outage, degraded quality that still returns 200.

## Configuration

[`config.example.yaml`](config.example.yaml) documents every field with its
default, and is validated on every CI run so it cannot silently rot.

Notes worth knowing:

- Environment references are expanded from the environment and **startup fails
  if a variable is unset**, so a missing webhook secret is a loud error rather
  than an empty string posted into the void. Expansion covers the whole file
  **including comments**, so do not write a reference in a comment for a
  variable that is not set.
- **Unknown YAML keys are rejected.** A typo like `intervall:` fails at startup
  instead of silently taking the default — which is the single most useful
  thing a config loader can do for you.
- `-check-config` validates and exits. Run it in your deploy pipeline.
- `hidden: true` keeps a service monitored and alerting but off the public page,
  for internal dependencies.
- ICMP uses an unprivileged UDP ping by default; `icmp.privileged: true`
  requires root or `CAP_NET_RAW`.
- `storage.retention.raw` must cover your longest burn-rate window, and
  `retention.daily` must cover `server.uptime_days`. Both are checked at
  startup, because the alternative is an SLO panel that goes blank a month
  later for reasons nobody can reconstruct.

### Flags

```
-config <path>      config file (env STATUSPAGE_CONFIG, default config.yaml)
-check-config       validate and exit
-healthcheck        probe the local /healthz and exit; for container healthchecks
-version            print version and exit
```

`-healthcheck` means the Docker image needs no `curl`, which keeps the attack
surface of the final image to one static binary.

## Development

```bash
make test     # race detector on
make lint     # gofmt + vet
make build    # static binary in bin/
make check    # validate config.yaml
make image
```

Tests use a fake clock (`internal/clock`), so scheduling, hysteresis, flap
windows and dedupe are tested deterministically without sleeping. CI also runs
the store suite against a real Postgres service container, because placeholder
syntax and upsert behaviour differ from SQLite, and builds the image then
fetches `/`, `/api/v1/status` and `/metrics` from the running container.

Everything is pure Go (`modernc.org/sqlite`), so `CGO_ENABLED=0` is set
throughout and the image is static.

## Limitations

- **Single instance.** The scheduler assumes it is the only one probing; two
  replicas double your probe rate and can both open incidents. Postgres
  storage makes shared history possible but leader election is not implemented.
- **No historical SLO recomputation.** Changing an objective does not
  retroactively re-evaluate past windows.
- **Alerts are best-effort.** A failing Slack webhook is logged, not retried
  into a durable queue — a monitor that stops probing because Slack is down
  would be worse than a missed message.
- **No authentication on the public page.** It is public by design; put it
  behind a proxy if you need otherwise.

## License

MIT — see [LICENSE](LICENSE).
