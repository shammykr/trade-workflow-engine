# Trade Workflow Service

A Spring Boot backend that orchestrates the **RFQ → quote → booking** workflow for structured
products (autocallables, reverse convertibles, capital-protected notes).

It integrates with a pricing service and a downstream booking system through Kafka event streams
and REST:

* Workflow state lives in PostgreSQL. Optimistic locking (`@Version`) keeps concurrent desk
  activity safe.
* Observability: structured JSON logs, Micrometer metrics and actuator health checks.
* Tests: one integration test class runs the end-to-end scenarios against real PostgreSQL and
  Kafka (Redpanda) containers.
* CI/CD: GitHub Actions runs lint, build and the integration tests, smoke-tests the Docker image
  and publishes it.

The detailed design is in **[DESIGN.md](DESIGN.md)**: architecture, data flow, integration
contracts and the state machine.

## Architecture

```
                 desk / sales UI
                       │  REST  /api/rfqs
                       ▼
 ┌───────────────────────────────────────────────────────────┐
 │                 trade-workflow-engine                     │
 │                                                           │
 │  api ──► workflow (RfqWorkflowService, state machine) ──► │──REST──► pricing service
 │            │   ▲            │                             │          (stub: /stub/pricing)
 │            ▼   │            ▼                             │              │
 │       PostgreSQL (rfq, quote, rfq_transition; @Version)   │              │ pricing-responses
 │                │                                          │◄─────────────┘
 │                └─ EventPublisher (after commit) ──────────│──► quote-events  (notifications)
 │                                                           │──► booking-events ──► booking system
 │  Kafka listeners ◄────────────────────────────────────────│◄── booking-confirmations ◄─┘ (stub)
 │  quote expiry scheduler                                   │
 │  actuator: /health /metrics /prometheus, JSON logs        │
 └───────────────────────────────────────────────────────────┘
```

**The external systems are simulated.** The pricing service and the booking system are stubs
that run inside the application, in the `stub` package (`stubs.enabled=true`). They still go
through the real integration paths:

* The workflow calls the pricing stub over HTTP, and the stub answers on Kafka.
* The booking stub consumes `booking-events` in its own consumer group and answers on
  `booking-confirmations`.

To use real services instead, set `stubs.enabled=false` and point `PRICING_BASE_URL` at the real
pricing service.

## State machine

```
PENDING ──price received──► QUOTED ──client accepts──► ACCEPTED ──booking confirmed──► BOOKED
   │                          │                           │
   │ pricing refused /        │ client declines /         │ booking rejected
   │ pricing unavailable      │ quote expired             │
   └──────────────► REJECTED ◄┴───────────────────────────┘
```

`BOOKED` and `REJECTED` are terminal. Every transition is checked by `RfqStatus`. Each one is
recorded in `rfq_transition` with the actor (desk user, `pricing-service`, `booking-system` or
`system`).

## Package layout

| Package | Contents |
|---|---|
| `api` | REST controller, request/response DTOs, problem-detail error mapping |
| `domain` | `Rfq`, `Quote`, `RfqTransition` entities; `RfqStatus` state machine; enums |
| `repository` | Spring Data JPA repositories |
| `workflow` | `RfqWorkflowService` (orchestration), quote expiry scheduler, pricing port |
| `integration` | REST pricing client, Kafka listeners for pricing responses and booking confirmations |
| `messaging` | Topic names, event records (the Kafka contracts), after-commit `EventPublisher` |
| `stub` | Simulated pricing service (REST + Kafka) and booking system (Kafka) |
| `observability` | Micrometer workflow metrics, Kafka health indicator |
| `config` | Kafka topics and error handling, typed properties, clock |

## Run

### Docker Compose (app + PostgreSQL + Redpanda)

```bash
docker compose up -d --build
curl localhost:8080/actuator/health
```

### Walk an RFQ through the workflow

```bash
# 1. Submit an RFQ -> 201, status PENDING
curl -s -X POST localhost:8080/api/rfqs -H 'content-type: application/json' -H 'X-Desk-User: sales.amy' \
  -d '{"clientId":"CLIENT-ACME","underlier":"SX5E","productType":"AUTOCALLABLE",
       "notional":5000000,"currency":"EUR","maturityMonths":36}'

# 2. A moment later it's QUOTED; the quote is valid for 30 s. Note the ETag (RFQ version).
curl -si localhost:8080/api/rfqs/$ID

# 3. Accept the quote, optionally with If-Match -> ACCEPTED, booking requested on Kafka
curl -s -X POST localhost:8080/api/rfqs/$ID/accept -H 'content-type: application/json' \
  -H 'If-Match: "1"' -H 'X-Desk-User: trader.jane' -d "{\"quoteId\":\"$QUOTE_ID\"}"

# 4. The booking system confirms -> BOOKED with a trade id; full audit trail:
curl -s localhost:8080/api/rfqs/$ID/transitions

# Watch the Kafka flow
docker compose exec redpanda rpk topic consume quote-events booking-events booking-confirmations
```

**Other paths to try:**

* **Pricing refused**: a notional above 50,000,000 is over the desk limit.
* **Booking rejected**: a `clientId` starting with `BLOCKED` simulates a failed KYC check.
* **Client declines**: `POST /api/rfqs/{id}/decline`.
* **Quote expires**: do nothing for 30 s.

### API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/rfqs` | Submit an RFQ; triggers pricing (201, `ETag`) |
| `GET` | `/api/rfqs/{id}` | RFQ with quotes and current version (`ETag`) |
| `GET` | `/api/rfqs?status=QUOTED&page=0&size=20` | List RFQs, optionally by status |
| `GET` | `/api/rfqs/{id}/transitions` | Audit trail of state changes |
| `POST` | `/api/rfqs/{id}/accept` | Accept the active quote: `{"quoteId": ...}`; optional `If-Match` |
| `POST` | `/api/rfqs/{id}/decline` | Decline the active quote |
| `GET` | `/actuator/health` | Health, including `db` and `kafka` components |
| `GET` | `/actuator/prometheus` | Metrics, Prometheus format |

**Errors** are returned as RFC 9457 problem details:

| Status | When |
|---|---|
| 400 | Validation failure |
| 404 | Unknown RFQ |
| 409 | Invalid state transition, expired quote, or a concurrent update that lost the optimistic-lock race |
| 412 | `If-Match` doesn't match the current version |

### Configuration

| Property / env var | Default | Meaning |
|---|---|---|
| `DB_URL`, `DB_USER`, `DB_PASSWORD` | `jdbc:postgresql://localhost:5432/workflow`, `workflow`, `workflow` | PostgreSQL |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | Kafka / Redpanda |
| `workflow.quote-ttl` | `30s` | How long a quote stays firm |
| `workflow.expiry-check-interval` | `1s` | Quote expiry sweep interval |
| `PRICING_BASE_URL` | empty (this app's stub) | Pricing service base URL |
| `stubs.enabled` | `true` | Run the simulated pricing and booking systems in-process |
| `stubs.pricing.max-notional` | `50000000` | Pricing stub refuses above this |
| `stubs.booking.blocked-client-prefix` | `BLOCKED` | Booking stub rejects these clients |

## Tests

`TradeWorkflowIntegrationTest` is the project's only test class. It starts the application on a
random port against PostgreSQL 16 and Redpanda containers (Testcontainers). An independent Kafka
consumer records every message, so each scenario checks both the database state and the Kafka
flow.

| Scenario | Verifies |
|---|---|
| Happy path | PENDING → QUOTED → ACCEPTED → BOOKED; `pricing-responses`, `quote-events` (created, accepted), `booking-events`, `booking-confirmations`; transition actors; quote TTL |
| Pricing refused | Over-limit notional → REJECTED(PRICING_REFUSED); no quote, no quote event |
| Booking rejected | Blocked client → ACCEPTED → REJECTED(BOOKING_REJECTED) |
| Quote expiry | REJECTED(QUOTE_EXPIRED) after the TTL; `QUOTE_EXPIRED` event; a late accept gets 409 |
| Client declines | REJECTED(CLIENT_DECLINED); nothing sent to booking |
| Concurrent accepts | 8 simultaneous accepts → exactly one 200, seven 409s (optimistic lock), one booking event |
| Stale `If-Match` | 412 with `currentVersion`; state unchanged; retry with the current version succeeds |
| Duplicate pricing response | Ignored by the idempotent consumer: still one quote, version unchanged |
| API errors | 409 invalid transition, 404 unknown RFQ, 400 validation |
| Observability | `/actuator/health` UP with `db` and `kafka`; workflow metrics in `/actuator/prometheus` |

```bash
mvn verify          # Spotless check, compile with -Werror, integration tests (needs Docker)
mvn spotless:apply  # fix formatting
```

**Latest local run:** 10 scenarios, 0 failures, about 17 s, plus container start-up. In the
concurrent-accept scenario, all 7 losing requests were rejected by JPA optimistic locking in each
of 3 runs.

## CI/CD

`.github/workflows/ci.yml` runs on every push and pull request:

1. **Build**:
   * Spotless lint (google-java-format).
   * `mvn verify`, which compiles with `-Xlint:all -Werror` and runs the integration tests with
     Testcontainers.
   * Uploads the test reports and the jar.
2. **Docker**: builds the image, starts the compose stack, and runs an RFQ round trip with
   `curl`: submit, then quote, then accept, then BOOKED.
3. **Publish** (pushes to `main` only): pushes the image to
   `ghcr.io/shammykr/trade-workflow-engine`.

## Tech stack

Java 21, Spring Boot 3.5 (Web, Data JPA, Validation, Actuator), Spring for Apache Kafka,
PostgreSQL 16 with Flyway, Micrometer with Prometheus, Redpanda (Kafka API), Testcontainers,
Awaitility, Spotless, Docker, Docker Compose and GitHub Actions.
