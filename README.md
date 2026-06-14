# crowncoins-production-missions — OpenTelemetry instrumentation reference

A small, always-on **NestJS (Fastify) + Redis + Kafka** service that reproduces the
`crowncoins-production-missions` stack and demonstrates **correct OpenTelemetry
instrumentation** as seen in Coralogix — including the parts that are commonly
missing or misconfigured:

1. **Kafka producer *and* consumer spans, connected with span links** (the usual gap:
   only producer spans exist, so Kafka shows up one-sided in the Service Map).
2. **Full HTTP route names** (`Fastify/GET//missions/v2/get-user-missions`) instead of
   bare `GET`/`POST`.
3. **Non-web background transactions** (`Monetization/missions-progress-process`, …).
4. **Clean, single-line JSON logs** (no fragmented multi-line client dumps).

It runs under the **OpenTelemetry Operator auto-injection** (no SDK bootstrapping in
app code) and continuously generates its own traffic so traces always flow.

---

## The problem this solves

In the live service, **100% of Kafka spans are `span.kind = producer` — there are no
consumer spans.** Because Kafka is asynchronous, a producer and its consumer are in
*different traces*; without a consumer span (and a link back to the producer) Coralogix
can only draw Kafka as a destination. The fix is to emit a **top-level consumer span**
carrying the required messaging attributes **and a span link** to the producer context
propagated through the Kafka message headers.

### Required Kafka span attributes (both producer and consumer)

| Attribute | Producer | Consumer |
|---|---|---|
| `span.kind` | `PRODUCER` | `CONSUMER` |
| `messaging.system` | `kafka` | `kafka` |
| `messaging.destination.name` | topic | topic |

> Setting `span.kind = internal` (or omitting `messaging.*`) prevents Coralogix from
> detecting Kafka. The **span link must be on the top-level consumer span**, not a child.

See [`src/kafka.service.ts`](src/kafka.service.ts) — the producer injects W3C
`traceparent` into the message headers; the consumer starts a span from `ROOT_CONTEXT`
(its own trace root) with `span.kind=CONSUMER` + the messaging attributes, extracts the
producer context from the headers, and attaches it as a **span link**.

---

## Architecture

```
 load generator (built-in) ─HTTP─▶ Fastify (NestJS)
                                     ├─ GET  /missions/v2/get-user-missions ─▶ Redis
                                     └─ POST /missions/progress ─▶ Kafka PRODUCER ──┐
                                                                                    │ (traceparent in headers)
 Kafka topic: missions-progress / rank                                             │
                                     Kafka CONSUMER ◀───────────────────────────────┘
                                       │  (span.kind=CONSUMER + span LINK to producer)
                                       ├─▶ Redis (apply progress)
                                       └─▶ non-web txns: Monetization/missions-progress-{process,build-lookups}
```

| Component | Purpose |
|---|---|
| `src/main.ts` | Nest+Fastify bootstrap. Fastify hook that renames the HTTP server span to the full route (see below). |
| `src/missions.controller.ts` | HTTP endpoints mirroring the real service. |
| `src/redis.service.ts` | ioredis (auto-instrumented → Redis spans). |
| `src/kafka.service.ts` | `@confluentinc/kafka-javascript` producer/consumer + **manual Kafka spans & span links** + the `Monetization/*` non-web transactions. |
| `src/loadgen.service.ts` | Built-in load generator so traces always flow. **Remove in production.** |

---

## How instrumentation is wired (auto-injection)

No OTel SDK is started in app code. The **OpenTelemetry Operator** injects it via an
`Instrumentation` custom resource + a pod annotation.

1. **`Instrumentation` CR** ([`k8s/instrumentation.yaml`](k8s/instrumentation.yaml)) —
   defines the Node.js auto-instrumentation image, the OTLP endpoint
   (`http://$(OTEL_IP):4318`, the node-local Coralogix agent), sampler and propagators.
2. **Pod annotation** ([`k8s/deployment.yaml`](k8s/deployment.yaml)):
   ```yaml
   annotations:
     instrumentation.opentelemetry.io/inject-nodejs: "nodejs-instrumentation"
   ```
   The operator injects an init container + `NODE_OPTIONS=--require .../autoinstrumentation.js`.
3. **Manual spans** use the `@opentelemetry/api` already wired to the injected global
   tracer provider — that is how the Kafka spans and `Monetization/*` transactions are
   created without any SDK setup.

### NestJS-on-Fastify specifics

- **Fastify instrumentation + route names.** The Fastify framework is instrumented with
  `@fastify/otel`, registered via `registerOnInitialization` in
  [`otel/coralogix-autoinstrumentation.js`](otel/coralogix-autoinstrumentation.js) (a
  plain app-level Fastify plugin cannot be used, because Nest seals the Fastify instance
  before app code runs). Its `requestHook` names the server span
  `Fastify/<METHOD>/<route>` and sets `http.route`; `instrumentHooks: false` drops the
  noisy per-lifecycle-hook spans. Because the Nest adapter also produces a **duplicate**
  `@opentelemetry/instrumentation-http` incoming server span, that one is suppressed so
  `@fastify/otel` owns the single, route-named server span.
- **Load-gen noise** is removed with `OTEL_NODE_DISABLED_INSTRUMENTATIONS=undici` in the
  Deployment (the built-in generator's outgoing client calls — lab-only).

---

## Build & deploy

Prerequisites: an EKS/K8s cluster with the **OpenTelemetry Operator** and a node-local
OTLP collector (the Coralogix agent) reachable at `http://$(status.hostIP):4318`, and an
image registry the cluster can pull from.

```bash
# 1. Build & push the auto-instrumentation init image (adds @fastify/otel)
cd otel
docker buildx build --platform linux/amd64 -f Dockerfile.autoinstrumentation \
  -t <YOUR_REGISTRY>/crowncoins-sim-autoinstrumentation:fastify --push .
cd ..
# -> set this image in k8s/instrumentation.yaml (spec.nodejs.image)

# 2. Build & push the app image
docker buildx build --platform linux/amd64 \
  -t <YOUR_REGISTRY>/crowncoins-sim-missions:latest --push .

# 3. Point k8s/deployment.yaml at your app image, then apply (order matters):
kubectl apply -f k8s/instrumentation.yaml   # Instrumentation CR (same namespace as the app)
kubectl apply -f k8s/redis.yaml
kubectl apply -f k8s/kafka.yaml             # single-node KRaft Kafka for the lab
kubectl apply -f k8s/deployment.yaml        # app Deployment + Service
```

> The manifests reference an internal registry and a copy of the cluster's
> `Instrumentation` config — **replace the image and OTLP/registry details with your
> own**. `k8s/kafka.yaml` and `k8s/redis.yaml` are lab conveniences; in production point
> `KAFKA_BOOTSTRAP` / `REDIS_URL` at your real brokers/cache.

### Key environment variables (Deployment)

| Var | Example | Notes |
|---|---|---|
| `OTEL_SERVICE_NAME` | `crowncoins-production-missions` | Service name in Coralogix |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://$(OTEL_IP):4318` | `OTEL_IP` from `status.hostIP` |
| `OTEL_NODE_DISABLED_INSTRUMENTATIONS` | `fastify,undici` | see above |
| `KAFKA_BOOTSTRAP` | `missions-kafka:9092` | Kafka brokers |
| `REDIS_URL` | `redis://missions-redis:6379` | Redis |
| `LOAD_RPS` | `8` | built-in load generator rate |

---

## Verifying in Coralogix

- **Service Map / Distributed Tracing** → Kafka appears between the producer and
  consumer (both `messaging.system=kafka`), connected via the span link.
- **APM → Transactions → Web** → `Fastify/GET//missions/v2/get-user-missions`, etc.
- **APM → Transactions → Non-web** → `Monetization/missions-progress-process`,
  `Monetization/missions-progress-build-lookups`.
- **Logs** → clean single-line JSON.

DataPrime sanity check (span kinds for the service):

```
source spans
| filter $d.process.serviceName == 'crowncoins-production-missions'
| filter $d.tags['messaging.system'] == 'kafka'
| groupby $d.tags['span.kind'] aggregate count() as cnt
```

Expect **both** `producer` and `consumer`.
