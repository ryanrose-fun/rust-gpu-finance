# Continuous GPU reconciliation

The GPU engine compares payment evidence; it does not execute payments. Kafka
ingestion, durable storage and the query API run on the host. Reconciliation stays
on the GPU with no CPU fallback.

## Ingestion controls

Two ordered workers independently consume the internal and provider partitions.
There is at most one prepared/in-flight batch per partition. A shared semaphore
bounds total HTTP requests. An empty/slow source cannot block the other source.

| Setting | Default | Bound |
|---|---:|---:|
| `PAYMENT_STREAM_CONCURRENCY` | 2 | 1–2 |
| `PAYMENT_STREAM_BATCH_EVENTS` | 2048 | 1–4096 |
| `PAYMENT_STREAM_BATCH_BYTES` | 1048576 | 1–8388608 |

Batching is opportunistic: combine consecutive records already fetched, without
waiting to fill a batch. Stop at a repeated payment or event ID, preserving each
source version and intermediate finding. A single record that exceeds the configured
limit stops ingestion with its cursor unchanged; it is never silently split or dropped.
Fetches are bounded to 4 MiB, individual records to 1 MiB and 512 events. Empty polls
wait at most 5 ms at the broker and sleep 5 ms locally.
Each worker retains unused records from its current fetch until subsequent batches
consume them. It does not repeatedly fetch the same backlog for every HTTP request.

The consumer writes the exact batch payload before HTTP delivery. The request ID
identifies its topic and offset range. On restart, stored payloads take precedence
over current batch settings. Each acknowledged batch advances only its own contiguous
offset range, with all per-record metrics committed atomically. Failure stops both
workers; other pending batches remain on disk. The old single-pending-record database
is migrated transactionally. Do not run an older consumer against an upgraded database.

## Live findings and investigation

Use the GPU API with an authorized bearer key:

```powershell
$headers = @{Authorization="Bearer $env:RECONCILE_API_KEY"}
$api = 'http://127.0.0.1:3000'
Invoke-RestMethod "$api/v1/gpu/findings?after=0&limit=1" -Headers $headers |
    ConvertTo-Json -Depth 20
```

`entries` are committed request batches. Each contains the GPU's changes and receipts,
the original normalized event evidence, request ID, sequence and recorded timestamp.
Changes include before/after status and `Appeared`, `Changed` or `Resolved`. Missing
counterparts are evidence states too. A batch with no changes still advances the cursor.

Persist `next_cursor` **after** your downstream tool processes the entire page, then
poll with `after=<next_cursor>`. Replaying a page is safe when the downstream tool
deduplicates by journal/tenant and sequence. The feed reads the existing durable
journal: no separate notification can be lost between commit and publication. This
is a resumable polling API, not an outbound Kafka topic or push webhook. Replication,
retention/archival and external subscriber acknowledgements are not implemented here.

`GET /v1/gpu/payments/{id}/history?after=0&limit=10` retrieves indexed evidence for one
payment, including stale/retried/conflicting attempts recorded by the GPU. History
preserves input evidence rather than treating every attempted update as applied.
Use receipts for the outcome and `GET /v1/gpu/payments/{id}` for current GPU values.

```powershell
$body = @{request_id=[guid]::NewGuid().ToString('N'); note='Provider contacted'; acknowledged=$true} |
    ConvertTo-Json
Invoke-RestMethod "$api/v1/gpu/payments/PAY-001/investigation" -Method Post `
    -Headers $headers -ContentType 'application/json' -Body $body
Invoke-RestMethod "$api/v1/gpu/payments/PAY-001/investigation?after=0&limit=10" -Headers $headers
```

Notes are append-only, actor-attributed, FULL-committed and idempotent by request ID.
Retry the same ID with the same body after a timeout. Acknowledgement is a triage
decision, not a change to payment evidence; `false` reopens acknowledgement and `null`
adds a note without changing it. Fold notes in sequence to obtain the latest decision.
Feed/history pages accept 1–20 batches; notes accept 1–100 entries. Start with small pages.

## Matching windows

`RECONCILE_GPU_MATCH_WAIT_MS` defaults to 30000 (0–86400000). Current payment queries
include `first_seen_at_ms`, `matching_deadline_ms` and `awaiting_counterpart`.
This lets a UI distinguish a counterpart still within its arrival window from an
overdue missing source, without altering the GPU's evidence result. Deadline evaluation
happens on reads; expiry alone does not append a feed event. Historical requests from
before this feature have unknown timestamps (`null`), never fabricated ones.

## Scoped credentials and company isolation

GPU mode supports the existing `RECONCILE_TENANTS_FILE` format with **exactly one
tenant per process**, no CPU partition setting, and no simultaneous legacy
`RECONCILE_API_KEY`. Its `events.sqlite3` and `gpu.sqlite3` must be in the same dedicated
directory. The GPU journal stores its owner and rejects another tenant ID on restart.
An existing legacy journal remains bound to `legacy-single-tenant`; switching its
ownership requires an explicit migration, not a configuration rename.

| Role | GPU permissions |
|---|---|
| Administrator | Read, write either source, annotate, checkpoint |
| InternalWriter | Read; write Internal events only |
| ProviderWriter | Read; write Provider events only |
| Auditor | Read only |
| Investigator | Read; append notes/acknowledgements |

Use separate secrets for each role. The Kafka consumer accepts
`PAYMENT_STREAM_INTERNAL_KEY` and `PAYMENT_STREAM_PROVIDER_KEY`; each overrides the
legacy shared key for its own source. Keep credentials out of source control and logs.
Supervisors can set `PAYMENT_STREAM_KEY_STDIN=1` and send the shared fallback key
as one newline-terminated line through a private stdin pipe instead of an environment
variable. EOF without a key fails closed. The hardware test launcher uses this mode
and clears inherited source keys, binding credential delivery to its child process.
The API supports `RECONCILE_API_KEY_STDIN=1` in the same way, reading its key before
opening storage or listening. The normal key validation and tenant-file restrictions
still apply. Scoped tenant configurations continue to obtain keys from their config file.
For separate companies, use separate processes, ports, directories, topic/run namespaces
and secrets. A shared GPU is not a hardware isolation boundary. This version does not
allocate tenant-isolated slots inside one GPU worker.

Per-process limits remain explicit: `RECONCILE_GPU_CAPACITY` (maximum 2M payments),
`RECONCILE_CONCURRENCY`, `RECONCILE_MAX_BODY_BYTES`, `RECONCILE_REQUESTS_PER_SECOND`,
and the bounded 32-command GPU queue. Reserve capacity for readers when setting
ingestion concurrency; a saturated actor also delays queries.

## Map source formats

See `examples/mappings/` for two synthetic checkout/provider formats. Mappings use
JSON pointers, explicit monotonically ordered source versions, integer minor-unit
amounts, currency codes and an allowlist of source statuses. Fractional/negative
amounts, missing IDs, unsupported currencies and unmapped statuses are rejected.
Event IDs must be stable and unique across sources within this tenant. A timestamp
alone is not necessarily a unique or correctly ordered source version.

```powershell
.\target\release\payment-stream.exe import $broker $run Internal examples/mappings/checkout.json examples/mappings/checkout-events.json
.\target\release\payment-stream.exe import $broker $run Provider examples/mappings/provider.json examples/mappings/provider-events.json
```

Normalize all input before publishing. Imports accept at most 10000 records / 16 MiB.
On ambiguous broker failure, rerun the same file with unchanged IDs; delivery is
at-least-once and GPU deduplication handles retries. These are tested configurable
format adapters, not certified connections to any bank or payment provider.

## Operational measurements

```powershell
.\target\release\payment-stream.exe status $broker $run $api C:\path\to\cursor.sqlite3
Invoke-RestMethod "$api/v1/gpu/health" -Headers $headers
```

`status` opens the cursor database read-only and reports each partition's high-watermark,
durable cursor, lag in records and age of its oldest prepared batch. It does not read
the entire backlog to estimate the oldest unprepared event. Repeat it periodically.
Health reports available preparation slots, committed batches, capacity and checkpoint
progress. Existing `/metrics` covers API admission and HTTP latency.
`reconcile_gpu_admitted_requests` exposes current GPU preparation/admission usage
without adding a query to the GPU actor queue.

Consumer JSONL includes fetch, normalization/batching, pending-write and admission
times. Durable completion metrics remain per input record. `RECONCILE_GPU_PROFILE=1`
logs preparation, queue wait, identity lookup, GPU compute, journal writes and FULL
commit time. Profiling is diagnostic overhead and should be held constant in comparisons.

## Acceptance modes

Set `STREAM_TEST_MODE` to `combined`, `burst`, `restart` or `steady` before running the
explicit Kafka/GPU acceptance test. Burst and restart cases isolate their respective
effects; every case also verifies final state after a restart. The separate
`scoped_product_workflow_survives_restart` test exercises source permissions,
investigation idempotency, feed continuation and historical evidence on actual GPU hardware.

See the [streaming guide](KAFKA_STREAMING.md) for broker setup and the
[measurement report](performance/gpu-streaming-product.md) for results and limitations.
