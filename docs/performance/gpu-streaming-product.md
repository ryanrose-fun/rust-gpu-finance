# GPU streaming: implementation and validation

15 November 2026. The implementation adds independent ordered source workers,
opportunistic batching, retained fetch buffers, durable per-source cursors, stage
measurements, scoped GPU credentials, findings/history and investigation notes.
See [operations](../GPU_OPERATIONS.md) for interfaces and configuration.

## Completed validation

- 145 portable tests passed; formatting and strict all-feature/all-target Clippy passed.
- The scoped GPU workflow passed on the RTX 5070 Laptop GPU: forbidden source writes,
  auditor/investigator permissions, idempotent notes, original evidence and resolved
  findings surviving restart.
- Small Kafka/GPU recovery checks passed, including replay after an API acknowledgement
  before the consumer cursor commit, exact event/request accounting and final state.
- Earlier 1.1-million-payment serial and concurrent cases passed their correctness
  checks. Their timings are not published as the final optimization comparison.

## Performance comparison status

The final comparison was stopped at the owner's request to conclude and publish.
No new headline speedup or bank-capacity claim is made. The final larger-batch,
burst-only and restart-only comparison remains to be completed.

Two issues were found during the experiments:

1. The initial worker discarded unused fetched records and fetched them again. It now
   retains one bounded fetch buffer per source; a regression test checks batch boundaries.
2. One batched run failed strict request accounting: extra API batches were absent from
   the supervised consumer's log and cursor database. Original event deduplication and
   sampled final states held, but that run failed acceptance. The launcher of those
   extra requests was not established; security software involvement is unconfirmed.

The harness now supplies both API and consumer credentials through private stdin pipes
only after spawning the child, with inherited consumer source keys cleared. It still
rejects unexplained extra journal requests. A subsequent Windows bind collision also
prevented one case from starting; that case generated no accepted workload result.
Process launch delays remain visible separately from GPU replay time. Diagnostic
logs/databases and preliminary reports are retained locally, not shipped as results.

## Reproduce the comparison

Start the broker using [Kafka setup](../KAFKA_STREAMING.md), then build once:

```powershell
cargo build --release --locked --features gpu,streaming --bins
$env:KAFKA_BROKER = '<WSL-IP>:19092'
$env:STREAM_TEST_SECONDS = '180'
$env:STREAM_TEST_RATE = '5000'
$env:STREAM_TEST_MODE = 'combined'
$env:PAYMENT_STREAM_CONCURRENCY = '2'
$env:PAYMENT_STREAM_BATCH_EVENTS = '2048'
cargo test --release --locked --features gpu,streaming --test kafka_gpu_stream sustained_burst_and_restart -- --ignored --nocapture
```

Compare concurrency/batch settings `1/512`, `2/512`, and `2/2048`, sequentially without
other benchmarks or compilation. Each combined case produces 1.1 million payments,
3.3 million unique events and deliberate retries: 5,000 payments/source/s for 180 seconds,
with a ten-second 5x burst. It kills the consumer after durable ACK but before cursor
commit and restarts API/consumer while producers continue. Use 60 seconds and mode
`burst` or `restart` to isolate those effects. Every case checks final recovered state.

Hold broker configuration, hardware, history indexing and profiling constant. The lab
uses one local Kafka 4.0.1 broker, RF1 and a 512 MiB heap. These are integration tests,
not maximum throughput or production-bank capacity measurements.

Record latency ends at durable API acknowledgement, before local cursor commit; total
durable elapsed time waits for all cursors. GPU `compute_ms` includes host packing and
transfers, not just kernel execution. Initial launch and final verification are outside
the ingestion timer; mid-stream launch/recovery delays are included. Prior polling
measurements used a different feature set and broker heap and are historical context.
