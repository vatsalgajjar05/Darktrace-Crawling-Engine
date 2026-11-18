# Benchmark Report

Date: 2026-11-18

## Measured controlled benchmark

The repository's controlled benchmark harness measured:

| Metric | Result |
|---|---:|
| Jobs attempted | 24 |
| Successful jobs | 24 |
| Failed jobs | 0 |
| Success rate | 100% |
| Parallel workers | 4 |
| Wall time | 11.63 s |
| Jobs per minute | 123.8 |
| Mean latency | 1.932 s |
| P50 latency | 1.856 s |
| P95 latency | 2.215 s |
| Maximum latency | 2.231 s |
| Minimum latency | 1.825 s |

This was a controlled repeatable workload. It is not an external-source throughput claim or production benchmark.

## Historical fairness comparison

The existing QA evidence reports fixed 8-job batches:

| Concurrency | Jobs | Success | Wall time | Jobs/minute |
|---:|---:|---:|---:|---:|
| 2 | 8 | 8 | 14.69 s | 32.67 |
| 4 | 8 | 8 | 6.49 s | 73.93 |
| 8 | 8 | 8 | 7.11 s | 67.56 |

Concurrency 4 was the best result for that workload. External source request limits were respected; no stress overload was sent to external sources.
