# Time-decay end-to-end benchmarks

Benchmark design: https://chatgpt.com/s/t_6a9e737e3fdc81918847e64226599e58

Related: https://github.com/ClickHouse/ClickHouse/pull/110934

The initial fork run uses the Linux x86_64 release binary built for PR head
`33f39cd7762b2ecd57e770e3d7367e07857a51fa`. Its embedded build commit is GitHub's
test merge `95627d85bb148c36357070442983df0c59037ebb`, with parents
`0b7a0b77620f1396735dea8d3593e63706fc8329` and that PR head. Its upstream build passed:
https://github.com/ClickHouse/ClickHouse/actions/runs/34022722703/job/101466279201
The workflow verifies pinned archive and binary checksums, including after a cache hit.
The benchmark branch contains only the harness and workflow additions to that source.

## Initial bounded run

- Primitive comparisons, full sorting and Top-K at 1K, 10K, 100K and 1M rows.
  Compare native values, `exponentialTimeDecayingValueAt`, and independent
  `Float64`/timestamp arithmetic; include negative, zero and positive values.
  Construct values with `initializeAggregation` using the actual aggregate
  implementation. Do not hand-encode values with SQL `log()`: its approximation
  can change near-tie order relative to the original Float64 inputs.
- Aggregate state creation and merging, checked against explicit exponential arithmetic.
- Four `AggregatingMergeTree` layouts: identity order, `minmax` index, sorted
  projection, and both. A raw `MergeTree` holds the explicit arithmetic baseline.
- The same logical dataset split across 1, 2, 4, 16 and 64 separately inserted parts,
  with unequal key-dependent fragment weights and background merges stopped.
  Projection-equipped tables explicitly use `deduplicate_merge_projection_mode=rebuild`. Compare `FINAL` and explicit aggregation before
  and after timed `OPTIMIZE FINAL`. Physical-row Top-K is a separate comparison
  and is not represented as logical merged Top-K. Capture active part counts.
- A threshold predicate tests `minmax` pruning; a sort alone does not imply index use.
- Growing history in ten batches with uniform and injected hot relations, sampled
  at 10%, 50% and 100% of one million observations, with global Top-64 and Top-128, plus the 16 most active sources and
  Top-64/128 targets within each selected source.
- A deliberately fragmented counterexample demonstrates why arbitrary candidate
  truncation before merging is not an exact optimization. Candidate-first timings use a fixed 128-row candidate budget, merge all rows
  for surviving keys, and report recall against complete Top-100. These are
  explicitly approximate; no exact speedup claim is made without a candidate bound.

Run on an isolated Linux machine with the verified binary at `build/clickhouse`:

```bash
SOURCE_SHA=33f39cd7762b2ecd57e770e3d7367e07857a51fa \
  bash benchmarks/time_decay/server.sh \
  --sizes 1000,10000,100000,1000000 --keys 10000 \
  --histories 1000000 --repeats 3
```

The script starts a dedicated loopback-only server on HTTP 18123/TCP 19000 and
stops it on exit. Each run creates a uniquely named database. Server memory is
capped at 6 GB and queries at 4 GB, with two query threads and a 180-second query
timeout. Use a dedicated machine: those ports and the data directory must be free.
All calculation budgets are zero; correctness is evaluated at fixed time 1000
with decay length 600. Timed read queries get one warm-up and three repetitions.
Mutating queries execute exactly once, without warm-up. Query case order is
reproducibly shuffled. This measures warm-cache behavior, including HTTP request
overhead, and does not claim production throughput.

## Outputs and failure policy

`build/results/` contains every timed SQL query, relevant execution plans,
correctness comparisons and result-order hashes, raw per-query metrics, CPU and
build identity, and a Markdown timing summary. `system.query_log` supplies server
duration, read rows/bytes, peak memory, user/system CPU time, selected parts and
marks, allocation event counts when available, and projections used. Preserve
raw plans to verify optimizer selection: merely creating an index or projection
is not proof it ran.

Unsupported index/projection DDL is recorded in `errors.json` and causes a failed
run after the remaining supported layouts finish. Query errors and incorrect
results stop the matrix and retain partial measurements. Missing metrics must
not be interpreted as zero resource usage. Numeric aggregate comparisons use
relative and absolute tolerances of `1e-10`; ordering uses exact identity order
with an explicit tie breaker. The generator avoids extreme exponent ranges;
underflow/tie collapse is a separate correctness workload.

## Accuracy and upstream comparison

The follow-up workflow runs upstream commit `3594fb13167f4656d9421b90e2f0c5517a041300`
(successful `amd_release` build: https://github.com/ClickHouse/ClickHouse/actions/runs/34112703786/job/101712837431)
and the PR test-merge binary `95627d85bb148c36357070442983df0c59037ebb` with the experimental
flag off and on, sequentially on the same machine. Both binaries are checksum-pinned.
Upstream has no experimental setting or custom decaying type; its portable baseline
uses existing window functions and ordinary `Float64` arithmetic. All modes use
identical generated inputs, query-thread limits and warm-up/repetition counts.
The differing source commits are a limitation when attributing performance changes.
Both query-result and predicate-result caches are disabled, so a warm-up cannot
mask skip-index benefits by remembering which granules fail a predicate.

`accuracy.json` records absolute, relative and L1-normalized errors against an
80-digit `Decimal` oracle for 103 groups, including negative timestamps, signed
inputs, exact zero, large magnitudes, and severe cancellation. It preserves original
inputs and full per-group results, measures sign errors, rank agreement and Top-10
recall. A passing L1-normalized bound does not imply good relative accuracy when
large terms cancel. Ordinary ordering checks additionally record positional accuracy
and recall. Flag-off runs assert explicit experimental aggregate/type rejection
while measuring the existing window and arithmetic paths normally.

Selective layout cases insert one million rows into 16 separate score bands,
query the highest 0.1% with a value predicate, and request score-ordered Top-100.
Top-100 includes both a mixed-direction control (`d DESC, id ASC`) and a fully
descending order (`d DESC, id DESC`) matching a reverse projection scan.
Compare plain, `minmax`, sorted projection, and both, using both the native carrier
and an upstream-compatible precomputed `Float64` score at time 1000. Read timings
exclude score construction. Exact expected identities are checked; `EXPLAIN`, rows
read, selected marks and actual projection use show whether the optimizer benefits.
The original fragmented `AggregatingMergeTree` suite still tests logical merged
results, including cases where projections cannot substitute for aggregation.

Run `--suite comparison --mode upstream`, `fork-off`, or `fork-on` with `server.sh`.
`BENCH_BINARY` selects the executable and `BENCH_DIR` isolates server data/logs.
The combined run summary is generated by `report.py`.

## Remaining scale-up

The linked plan's 10M/100M primitive sizes and 10M/100M/1B observation histories
are later stages, not implied by a passing initial run. The runner accepts larger
sizes, but full-order validation currently materializes identities in Python and
must be made streaming before 100M-row validation. The initial history workload
has 10K sources and 1,000 possible targets/source (configurable with `--sources`
and `--targets`). Zipf distribution and realistic signed cancellation workloads
still need separate extensions and runs. GitHub-hosted runner variability requires
repeated independent runs before publishing stable performance conclusions.
# Calculation-budget sweep

All original results used calculation budget `0` (cutoff disabled); upstream does not expose this custom setting. New measurement, correctness, and numerical-accuracy records explicitly include `calculation_budget` (`null` when unsupported).

The separate `time-decay-budget.yml` workflow compares budgets `0,1,4,8,16` on the same pinned PR binary. Nonzero values bound the calculation-index distance in decay lengths; they are neither error percentages nor execution-time limits. The suite measures native indexed inputs in forward and reverse order, eight-way state merging, and raw `(value,time)` inputs as an unaffected control. Positive and signed datasets each contain one million rows and 1,000 groups. An independent 80-digit Decimal oracle supplies numerical residuals, rank accuracy, and Top-K recall beside latency and speedup. Approximate results are reported without claiming the exact-mode accuracy guarantee.

Run locally with `bash benchmarks/time_decay/server.sh --suite budget --sizes 1000000 --budgets 0,1,4,8,16 --repeats 3 --out build/results`, then `python3 benchmarks/time_decay/budget_report.py build/results`. This is a focused aggregate cutoff comparison; prior index/projection comparisons remain exact-budget controls.
