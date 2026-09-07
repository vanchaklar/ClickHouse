"""Summarize paired runs without hiding unsupported or missing measurements."""
import json
import pathlib
import statistics
import sys

root=pathlib.Path(sys.argv[1])
print('# Accuracy and upstream comparison\n')
print('PR head: `33f39cd7`; actual PR binary test-merge: `95627d85` (parents `0b7a0b77`, `33f39cd7`); upstream binary: `3594fb13`. Same runner, separate server data directories, two query threads, three warm repetitions. Different source baselines mean timing differences cannot be attributed solely to the PR.\n')
print('Numerical oracle: 80-digit Decimal arithmetic on the exact Float64 inputs, 103 groups including cancellation and large magnitudes. L1 error divides absolute error by the sum of absolute input contributions. Relative error is undefined for zero truth. Query-result and predicate-result caches are disabled in all runs.\n')
print('| Build / flag | Method | Max relative error | Max L1 error | Sign errors | Rank accuracy | Top-10 recall |\n|---|---|---:|---:|---:|---:|---:|')
measurements={}
for mode in ['upstream','fork-off','fork-on','full']:
    p=root/mode/'results'
    if not (p/'accuracy.json').exists():
        print(f'| {mode} | MISSING | | | | | |')
        continue
    for name,a in json.loads((p/'accuracy.json').read_text()).items():
        print(f'| {mode} | {name} | {a["max_relative_error"]:.3g} | {a["max_l1_normalized_error"]:.3g} | {a["sign_errors"]} | {a["positional_accuracy"]:.1%} | {a["top10_recall"]:.1%} |')
    m=p/'measurements.json'
    if m.exists():measurements[mode]=json.loads(m.read_text())
print('\nCancellation details and all numeric residuals are retained in each `accuracy.json`; passing the scaled bound does not imply a small relative error near zero.\n')
print('| Build / flag | Check count | Failed checks | Missing query metrics |\n|---|---:|---:|---:|')
for mode,m in measurements.items():
    c=json.loads((root/mode/'results/correctness.json').read_text())
    print(f'| {mode} | {len(c)} | {sum(not x["passed"] for x in c)} | {sum(not x.get("query_log_record_found",False) for x in m)} |')
print('\n## Common query timing\n\n| Build / flag | Case | Median wall ms |\n|---|---|---:|')
for mode,m in measurements.items():
    for name in dict.fromkeys(r['name'] for r in m if r['name'].startswith('portable_1000000_')):
        group=[r for r in m if r['name']==name]
        print(f'| {mode} | {name} | {statistics.median(r["wall_seconds"] for r in group)*1000:.3f} |')
print('\n## Selective indexes and projections\n\nRows are ingested as 16 separate score bands; predicate selects the highest 0.1%. Top-100 uses score order. Float64 layouts precompute the score at time 1000; native layouts carry the decaying type. Their preprocessing costs are outside these read timings.\n')
print('| Build / flag | Case | Median wall ms | Rows read | Marks | Projection used |\n|---|---|---:|---:|---:|---|')
for mode,m in measurements.items():
    for name in dict.fromkeys(r['name'] for r in m if r['name'].startswith('indexed_')):
        group=[r for r in m if r['name']==name]
        print(f'| {mode} | {name} | {statistics.median(r["wall_seconds"] for r in group)*1000:.3f} | {statistics.median(r["read_rows"] for r in group):g} | {statistics.median(r["selected_marks"] for r in group):g} | {any(r["projections"] for r in group)} |')
print('\nThe `ordered_top100` variant uses matching descending directions for score and identity so the ascending projection can be read backwards. The original `top100` keeps mixed directions as a control. Exact indexed result identities are checked before timing; inspect the corresponding plans and query metrics for optimizer evidence. Flag-off rejection details are in `fork-off/results/flag_off.json`. Upstream has no custom decaying-type equivalent.\n')
