"""Display measured budget/accuracy tradeoffs without implying an error guarantee."""
import json
import pathlib
import statistics
import sys

p=pathlib.Path(sys.argv[1])
a=json.loads((p/'budget_accuracy.json').read_text())
m=json.loads((p/'measurements.json').read_text())
latency={r['name']:statistics.median(x['wall_seconds'] for x in m if x['name']==r['name'])*1000 for r in a}
baseline={(r['dataset'],r['path']):latency[r['name']] for r in a if r['calculation_budget']==0}
print('# Calculation budget: performance and accuracy\n')
print('PR head `33f39cd7`, release binary test-merge `95627d85`. Budget 0 disables the cutoff. Nonzero budgets specify calculation-index distance in decay lengths; larger budgets retain more history. They are not error percentages or CPU-time limits.\n')
repetitions=len([r for r in m if r['name']==a[0]['name']])
print(f'Two query threads; {repetitions} warm repetitions per case. Each dataset has '+str(a[0]['rows'])+' rows and '+str(a[0]['groups'])+' groups, with 1,000 observations spanning 99.9 decay lengths per group. Oracle: 80-digit Decimal arithmetic on exact binary inputs. Raw-value aggregation is a control because this cutoff applies to indexed inputs. Forward/reverse timings include ordering; a full-row-count LIMIT preserves that ordering in the query plan. State merging uses eight interleaved batches.\n')
print('| Dataset | Path | Budget | Median ms | Speedup vs 0 | Max relative error | Max L1 error | Rank accuracy | Top-K recall |\n|---|---|---:|---:|---:|---:|---:|---:|---:|')
for r in sorted(a,key=lambda r:(r['dataset'],r['path'],r['calculation_budget'])):
    ms=latency[r['name']]
    print(f'| {r["dataset"]} | {r["path"]} | {r["calculation_budget"]:g} | {ms:.3f} | {baseline[r["dataset"],r["path"]]/ms:.2f}x | {r["max_relative_error"]:.3g} | {r["max_l1_normalized_error"]:.3g} | {r["positional_accuracy"]:.1%} | {r["top_k_recall"]:.1%} |')
c=json.loads((p/'correctness.json').read_text())
print(f'\n{len(c)} checks; {sum(not x["passed"] for x in c)} failed; {len(m)} timed queries; {sum(not x.get("query_log_record_found",False) for x in m)} missing query-log records.\n')
print('Nonzero budgets intentionally approximate and may depend on input order. Their finite outputs are checked, but measured accuracy is reported rather than treated as guaranteed. Budget-zero and raw-input controls retain the exact-mode numerical gate. L1 error is absolute error divided by the sum of absolute input contributions. Full residuals, plans, query settings, and measurements are retained in the artifact. Previous upstream/flag-off/index/projection comparisons all used budget 0 where supported; upstream has no corresponding budget setting.\n')
