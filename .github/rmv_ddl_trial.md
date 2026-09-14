# RMV / DDL compatibility experiment

Goal: test the net `ddl_workload` feature patch together with asynchronous RMV QUERY admission on a disposable fork branch.

- Upstream PR: https://github.com/ClickHouse/ClickHouse/pull/112808
- Pinned upstream head: debb128c12ba20712d13308cbfb61a63c2dfebbb
- Dependency: https://github.com/ClickHouse/ClickHouse/pull/118419
- Dependency head: 141d322961fa89ce4951ab39dc8fb82edb686c6b
- Dependency merge base: 50df68335ca29327057daafa12798aafbd362de6
- RMV compatibility source: 20246971175667fe1fb889d98be319a609faa6b5
- Combined source commit: 2319bf72e5e9e782768cf1860b45c5b4f6033cfe
- Source branch: agent/rmv-ddl-trial-20260914
- CI branch: agent/rmv-ddl-trial-ci-20260914

The source commit has the upstream head as its only parent. It imports the dependency's net feature delta and the required RMV compatibility edits, with no master history or merge ancestry. CI configuration and runner CPU quota adjustments are separate. To discard the experiment, leave the upstream branch untouched and abandon the trial branches. Do not use git revert, reset, force-push, rebase, or amend.

Validation must run all three complete suites:
- test_refreshable_mv_query_slots, including the four combined DDL-mode cases
- test_refreshable_mv_query_slot_starvation: pool=5, 10 slow RMVs sharing one QUERY slot, 100 fast RMVs every second, continued fast progress and slow-slot handoff
- test_ddl_workload

No local builds, tests, or formatters. Preserve exact source/build-recipe binary caching, compiler-content checks, dependency fingerprints, compile=2/link=1, and integration workers=1. A new branch may miss caches due to GitHub cache scope; do not weaken exact binary keys.

Existing RMV cancellation review findings are not repaired by this dependency experiment. Diagnose failures against this exact source before attributing them to DDL integration. Async MEMORY RESERVATION admission remains out of scope.

Never update the upstream PR or either original feature branch automatically from this experiment. If tests pass, report the result and the remaining upstream readiness issues for a separate promotion decision.
