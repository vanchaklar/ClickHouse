# Exponential time-decay representation backlog

Base: ClickHouse/ClickHouse PR #110934 at `e07a72988b81c8b1a77dd56822d6d25393a3e1f6`.

This file records the agreed representation redesign without changing the active PR implementation yet.

## Anchor

Keep the logical SQL type independent of physical storage:

`ExponentialTimeDecaying(decay_length)`

The decay length belongs to the datatype and must never be stored per row.

A newly materialized value may use one of several physical representations. The default representation is chosen by a setting only when a cast/materialization must choose a new physical form. Existing values preserve the representation they already carry.

Generic `reinterpret` / `reinterpretAs` must not convert or expose these physical representations. Representation changes go through semantic casts.

## Physical representations

### `compact`

8-byte lossy representation:

`shiftOneBitAndSign(unit_timestamp)`

Requirements:

- derive `unit_timestamp = anchor_time + decay_length * log(abs(value_at_anchor))`;
- map finite `Float64` unit timestamps to a monotonic sortable integer key;
- discard one low-order ordering bit;
- encode curve sign in the recovered bit capacity;
- provide a dedicated zero code between negative and positive ranges;
- ordinary integer ordering must match curve ordering;
- document that conversion to `compact` is irreversible and loses one bit of unit-timestamp precision.

### `direct`

16-byte authoritative representation:

`(value_at_anchor Float64, anchor_time Float64)`

Requirements:

- all normal arithmetic uses this payload directly;
- evaluation uses `value_at_anchor * exp((anchor_time - target_time) / decay_length)`;
- avoid reconstructing values through absolute `unit_timestamp`;
- use the stable relative logarithmic expression for comparison/cutoff:
  `(anchor_a - anchor_b) / decay_length + log(abs(value_a)) - log(abs(value_b))`;
- define one canonical zero representation.

### `ordered`

Direct payload plus a compact ordering/index prefix.

Requirements:

- authoritative arithmetic remains the `direct` payload;
- prefix exists only for ordering/index acceleration;
- prefix must never define logical equality by itself;
- ambiguous/near-boundary prefix comparisons fall back to the direct comparator;
- hashing must ignore acceleration-only metadata.

### `precise`

Compensated high-accuracy representation, expected to use high/low components for value and anchor.

Requirements:

- preserve more precision through timestamp differences, re-anchoring, and repeated aggregation;
- aggregate state must retain additional precision so the mode is not merely a wider final container around an already-rounded `Float64`;
- significance decisions use the stable direct formulation, not lossy absolute unit timestamps.

## Datatype and naming

Rename `ExponentialTimeDecayingFloat64` to:

`ExponentialTimeDecaying(decay_length)`

The public logical type name should not expose the current internal floating-point representation.

Representation remains physical metadata, not part of logical decay compatibility.

## Representation selection

Add a setting such as:

`exponential_time_decay_value_representation`

Allowed values:

- `compact`
- `direct`
- `ordered`
- `precise`

Rules:

- consult the setting only when a cast/conversion/materialization must choose a new physical representation;
- explicit destination representation, where an internal conversion path supplies one, wins over the setting;
- reading, comparing, merging, recovering, or passing through an already materialized value must not consult the setting;
- raw aggregate output that creates a new decaying value uses the resolved destination/default representation;
- re-aggregation should preserve an existing representation where possible;
- cross-representation operations remain logically compatible for equal `decay_length`.

## Dedicated column

Introduce a dedicated `IColumn` implementation instead of relying on a raw `ColumnTuple`.

The column must carry its resolved physical representation so `IColumn::compareAt` does not need access to query settings.

Implement representation-aware:

- clone/resize;
- insert/range insert;
- filter;
- permute;
- index;
- replicate;
- scatter;
- comparison;
- permutation;
- hashing;
- arena serialization;
- storage serialization;
- subcolumn handling where appropriate.

Use first-class column implementations such as `ColumnQBit` as architectural precedent.

## Numerical path

Remove absolute `unit_timestamp` as the universal authoritative numeric representation.

For `direct` and `ordered`:

- keep `value_at_anchor` and `anchor_time`;
- decay the older value directly to the selected anchor;
- never use `log -> absolute timestamp -> subtract -> exp` as the normal arithmetic round trip.

For cutoff/index comparisons, use relative differences before mixing logarithmic magnitude with timestamps.

For `compact`, quantization is intentional and documented.

For `precise`, keep compensated components through arithmetic.

## Aggregates

Rework finalized decaying-value input/output:

- raw aggregate state remains anchored weighted-sum arithmetic;
- finalizing to `direct` stores weighted sum + max time directly;
- finalizing to `ordered` creates the ordering prefix after producing the direct payload;
- finalizing to `compact` intentionally quantizes;
- finalizing to `precise` must preserve compensated aggregate information;
- re-aggregation reads the actual physical representation instead of assuming tuple fields.

Replace `calculation_index_time` logic where absolute timestamps magnify precision loss.

## Cross-representation semantics

For equal `decay_length`, all physical representations are the same logical domain.

The following must agree across representations:

- equality;
- relative comparison;
- sorting;
- hashing;
- `IN`;
- `DISTINCT`;
- `GROUP BY`;
- joins;
- common-supertype resolution.

Physical prefixes must not leak into logical hashes.

Different decay lengths remain incompatible.

## Serialization and storage

Persist enough information to reconstruct the physical representation without storing `decay_length` per row.

Prefer representation metadata once per column/substream/part rather than once per value.

Audit Native format / TCP bytes and update the matching specification if the bytes-on-wire change.

Existing PR-era `(sign, signed_unit_time)` data requires an explicit compatibility or migration decision. Do not silently reinterpret it as `direct`.

## `reinterpret` / `reinterpretAs`

Do not allow generic byte reinterpretation to:

- choose a representation;
- convert between representations;
- expose ordered-prefix bytes as the logical value;
- bypass semantic validation.

Representation changes require semantic `CAST`.

If raw physical inspection is ever needed, add a dedicated diagnostic function instead.

## Introspection

Add an explicit representation-inspection helper, e.g.

`exponentialTimeDecayingRepresentation(value)`

It should return `compact`, `direct`, `ordered`, or `precise`.

## Server-level warning

Emit a deduplicated `WARNING` once per persisted table/column per server lifetime when storage is about to contain, or is observed to contain, multiple physical representations for the same logical `ExponentialTimeDecaying` column.

The warning must:

- list observed representations;
- include the current default representation;
- explain that logical values remain compatible;
- warn that storage size, numerical characteristics, and ordering/index performance may differ between parts;
- state that changing the default setting does not rewrite existing data;
- recommend rematerializing the column/table under a consistent representation if uniform storage is desired;
- not fire for temporary/query-local columns;
- not fire merely because the setting changed.

## User guidance

Document the intended tradeoffs:

- `compact`: 8-byte, directly sortable, lossy;
- `direct`: 16-byte accurate baseline;
- `ordered`: direct arithmetic plus ordering/index acceleration;
- `precise`: largest representation, highest numerical accuracy.

Recommend consistent representation settings across writers to the same persisted table unless heterogeneous storage is intentional.

Changing the default affects newly materialized values only.

Provide a migration/rematerialization workflow using semantic casts and `INSERT ... SELECT` / table rewrite.

## Conversion machinery audit

Audit and update as required:

- `FunctionsConversion`;
- `convertColumnToType`;
- `tupleElement`;
- `in`;
- common-supertype logic;
- `Variant` / `Dynamic`;
- nested arrays/tuples/maps;
- JSON extraction;
- LowCardinality/Sparse;
- MergeTree sorting and primary-key safety;
- set/hash-table paths;
- binary type encoding;
- format integrations.

Avoid restoring broad generic hooks that were only needed by the earlier custom-tuple implementation.

## Tests

Do not modify old tests without explicit approval.

Add new dedicated tests for:

- representation selection at materialization/cast boundaries;
- cross-mode equality/hash/order/`IN`/`GROUP BY`;
- compact quantization bounds and irreversibility;
- direct accuracy with large absolute timestamps and very short decay lengths;
- ordered-prefix fast path and exact fallback;
- precise improvement over direct;
- cross-mode arithmetic and aggregation;
- mixed persisted parts and server warning;
- restart / ATTACH / reconstruction;
- absence of a `reinterpretAs` bypass.

## Accuracy oracle

Add high-precision reference coverage for:

- timestamps around `1e9` and `1e12`;
- negative timestamps;
- tiny decay lengths;
- very large/small magnitudes;
- signed cancellation;
- cutoff decisions close to the boundary.

Quantify the expected loss of `compact` and the improvement of `precise`.

## Benchmarks

Measure each representation for:

- bytes/value;
- cast/materialization cost;
- `valueAt`;
- addition;
- aggregate finalization/re-aggregation;
- sorting;
- comparisons;
- `IN` / hashing;
- MergeTree ordering/index workloads.

Report accuracy alongside speed for `compact` and `precise`.

## Implementation sequence

1. Rename the logical datatype.
2. Introduce the dedicated column abstraction.
3. Implement `direct` and the stable comparator/cutoff formula.
4. Add the representation-selection setting and materialization rules.
5. Implement `compact`.
6. Implement `ordered` as prefix + direct payload.
7. Implement `precise` and compensated aggregate state.
8. Complete cross-representation hashing/comparison/conversion.
9. Add persistence metadata, warning, introspection, migration guidance.
10. Run correctness oracle and benchmarks before changing defaults.
