#!/usr/bin/env python3
"""Focused native ExpoDecay checks; no private application code or services."""
import argparse
import json
import math
from pathlib import Path
import subprocess
import sys

SAMPLES = [(1, -8.0, 0.0), (2, 3.0, 10.0), (3, 6.0, 2.0), (4, -1.0, 4.0)]
VALUES = "VALUES('id UInt8, value Float64, time Float64', " + ", ".join(
    f"({index}, {value}, {time})" for index, value, time in SAMPLES
) + ")"
EXPECTED = math.fsum(value * math.exp((time - 10) / 10) for _, value, time in SAMPLES)
WEIGHT = math.fsum(math.exp((time - 10) / 10) for _, _, time in SAMPLES)


def type_literal(width, decay=10):
    return "'" + f"ExponentialTimeDecaying{width}({decay}, 'compact')".replace("'", "\\'") + "'"


def close(actual, expected, tolerance=1e-10):
    actual = float(actual)
    if not math.isfinite(actual) or not math.isclose(
        actual, expected, rel_tol=tolerance, abs_tol=tolerance
    ):
        raise AssertionError(f"{actual!r} differs from {expected!r}")


def require(condition, message):
    if not condition:
        raise AssertionError(message)


class Smoke:
    def __init__(self, binary, output):
        self.binary = str(binary.resolve())
        self.output = output.resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        self.results = []

    def query(self, name, sql, *, native_output=None, native_input=None):
        self.output.joinpath(name + ".sql").write_text(sql.strip() + "\n")
        state = self.output / "local-data" / name
        state.mkdir(parents=True, exist_ok=True)
        command = [
            self.binary, "local", "--path", str(state),
            "--allow_experimental_time_decay_aggregate_functions=1",
            "--max_threads=2", "--max_block_size=3",
            "--format", "Native" if native_output else "JSONEachRow",
            "--query", sql,
        ]
        if native_input:
            command += ["--input-format", "Native", "--file", str(native_input)]
        with self.output.joinpath(name + ".log").open("wb") as log:
            result = subprocess.run(
                command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=log, timeout=120
            )
            if native_output:
                native_output.write_bytes(result.stdout)
                log.write(f"\nNative bytes: {len(result.stdout)}\n".encode())
            else:
                log.write(b"\nSQL output:\n" + result.stdout)
        if result.returncode:
            raise RuntimeError(f"{name}: ClickHouse returned {result.returncode}; see {name}.log")
        if native_output:
            return None
        return [json.loads(line) for line in result.stdout.decode().splitlines() if line.strip()]

    def case(self, name, operation):
        try:
            detail = operation()
            self.results.append({"name": name, "status": "passed", "detail": detail})
        except (AssertionError, RuntimeError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
            self.results.append({"name": name, "status": "failed", "error": str(error)})

    def finish(self):
        failed = [item["name"] for item in self.results if item["status"] != "passed"]
        summary = {
            "status": "failed" if failed else "passed",
            "cases": self.results,
            "scope": "Native SQL aggregation, compact values, Native state serialization, merge, cancellation, ordering, and threshold diagnostics",
            "not_run": ["full stateless suite", "private application training", "phone device validation"],
            "known_unresolved": ["04613 overflow"],
        }
        self.output.joinpath("smoke-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(f"{len(self.results) - len(failed)}/{len(self.results)} focused smoke cases passed")
        return 1 if failed else 0


def numeric_aggregate(smoke, order):
    row = smoke.query(
        f"numeric_aggregate_{order}",
        f"""SELECT exponentialTimeDecayedSum(10)(value, time) AS result,
                   exponentialTimeDecayedAvg(10)(value, time) AS average,
                   exponentialTimeDecayedCount(10)(time) AS weight
            FROM (SELECT * FROM {VALUES} ORDER BY id {order.upper()})""",
    )[0]
    close(row["result"], EXPECTED, 1e-12)
    close(row["average"], EXPECTED / WEIGHT, 1e-12)
    close(row["weight"], WEIGHT, 1e-12)
    return row


def compact_aggregate(smoke, width, order):
    row = smoke.query(
        f"compact{width}_aggregate_{order}",
        f"""WITH exponentialTimeDecayedSum(curve) AS result
            SELECT exponentialTimeDecayingValueAt(result, 10.) AS value,
                   exponentialTimeDecayingDecayLength(result) AS decay_length,
                   toTypeName(result) AS type
            FROM
            (
                SELECT CAST((value, time), {type_literal(width)}) AS curve
                FROM {VALUES} ORDER BY id {order.upper()}
            )""",
    )[0]
    close(row["value"], EXPECTED)
    require(float(row["decay_length"]) == 10, "Decay length changed")
    require(f"ExponentialTimeDecaying{width}" in row["type"], "Compact width changed")
    require("direct" not in row["type"], "Result unexpectedly became direct")
    return row


def compact_state_roundtrip(smoke, width):
    native = smoke.output / f"compact{width}-states.native"
    smoke.query(
        f"compact{width}_state_write",
        f"""SELECT id % 2 AS batch,
                   exponentialTimeDecayedSumState(
                       CAST((value, time), {type_literal(width)})
                   ) AS state
            FROM {VALUES} GROUP BY batch ORDER BY batch""",
        native_output=native,
    )
    row = smoke.query(
        f"compact{width}_state_merge",
        """WITH exponentialTimeDecayedSumMerge(state) AS result
           SELECT exponentialTimeDecayingValueAt(result, 10.) AS value,
                  toTypeName(result) AS type FROM table""",
        native_input=native,
    )[0]
    close(row["value"], EXPECTED)
    require(f"ExponentialTimeDecaying{width}" in row["type"], "Merged compact width changed")
    return row


def compact_cancellation(smoke, width):
    row = smoke.query(
        f"compact{width}_cancellation",
        f"""WITH
                CAST((4., 3.), {type_literal(width)}) AS positive,
                CAST((-4., 3.), {type_literal(width)}) AS negative,
                positive + negative AS zero
            SELECT exponentialTimeDecayingValueAt(zero, 10.) AS added,
                   exponentialTimeDecayingValueAt(
                       exponentialTimeDecayedSum(curve), 10.) AS aggregated
            FROM
            (
                SELECT CAST((v, 3.), {type_literal(width)}) AS curve
                FROM VALUES('v Float64', (4.), (-4.))
            )""",
    )[0]
    close(row["added"], 0, 1e-12)
    close(row["aggregated"], 0, 1e-12)
    return row


def compact_order(smoke, width):
    rows = smoke.query(
        f"compact{width}_order",
        f"""SELECT id, exponentialTimeDecayingValueAt(curve, 45.) AS value
            FROM
            (
                SELECT id, CAST((value, time), {type_literal(width)}) AS curve
                FROM VALUES('id UInt8, value Float64, time Float64',
                            (1, -100, 0), (2, -2, 45), (3, 0, -1000),
                            (4, 100, 0), (5, 2, 45))
            )
            ORDER BY curve, id""",
    )
    require([int(row["id"]) for row in rows] == [2, 1, 3, 4, 5], "Curve order mismatch")
    values = [float(row["value"]) for row in rows]
    require(values == sorted(values), "Key order differs from evaluated value order")
    return rows


def compact_left_join(smoke, width, use_nulls):
    rows = smoke.query(
        f"compact{width}_left_join_nulls{use_nulls}",
        f"""WITH toDateTime64('2030-01-01 00:00:01', 6, 'UTC') AS at
            SELECT l.id,
                   ifNull(exponentialTimeDecayingValueAt(r.activity, at), 0.) AS value
            FROM (SELECT number + toUInt64(1) AS id FROM numbers(2)) AS l
            LEFT JOIN
            (
                SELECT toUInt64(2) AS id,
                       CAST((2., at), {type_literal(width, 3)}) AS activity
            ) AS r ON l.id = r.id
            ORDER BY l.id
            SETTINGS join_use_nulls = {use_nulls}""",
    )
    require([int(row["id"]) for row in rows] == [1, 2], "Mixed left join rows changed")
    close(rows[0]["value"], 0, 1e-12)
    close(rows[1]["value"], 2, 1e-6 if width == 64 else 1e-12)
    return rows


def threshold_diagnostic(smoke, width):
    row = smoke.query(
        f"compact{width}_threshold",
        f"""WITH
                toDateTime64('2030-01-01 00:00:00.000003', 6, 'UTC') AS event_time,
                toFloat64(toUnixTimestamp64Micro(event_time)) / 1000000 AS time,
                CAST((1., event_time), {type_literal(width, 3)}) AS curve,
                exponentialTimeDecayingValueAt(curve, time) AS value
            SELECT time, value, value >= 1 AS active,
                   exponentialTimeDecayingUnitTime(curve) AS unit_time,
                   toTypeName(curve) AS type""",
    )[0]
    require(math.isfinite(float(row["value"])), "Threshold evaluation is not finite")
    if width == 128:
        require(int(row["active"]) == 1, "Compact128 lost the unit-value boundary")
        close(row["value"], 1, 1e-12)
    # Compact64 classification is evidence only: its quantized key need not retain this boundary.
    return row


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    smoke = Smoke(args.binary, args.output)
    for order in ("asc", "desc"):
        smoke.case(f"numeric_aggregate_{order}", lambda order=order: numeric_aggregate(smoke, order))
    for width in (64, 128):
        for order in ("asc", "desc"):
            smoke.case(f"compact{width}_aggregate_{order}",
                       lambda width=width, order=order: compact_aggregate(smoke, width, order))
        smoke.case(f"compact{width}_state_roundtrip",
                   lambda width=width: compact_state_roundtrip(smoke, width))
        smoke.case(f"compact{width}_cancellation",
                   lambda width=width: compact_cancellation(smoke, width))
        smoke.case(f"compact{width}_order", lambda width=width: compact_order(smoke, width))
        for use_nulls in (0, 1):
            smoke.case(f"compact{width}_left_join_nulls{use_nulls}",
                       lambda width=width, use_nulls=use_nulls:
                           compact_left_join(smoke, width, use_nulls))
        smoke.case(f"compact{width}_threshold",
                   lambda width=width: threshold_diagnostic(smoke, width))
    return smoke.finish()


if __name__ == "__main__":
    sys.exit(main())
