#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Reduces one run's stats.jsonl to a per-rung summary on stdout.

    summarize.py stats.jsonl > summary.json

A convenience for a human opening a run directory.  Nothing downstream reads
what this writes: stats.jsonl is the run's output, and the tools that plot it
live outside this repo.  The point of having it here is that "sustainable QPS"
means one specific thing (below), and everyone reading a run should get the
same number for it.

Sustainable QPS is the highest rung where, over that rung's measured windows:

  * achieved QPS is at least 99% of the rung's nominal rate,
  * successful QPS is at least 99% of that rate, and
  * no window's p50 latency exceeds 100ms.

The denominator is the rung's nominal rate and not its offered QPS, which
would be close to circular: the generator very nearly always achieves what it
managed to offer, so an offered-QPS denominator passes every rung including
ones where the pacer never reached the target at all.  A rung cut short --- the
run ending mid-rung, so its last window carries a fraction of the traffic ---
then reads as sustained on a rate it never actually served.

Every rung is tested, not just rungs below the first failure: a rung can fail
on a transient and the ladder recover above it, and the highest rung that holds
is the honest answer.  The reported value is the rung's nominal rate, not its
measured throughput, because that is the rate the router was asked to sustain.
"""

import argparse
import json
import sys

# The two thresholds that define "sustained".  99% rather than 100% because a
# single dropped request in a 45s rung is not a capacity statement; 100ms
# because past it the queue, not the router, is what the number describes.
SUSTAIN_RATIO = 0.99
P50_CEILING_MS = 100.0

# Above these the hop breakdown should not be read closely: the four
# instruments disagreed about how many requests they saw, or Envoy's
# whole-millisecond histogram resolution is a large share of the total.
SPREAD_LIMIT = 0.1
RESOLUTION_LIMIT = 0.05

SPAN_KEYS = (
    "span_before_envoy_ms",
    "span_envoy_ms",
    "span_sidecar_ms",
    "span_worker_ms",
    "span_resume_ms",
    "span_total_ms",
)


def load(path: str) -> list:
    """Reads stats.jsonl, skipping blank lines."""
    out = []
    with open(path, encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError as err:
                raise SystemExit("%s:%d: %s" % (path, n, err))
    return out


def mean(values: list) -> float:
    return sum(values) / len(values) if values else 0.0


def present(records: list, key: str) -> list:
    """Values of key across records that have it.

    An absent key means cAdvisor never reported that container in that window.
    Averaging over the windows that saw it, rather than over all of them, is
    what keeps a scrape gap from reading as an idle container.
    """
    return [r[key] for r in records if r.get(key) is not None]


def weighted(records: list, key: str) -> float:
    """Request-weighted mean of key.

    The spans are per-request means over their window, so a plain average would
    weight a window that carried a tenth of the requests as heavily as a full
    one.  The weight is the window's request count, recovered as achieved QPS
    times the window's length.
    """
    num = 0.0
    den = 0.0
    for r in records:
        v = r.get(key)
        if v is None:
            continue
        w = r.get("achieved_qps", 0.0) * r.get("window_seconds", 0.0)
        if w <= 0:
            continue
        num += v * w
        den += w
    return num / den if den else 0.0


def summarize_rung(records: list) -> dict:
    """Reduces one rung's measured windows to a single row."""
    out = {
        "rung": records[0].get("rung"),
        "rung_qps": records[0].get("rung_qps"),
        "windows": len(records),
        "offered_qps": mean([r.get("offered_qps", 0.0) for r in records]),
        "achieved_qps": mean([r.get("achieved_qps", 0.0) for r in records]),
        "success_qps": mean([r.get("success_qps", 0.0) for r in records]),
        # Maxima, not means: a percentile that spiked in one window is the
        # thing worth knowing, and averaging it away is how a stall hides.
        "latency_p50_ms": max(r.get("latency_p50_ms", 0.0) for r in records),
        "latency_p95_ms": max(r.get("latency_p95_ms", 0.0) for r in records),
        "dispatch_lag_p95_ms": max(r.get("dispatch_lag_p95_ms", 0.0) for r in records),
        "in_flight_max": max(int(r.get("in_flight_max", 0)) for r in records),
    }
    for key in ("envoy_cpu_cores", "sidecar_cpu_cores",
                "envoy_memory_bytes", "sidecar_memory_bytes"):
        values = present(records, key)
        if values:
            out[key] = mean(values)

    for key in SPAN_KEYS:
        if present(records, key):
            out[key] = weighted(records, key)
    # The worst window's markers, so the rung's breakdown is judged by its
    # least trustworthy window rather than by an average that hides it.
    for key in ("span_count_spread", "span_resolution_share"):
        values = present(records, key)
        if values:
            out[key] = max(values)
    if out.get("span_count_spread", 0.0) > SPREAD_LIMIT or \
            out.get("span_resolution_share", 0.0) > RESOLUTION_LIMIT:
        out["span_caveat"] = ("the four instruments disagreed about request counts, or "
                              "millisecond rounding is a large share of the total; "
                              "read the hop breakdown as an ordering, not as figures")

    trips = sorted({g for r in records for g in (r.get("guards") or [])})
    if trips:
        out["guards"] = trips
    if any(r.get("guard_fatal") for r in records):
        out["guard_fatal"] = True
    return out


def sustained(rungs: list) -> dict:
    """Highest rung meeting the sustain criterion, with why it was the highest."""
    best = None
    for row in rungs:
        target = row["rung_qps"]
        if target <= 0:
            continue
        ok = (row["achieved_qps"] >= SUSTAIN_RATIO * target
              and row["success_qps"] >= SUSTAIN_RATIO * target
              and row["latency_p50_ms"] <= P50_CEILING_MS)
        if ok and (best is None or row["rung_qps"] > best["rung_qps"]):
            best = row
    if best is None:
        return {"qps": 0.0, "rung": None,
                "note": "no rung held the criterion, not even the first"}
    return {
        "qps": best["rung_qps"],
        "rung": best["rung"],
        "achieved_qps": best["achieved_qps"],
        "latency_p50_ms": best["latency_p50_ms"],
        "latency_p95_ms": best["latency_p95_ms"],
        "envoy_cpu_cores": best.get("envoy_cpu_cores"),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("stats", help="Path to a run's stats.jsonl.")
    args = ap.parse_args()

    records = load(args.stats)
    if not records:
        raise SystemExit("%s holds no records" % args.stats)

    # Warmup windows are a rung's settling, written to the file so a reader can
    # see it and excluded here so it does not drag the rung's own numbers.
    measured = [r for r in records if not r.get("warmup") and r.get("rung", -1) >= 0]
    if not measured:
        raise SystemExit("%s holds no measured windows: every record is warmup or "
                         "between rungs" % args.stats)

    by_rung = {}
    for r in measured:
        by_rung.setdefault(r["rung"], []).append(r)
    rungs = [summarize_rung(by_rung[k]) for k in sorted(by_rung)]

    first = records[0]
    summary = {
        "test_name": first.get("test_name"),
        "tag": first.get("tag"),
        "cpu_limit_cores": first.get("cpu_limit_cores"),
        "windows": len(records),
        "measured_windows": len(measured),
        "started": first.get("timestamp"),
        "ended": records[-1].get("timestamp"),
        "sustainable_qps": sustained(rungs),
        "criterion": {
            "ratio": SUSTAIN_RATIO,
            "p50_ceiling_ms": P50_CEILING_MS,
        },
        "rungs": rungs,
    }
    fatal = [r for r in records if r.get("guard_fatal")]
    if fatal:
        # The ladder stopped early. The windows below the trip still stand, so
        # this is a caveat on the top of the curve, not on the whole run.
        summary["rig_limited"] = {
            "guards": sorted({g for r in fatal for g in (r.get("guards") or [])}),
            "rung": fatal[0].get("rung"),
            "note": "the rig ran out before the router did; rungs above this one were never run",
        }

    json.dump(summary, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
