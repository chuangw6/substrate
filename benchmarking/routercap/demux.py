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

"""Splits an in-cluster run's pod output into the files a local run writes.

The generator runs in a distroless container, so ``kubectl cp`` cannot retrieve
what it writes.  Instead the binary tags every record and writes it to stdout,
and this puts them back:

    kubectl logs -f job/... | demux.py OUTDIR

    stats.jsonl   one flat JSON object per measurement window
    job.log       everything else, which is the binary's own stderr

The tag is still needed with a single record stream because ``kubectl logs``
merges the container's stdout and stderr into one stream.  Writes are flushed
per line, so a run killed halfway leaves every window it had already measured.
"""

import argparse
import json
import os
import sys

STREAM_STATS = "stats"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("outdir", help="Directory to write into; created if absent.")
    ap.add_argument(
        "--quiet",
        action="store_true",
        help="Do not echo a progress line per record to stderr.",
    )
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    stats_path = os.path.join(args.outdir, "stats.jsonl")
    log_path = os.path.join(args.outdir, "job.log")

    stats = open(stats_path, "w", encoding="utf-8")
    log = open(log_path, "w", encoding="utf-8")
    counts = {"stats": 0, "log": 0}

    try:
        for line in sys.stdin:
            line = line.rstrip("\n")
            if not line:
                continue
            rec = None
            if line.startswith("{"):
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    rec = None
            stream = rec.get("stream") if isinstance(rec, dict) else None
            if stream == STREAM_STATS:
                # Unwrapped, so what lands here is byte-identical in shape to
                # what a local --output-dir run writes and nothing downstream
                # can tell the two apart.
                stats.write(json.dumps(rec["record"], separators=(",", ":")) + "\n")
                stats.flush()
                counts["stats"] += 1
                if not args.quiet:
                    progress(rec["record"])
            else:
                log.write(line + "\n")
                log.flush()
                counts["log"] += 1
    finally:
        stats.close()
        log.close()

    print(
        "[demux] {stats} records, {log} log lines -> {d}".format(d=args.outdir, **counts),
        file=sys.stderr,
    )
    # An empty stats.jsonl means the run produced no measurement at all, which
    # a directory holding the file would otherwise not show.
    if counts["stats"] == 0:
        print("[demux] WARNING: the run produced no records", file=sys.stderr)
        return 1
    return 0


def progress(rec: dict) -> None:
    """Echoes one record as a human-readable line."""

    def cores(key: str) -> str:
        """Cores for key, or a dash when this window never sampled it.

        cAdvisor can close a window before a given container has ticked;
        printing 0.00c there would read as "went idle" when it means "nobody
        looked".
        """
        v = rec.get(key)
        return "    -" if v is None else "%5.2f" % v

    trips = ",".join(rec.get("guards") or [])
    print(
        "[{cpu:>3}c rung {rung:>2}{warm}] offered {off:>7.0f}  achieved {ach:>7.0f}  "
        "inflight {inf:>6d}  p50 {p50:>6.1f}ms  p95 {p95:>7.1f}ms  "
        "envoy {envoy}c  sidecar {side}c{guards}".format(
            cpu=rec.get("cpu_limit_cores", 0),
            rung=rec.get("rung", 0),
            warm="w" if rec.get("warmup") else " ",
            off=rec.get("offered_qps", 0.0),
            ach=rec.get("achieved_qps", 0.0),
            inf=int(rec.get("in_flight_max", 0)),
            p50=rec.get("latency_p50_ms", 0.0),
            p95=rec.get("latency_p95_ms", 0.0),
            envoy=cores("envoy_cpu_cores"),
            side=cores("sidecar_cpu_cores"),
            guards="  GUARD:" + trips if trips else "",
        ),
        file=sys.stderr,
    )


if __name__ == "__main__":
    sys.exit(main())
