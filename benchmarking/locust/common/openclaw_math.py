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

"""Pure scheduling/aggregation logic for the OpenClaw density test.

Shared by tests/openclaw_cycle.py (the locust user class) and
common/openclaw_ladder.py (the band state machine). Stdlib only, so the
unit tests in benchmarking/openclaw-density/tests/ run without the locust
image's dependencies.
"""


def parse_points(points: str, workers: int) -> list[int]:
    """Resolve --points to actor counts. 'Nx' entries are multiples of
    --workers; a multiplier with no --workers is an error, because a ladder
    silently anchored on 0 measures nothing."""
    counts = []
    for raw in points.split(","):
        item = raw.strip().lower()
        if not item:
            continue
        if item.endswith("x"):
            if workers <= 0:
                raise ValueError(
                    f"--points entry {item!r} is a worker multiplier but "
                    f"--workers is not set; pass --workers <measured X>"
                )
            counts.append(int(float(item[:-1]) * workers))
        else:
            counts.append(int(item))
    if not counts:
        raise ValueError("--points has no entries")
    return counts


# Clearance between the staggered window and the cycle's edges, beyond the
# active dwell itself: room for the herd's checkpoint tail at the front,
# and for the last staggered actor's restore + suspend before the next
# boundary at the back.
STAGGER_EDGE_BUFFER_S = 120.0


def cohort_and_offset(
    rank: int,
    total: int,
    herd_fraction: float,
    cycle_seconds: float,
    dwell_seconds: float,
    window: str = "remaining",
) -> tuple[str, float]:
    """Cohort and wake offset for the actor at `rank` among `total` alive.

    Rank-based rather than fixed at spawn so the split holds for whatever
    population a band has: ranks re-pack after every population change,
    so every cycle keeps exactly herd_fraction herd actors and an evenly
    spaced remainder.

    `window` selects where the staggered wakes land:

      remaining  the capacity model's prose ("the remaining time" of the
                 cycle, staggered to minimize peak usage): spread evenly
                 over [dwell + buffer, cycle - dwell - buffer], after the
                 herd's active phase and clear of the next boundary. The
                 herd always finds every worker free.
      full       the model's literal formula (uncoordinated arrivals):
                 spread over the whole cycle, so some staggered wakes land
                 inside the herd's window by chance. Midpoints keep pod 0
                 off the exact boundary.
    """
    if herd_fraction > 0:
        stride = max(1, round(1.0 / herd_fraction))
        if rank % stride == 0:
            return "herd", 0.0
        n_herd = (total + stride - 1) // stride
        j = rank - (rank // stride + 1)
    else:
        n_herd = 0
        j = rank
    n_staggered = max(1, total - n_herd)
    if window == "full":
        start, end = 0.0, cycle_seconds
    else:
        start = dwell_seconds + STAGGER_EDGE_BUFFER_S
        end = cycle_seconds - dwell_seconds - STAGGER_EDGE_BUFFER_S
        if end <= start:  # degenerate short cycles: fall back to full
            start, end = 0.0, cycle_seconds
    return "staggered", start + ((j + 0.5) / n_staggered) * (end - start)


def percentiles(values: list[float]) -> dict:
    """Summary stats for summary.json / model_params.json."""
    if not values:
        return {}
    ordered = sorted(values)

    def pct(p: float) -> float:
        i = min(len(ordered) - 1, int(p * len(ordered)))
        return round(ordered[i], 4)

    return {
        "count": len(ordered),
        "p50_s": pct(0.50),
        "p90_s": pct(0.90),
        "p99_s": pct(0.99),
        "max_s": round(ordered[-1], 4),
    }


def band_verdict(
    records: list[dict],
    band_id: int,
    calm_start_seconds: float = 0.0,
    staggered_p90_max_seconds: float | None = None,
    staggered_suspend_p90_max_seconds: float | None = None,
    herd_resume_p90_max_seconds: float | None = None,
) -> dict:
    """Pass/fail for one measured band from the test's own op records.

    Three rules; a band passes only when every enabled rule passes.

      A  on time: zero cycle overruns and zero failed operations.
      B  wake-ups stay near idle speed: p90 of successful staggered resumes
         whose slot offset is >= calm_start_seconds (i.e. outside the herd
         storm) is <= staggered_p90_max_seconds. Enabled when a bar is given.
      C  suspends stay fast: p90 of the same actors' suspends (staggered,
         slot offset >= calm_start_seconds) is <= staggered_suspend_p90_max_seconds.
         Herd suspends are excluded: they land in one wave 3 minutes after
         the herd's resumes and would dominate the tail. Enabled when a bar
         is given.
      D  the herd drains fast: p90 of successful herd resumes is
         <= herd_resume_p90_max_seconds. Every actor in a herd waits about
         the same time, so p90 and p50 are close; p90 is the SLO framing. Enabled when a
         bar is given.

    Rules B and C exist because rule A alone cannot show where the machines
    stop keeping up: a band can be on time while every restore in it is
    ten times slower than on an idle node. A rule with no samples fails,
    since a band that produced nothing to judge proved nothing.

    Warm-up records (phase != "band") never count: they happen outside
    any measured cycle.
    """
    misses = failures = ops = 0
    calm_resumes: list[float] = []
    suspends: list[float] = []
    herd_resumes: list[float] = []
    for r in records:
        if r.get("band_id") != band_id or r.get("phase") != "band":
            continue
        if r["op"] == "cycle_overrun":
            misses += 1
            continue
        ops += 1
        if not r["ok"]:
            failures += 1
            continue
        if r["op"] == "resume_herd":
            herd_resumes.append(r["elapsed_s"])
        elif r["op"] == "resume_staggered" and r.get("offset_s", 0.0) >= calm_start_seconds:
            calm_resumes.append(r["elapsed_s"])
        elif (r["op"] == "suspend" and r.get("cohort") == "staggered"
              and r.get("offset_s", 0.0) >= calm_start_seconds):
            suspends.append(r["elapsed_s"])

    rules = {
        "A": {
            "name": "on_time",
            "value": {"missed_slots": misses, "failed_ops": failures},
            "passed": misses == 0 and failures == 0 and ops > 0,
        },
    }
    if staggered_p90_max_seconds is not None:
        p = percentiles(calm_resumes)
        rules["B"] = {
            "name": "calm_staggered_resume_p90",
            "value": p.get("p90_s"), "samples": len(calm_resumes),
            "bar": staggered_p90_max_seconds,
            "passed": bool(p) and p["p90_s"] <= staggered_p90_max_seconds,
        }
    if staggered_suspend_p90_max_seconds is not None:
        p = percentiles(suspends)
        rules["C"] = {
            "name": "calm_staggered_suspend_p90",
            "value": p.get("p90_s"), "samples": len(suspends),
            "bar": staggered_suspend_p90_max_seconds,
            "passed": bool(p) and p["p90_s"] <= staggered_suspend_p90_max_seconds,
        }
    if herd_resume_p90_max_seconds is not None:
        p = percentiles(herd_resumes)
        rules["D"] = {
            "name": "herd_resume_p90",
            "value": p.get("p90_s"), "samples": len(herd_resumes),
            "bar": herd_resume_p90_max_seconds,
            "passed": bool(p) and p["p90_s"] <= herd_resume_p90_max_seconds,
        }
    failed = [k for k, v in rules.items() if not v["passed"]]
    return {
        "passed": not failed,
        "missed_slots": misses,
        "failed_ops": failures,
        "ops": ops,
        "rules": rules,
        "failed_rules": failed,
    }


def next_probe(
    last_pass: int | None, first_fail: int | None, resolution: int
) -> int | None:
    """Midpoint of the (pass, fail) bracket, or None when there is no
    bracket or it is already within `resolution` actors."""
    if last_pass is None or first_fail is None:
        return None
    if first_fail - last_pass <= resolution:
        return None
    return (last_pass + first_fail) // 2
