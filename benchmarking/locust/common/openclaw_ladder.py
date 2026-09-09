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

"""Band state machine for the OpenClaw density test.

One process runs the shape, the users, and this controller, so a module
singleton (`ladder`) is the coordination point:

  * shapes/openclaw_density.py calls `ladder.tick(...)` and returns the
    user target it hands back;
  * tests/openclaw_cycle.py reads `ladder.phase`, `ladder.epoch`,
    `ladder.band_id` and `ladder.label_n` to decide when to wake.

Each band runs in a clean state:

    WARMUP -> BAND -> PAUSE -> BAND -> ... -> DONE

A PAUSE has two stages. First the *drain*: the population is unchanged
until the pool checker confirms every actor in the atespace is
SUSPENDED — every routine of the finished band is complete, every worker
is free. Only then is the band's verdict computed (so a late actor's
overrun still counts) and the next band's size decided; growing or
shrinking the population is safe at that point because nobody is
mid-operation. Then the *warm* stage: wait until the alive user count
equals the new target, every alive user has finished its one-time
warm-up boot, a settle period has passed since the last warm-up
operation (physical work behind a suspend — snapshot upload, sandbox
teardown, disk writeback — outlives the API state), and the pool is clean
again (all actors suspended, all workers unassigned) — then start the
band's clock. WARMUP (before band 1) is the warm stage alone. Node CPU at
each band's start is recorded so a band that started on busy machines is
visible in the summary.

Everything external is injected (clock, pool checker, verdict, logger) so
the machine is unit-testable without locust or a cluster.
"""

import logging
import time
from dataclasses import dataclass, field

from common.openclaw_math import next_probe, parse_points

logger = logging.getLogger(__name__)

WARMUP, BAND, PAUSE, DONE = "warmup", "band", "pause", "done"

# Users spawn fast: they self-schedule onto cycle boundaries, so the ramp
# only needs to finish well inside the pause.
SPAWN_RATE = 50.0

# How often PAUSE re-runs the (comparatively expensive) ListActors check.
POOL_CHECK_INTERVAL_S = 10.0


@dataclass
class LadderConfig:
    points: list[int]
    cycle_seconds: float
    cycles_per_point: int
    lead_seconds: float
    barrier_max_seconds: float
    bisect: bool
    bisect_resolution: int
    settle_seconds: float = 120.0

    @classmethod
    def from_options(cls, opts) -> "LadderConfig":
        counts = parse_points(opts.points, opts.workers)
        workers = opts.workers or max(counts)
        resolution = int(opts.bisect_resolution) if opts.bisect_resolution else max(1, workers // 4)
        return cls(
            points=counts,
            cycle_seconds=float(opts.cycle_seconds),
            cycles_per_point=int(opts.cycles_per_point),
            lead_seconds=min(float(opts.first_boundary_lead_seconds), float(opts.cycle_seconds)),
            barrier_max_seconds=float(opts.barrier_max_seconds),
            bisect=str(opts.bisect).lower() == "true",
            bisect_resolution=resolution,
            settle_seconds=float(getattr(opts, "warmup_settle_seconds", 120.0)),
        )


@dataclass
class BandResult:
    band_id: int
    n: int
    passed: bool
    missed_slots: int
    failed_ops: int
    ops: int
    suspect: bool
    started_at: float
    ended_at: float
    kind: str  # "ladder" or "probe"
    cpu_at_start: list[float] = field(default_factory=list)  # cores per SUT node
    rules: dict = field(default_factory=dict)        # band_verdict()["rules"]
    failed_rules: list[str] = field(default_factory=list)


@dataclass
class Ladder:
    cfg: LadderConfig | None = None
    phase: str = WARMUP
    target: int = 0            # users the shape currently asks locust for
    label_n: int = 0           # n stamped on ops (the band being run/prepared)
    band_id: int = 0           # sequence number of the band being run/prepared
    epoch: float | None = None # current band's chime anchor (None outside BAND)
    band_end: float | None = None
    queue: list[int] = field(default_factory=list)
    verdict_pending: bool = False  # PAUSE drain stage: verdict not yet taken
    pause_started: float = 0.0
    last_pool_check: float = 0.0
    suspect_next: bool = False
    results: list[BandResult] = field(default_factory=list)
    last_pass: int | None = None
    first_fail: int | None = None
    knee_note: str = ""
    band_started_at: float = 0.0
    band_cpu_at_start: list[float] = field(default_factory=list)

    # ------------------------------------------------------------------
    def configure(self, cfg: LadderConfig, now: float) -> None:
        self.cfg = cfg
        self.queue = list(cfg.points)
        self.target = self.queue.pop(0)
        self.label_n = self.target
        self.band_id = 1
        self.phase = WARMUP
        self.pause_started = now
        logger.info("Ladder: points=%s bisect=%s resolution=%d",
                    cfg.points, cfg.bisect, cfg.bisect_resolution)

    @property
    def in_band(self) -> bool:
        return self.phase == BAND and self.epoch is not None

    def cycle_index(self, now: float) -> int:
        """1-based index of the next chime after `now` within the current
        band, or a value > cycles_per_point once the band's chimes are
        exhausted."""
        assert self.epoch is not None and self.cfg is not None
        return int((now - self.epoch) // self.cfg.cycle_seconds) + 1

    # ------------------------------------------------------------------
    def tick(
        self,
        now: float,
        alive: int,
        all_warmed: bool,
        pool_clean,          # callable() -> bool | None (None = check failed)
        verdict,             # callable(band_id) -> dict (see band_verdict)
        last_warm_activity: float | None = None,  # time of the last warm-up op
        node_cpu=lambda: [],  # callable() -> cores in use per SUT node
    ) -> tuple[int, float] | None:
        """Advance the machine. Returns (users, spawn_rate) for the shape,
        or None when the run is complete."""
        assert self.cfg is not None
        cfg = self.cfg

        if self.phase == DONE:
            return None

        if self.phase == BAND:
            assert self.band_end is not None
            if now < self.band_end:
                return (self.target, SPAWN_RATE)
            self._end_band(now)
            return (self.target, SPAWN_RATE)

        # PAUSE, drain stage: population unchanged until every routine of
        # the finished band is complete (pool clean), then verdict + next.
        if self.verdict_pending:
            drained = self._pool_is_clean(now, pool_clean)
            if not drained and now - self.pause_started > cfg.barrier_max_seconds:
                logger.error("Drain exceeded %.0fs after band %d; taking the verdict anyway",
                             cfg.barrier_max_seconds, self.band_id)
                drained = True
            if drained:
                self._finish_band(now, verdict)
            return None if self.phase == DONE else (self.target, SPAWN_RATE)

        # WARMUP, or PAUSE warm stage --------------------------------------
        waited = now - self.pause_started
        settled = (last_warm_activity is None
                   or now - last_warm_activity >= cfg.settle_seconds)
        ready = alive == self.target and all_warmed and settled
        if ready and self._pool_is_clean(now, pool_clean):
            self.band_cpu_at_start = [round(c, 1) for c in node_cpu()]
            self._start_band(now)
        elif waited > cfg.barrier_max_seconds:
            logger.error(
                "Pause exceeded %.0fs (alive=%d target=%d warmed=%s); starting "
                "band %d anyway and marking it SUSPECT",
                cfg.barrier_max_seconds, alive, self.target, all_warmed, self.band_id,
            )
            self.suspect_next = True
            self.band_cpu_at_start = [round(c, 1) for c in node_cpu()]
            self._start_band(now)
        return (self.target, SPAWN_RATE)

    # ------------------------------------------------------------------
    def _pool_is_clean(self, now: float, pool_clean) -> bool:
        if now - self.last_pool_check < POOL_CHECK_INTERVAL_S:
            return False
        self.last_pool_check = now
        result = pool_clean()
        return bool(result)

    def _start_band(self, now: float) -> None:
        cfg = self.cfg
        assert cfg is not None
        self.epoch = now + cfg.lead_seconds - cfg.cycle_seconds
        # Last chime + one full cycle: the deadline window of the last
        # routines. Stragglers past this point are absorbed by the pause.
        self.band_end = self.epoch + (cfg.cycles_per_point + 1) * cfg.cycle_seconds
        self.band_started_at = now
        self.phase = BAND
        logger.info(
            "Band %d start: n=%d, first chime in %.0fs, %d cycle(s), ends in %.0fs, "
            "node cpu at start=%s",
            self.band_id, self.label_n, cfg.lead_seconds, cfg.cycles_per_point,
            self.band_end - now, self.band_cpu_at_start,
        )

    def _end_band(self, now: float) -> None:
        """The band's clock is over; enter the pause's drain stage. The
        verdict waits until the pool is clean so late routines count."""
        self.epoch = None
        self.band_end = None
        self.phase = PAUSE
        self.verdict_pending = True
        self.pause_started = now
        logger.info("Band %d (n=%d) clock ended; draining before verdict",
                    self.band_id, self.label_n)

    def _finish_band(self, now: float, verdict) -> None:
        cfg = self.cfg
        assert cfg is not None
        self.verdict_pending = False
        v = verdict(self.band_id)
        kind = "ladder" if self.band_id <= len(cfg.points) else "probe"
        res = BandResult(
            band_id=self.band_id, n=self.label_n, passed=bool(v["passed"]),
            missed_slots=int(v["missed_slots"]), failed_ops=int(v["failed_ops"]),
            ops=int(v["ops"]), suspect=self.suspect_next,
            started_at=self.band_started_at, ended_at=now, kind=kind,
            cpu_at_start=list(self.band_cpu_at_start),
            rules=dict(v.get("rules", {})),
            failed_rules=list(v.get("failed_rules", [])),
        )
        self.results.append(res)
        self.suspect_next = False
        rule_text = " ".join(
            f"{k}={r.get('value')}{'<=' if r['passed'] else '>'}{r.get('bar')}"
            for k, r in res.rules.items() if k != "A")
        logger.info("Band %d (n=%d) %s: misses=%d failed_ops=%d ops=%d %s%s%s",
                    res.band_id, res.n, "PASS" if res.passed else "FAIL",
                    res.missed_slots, res.failed_ops, res.ops, rule_text,
                    f" failed_rules={res.failed_rules}" if res.failed_rules else "",
                    " [SUSPECT]" if res.suspect else "")
        self._update_bracket(res)

        nxt = self._next_target()
        if nxt is None:
            self.phase = DONE
            logger.info("Ladder complete. %s", self.knee_note or "")
            return
        # Pool is clean here, so growing or shrinking is safe: no user is
        # mid-operation. The warm stage's barrier clock starts now.
        self.pause_started = now
        self.band_id += 1
        self.label_n = nxt
        self.target = nxt
        logger.info("Pause: preparing band %d (n=%d, was %d)",
                    self.band_id, nxt, self.results[-1].n)

    def _update_bracket(self, res: BandResult) -> None:
        if res.passed:
            if self.last_pass is None or res.n > self.last_pass:
                self.last_pass = res.n
            if self.first_fail is not None and res.n > self.first_fail:
                logger.warning("Non-monotonic: n=%d passed above earlier fail n=%d; "
                               "dropping that fail from the bracket", res.n, self.first_fail)
                self.first_fail = None
        else:
            if self.last_pass is None or res.n > self.last_pass:
                if self.first_fail is None or res.n < self.first_fail:
                    self.first_fail = res.n

    def _next_target(self) -> int | None:
        cfg = self.cfg
        assert cfg is not None
        if self.queue:
            return self.queue.pop(0)
        if not cfg.bisect:
            return None
        if self.last_pass is None:
            self.knee_note = "knee: first band failed; nothing to bisect"
            return None
        if self.first_fail is None:
            self.knee_note = f"knee: all bands passed up to n={self.last_pass}; no failure to bracket"
            return None
        probe = next_probe(self.last_pass, self.first_fail, cfg.bisect_resolution)
        if probe is None:
            self.knee_note = (f"knee: last_pass={self.last_pass} first_fail={self.first_fail} "
                              f"(within resolution {cfg.bisect_resolution})")
            return None
        return probe

    # ------------------------------------------------------------------
    def summary(self) -> dict:
        return {
            "bands": [
                {
                    "band_id": r.band_id, "n": r.n, "kind": r.kind,
                    "passed": r.passed, "suspect": r.suspect,
                    "missed_slots": r.missed_slots, "failed_ops": r.failed_ops,
                    "ops": r.ops,
                    "started_at": round(r.started_at), "ended_at": round(r.ended_at),
                    "cpu_at_start": r.cpu_at_start,
                    "rules": r.rules, "failed_rules": r.failed_rules,
                }
                for r in self.results
            ],
            "knee": {
                "last_pass": self.last_pass,
                "first_fail": self.first_fail,
                "probes": [r.n for r in self.results if r.kind == "probe"],
                "note": self.knee_note,
            },
        }


# The process-wide instance shared by the shape and the user class.
ladder = Ladder()


def wall_clock() -> float:
    return time.time()
