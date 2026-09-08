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

"""Unit tests for common/openclaw_math.py and common/openclaw_ladder.py
(stdlib only):

    python3 -m unittest discover benchmarking/openclaw-density/tests
"""

import os
import sys
import unittest

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "locust"),
)

from common.openclaw_ladder import (  # noqa: E402
    BAND,
    DONE,
    PAUSE,
    POOL_CHECK_INTERVAL_S,
    WARMUP,
    Ladder,
    LadderConfig,
)
from common.openclaw_math import (  # noqa: E402
    band_verdict,
    cohort_and_offset,
    next_probe,
    parse_points,
    percentiles,
)


class ParsePointsTest(unittest.TestCase):
    def test_absolute_counts(self):
        self.assertEqual(parse_points("160,240,320", 0), [160, 240, 320])

    def test_multipliers_resolve_against_workers(self):
        self.assertEqual(parse_points("2x,3x,4x", 80), [160, 240, 320])

    def test_mixed_and_whitespace(self):
        self.assertEqual(parse_points(" 2x , 100 ", 50), [100, 100])

    def test_multiplier_without_workers_raises(self):
        with self.assertRaises(ValueError):
            parse_points("2x", 0)

    def test_empty_raises(self):
        with self.assertRaises(ValueError):
            parse_points(" , ", 80)


class CohortAndOffsetTest(unittest.TestCase):
    CYCLE = 1800.0
    DWELL = 180.0
    BUFFER = 120.0  # openclaw_math.STAGGER_EDGE_BUFFER_S

    def _cohorts(self, total, herd=0.25, cycle=None, dwell=None, window="remaining"):
        return [
            cohort_and_offset(
                r, total, herd,
                self.CYCLE if cycle is None else cycle,
                self.DWELL if dwell is None else dwell,
                window,
            )
            for r in range(total)
        ]

    def test_quarter_herd_is_every_fourth_rank(self):
        results = self._cohorts(320)
        herd = [r for r, (c, _) in enumerate(results) if c == "herd"]
        self.assertEqual(len(herd), 80)
        self.assertEqual(herd[:3], [0, 4, 8])

    def test_split_holds_for_every_band_size(self):
        for n in (352, 528, 704, 440):
            herd = sum(1 for c, _ in self._cohorts(n) if c == "herd")
            self.assertEqual(herd, n // 4)

    def test_herd_wakes_at_boundary(self):
        for cohort, offset in self._cohorts(320):
            if cohort == "herd":
                self.assertEqual(offset, 0.0)

    def test_remaining_window_clears_herd_and_next_boundary(self):
        offsets = [o for c, o in self._cohorts(320) if c == "staggered"]
        self.assertEqual(len(offsets), 240)
        self.assertGreaterEqual(min(offsets), self.DWELL + self.BUFFER)
        self.assertLessEqual(max(offsets), self.CYCLE - self.DWELL - self.BUFFER)

    def test_full_window_spans_whole_cycle(self):
        offsets = sorted(o for c, o in self._cohorts(320, window="full") if c == "staggered")
        self.assertLess(offsets[0], self.DWELL)  # inside the herd window
        self.assertGreater(offsets[-1], self.CYCLE - self.DWELL)

    def test_staggered_offsets_evenly_spaced(self):
        for window in ("remaining", "full"):
            offsets = sorted(o for c, o in self._cohorts(320, window=window) if c == "staggered")
            gaps = [b - a for a, b in zip(offsets, offsets[1:])]
            self.assertAlmostEqual(min(gaps), max(gaps), places=6)

    def test_degenerate_short_cycle_falls_back_to_full_spread(self):
        for _, o in self._cohorts(8, cycle=120.0, dwell=60.0):
            self.assertGreaterEqual(o, 0.0)
            self.assertLess(o, 120.0)

    def test_full_herd(self):
        self.assertTrue(all(c == "herd" for c, _ in self._cohorts(10, herd=1.0)))

    def test_no_herd_is_all_staggered(self):
        results = self._cohorts(8, herd=0.0)
        self.assertTrue(all(c == "staggered" for c, _ in results))
        offsets = [o for _, o in results]
        self.assertEqual(offsets, sorted(offsets))


class PercentilesTest(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(percentiles([]), {})

    def test_known_values(self):
        stats = percentiles([float(v) for v in range(1, 101)])
        self.assertEqual(stats["count"], 100)
        self.assertEqual(stats["p50_s"], 51.0)
        self.assertEqual(stats["p99_s"], 100.0)
        self.assertEqual(stats["max_s"], 100.0)


def _rec(band_id, op, ok=True, phase="band"):
    return {"band_id": band_id, "op": op, "ok": ok, "phase": phase,
            "n": 0, "cohort": "x", "elapsed_s": 1.0, "code": "OK", "t": 0}


class BandVerdictTest(unittest.TestCase):
    def test_clean_band_passes(self):
        recs = [_rec(1, "resume_herd"), _rec(1, "suspend")]
        v = band_verdict(recs, 1)
        self.assertTrue(v["passed"])
        self.assertEqual((v["missed_slots"], v["failed_ops"], v["ops"]), (0, 0, 2))

    def test_overrun_fails(self):
        recs = [_rec(1, "resume_herd"), _rec(1, "cycle_overrun", ok=False)]
        self.assertFalse(band_verdict(recs, 1)["passed"])
        self.assertEqual(band_verdict(recs, 1)["missed_slots"], 1)

    def test_failed_op_fails(self):
        recs = [_rec(1, "resume_herd", ok=False)]
        v = band_verdict(recs, 1)
        self.assertFalse(v["passed"])
        self.assertEqual(v["failed_ops"], 1)

    def test_other_bands_and_warmup_ignored(self):
        recs = [_rec(1, "resume_herd"),
                _rec(2, "cycle_overrun", ok=False),          # another band
                _rec(1, "boot", ok=False, phase="warmup")]   # warm-up
        self.assertTrue(band_verdict(recs, 1)["passed"])

    def test_empty_band_is_not_a_pass(self):
        self.assertFalse(band_verdict([], 1)["passed"])

    def test_rules_off_by_default(self):
        v = band_verdict([_rec(1, "resume_staggered")], 1)
        self.assertEqual(list(v["rules"]), ["A"])
        self.assertEqual(v["failed_rules"], [])


def _timed(band_id, op, elapsed_s, offset_s=1200.0, ok=True, cohort="staggered"):
    r = _rec(band_id, op, ok=ok)
    r.update({"elapsed_s": elapsed_s, "offset_s": offset_s, "cohort": cohort})
    return r


class HealthRulesTest(unittest.TestCase):
    """Rules B (calm-window staggered p90) and C (suspend p50)."""

    def test_rule_b_passes_when_calm_resumes_fast(self):
        recs = [_timed(1, "resume_staggered", 4.0) for _ in range(20)]
        v = band_verdict(recs, 1, calm_start_seconds=900,
                         staggered_p90_max_seconds=10)
        self.assertTrue(v["passed"])
        self.assertEqual(v["rules"]["B"]["value"], 4.0)
        self.assertEqual(v["rules"]["B"]["samples"], 20)

    def test_rule_b_fails_on_slow_tail(self):
        recs = ([_timed(1, "resume_staggered", 4.0) for _ in range(15)]
                + [_timed(1, "resume_staggered", 90.0) for _ in range(5)])
        v = band_verdict(recs, 1, calm_start_seconds=900,
                         staggered_p90_max_seconds=10)
        self.assertFalse(v["passed"])
        self.assertEqual(v["failed_rules"], ["B"])
        self.assertTrue(v["rules"]["A"]["passed"])  # still on time

    def test_rule_b_ignores_slots_inside_the_storm(self):
        slow_in_storm = [_timed(1, "resume_staggered", 300.0, offset_s=120.0)
                         for _ in range(10)]
        calm = [_timed(1, "resume_staggered", 4.0, offset_s=1000.0)
                for _ in range(10)]
        v = band_verdict(slow_in_storm + calm, 1, calm_start_seconds=900,
                         staggered_p90_max_seconds=10)
        self.assertTrue(v["passed"])
        self.assertEqual(v["rules"]["B"]["samples"], 10)

    def test_rule_b_ignores_herd_and_failed_ops(self):
        recs = ([_timed(1, "resume_herd", 400.0) for _ in range(10)]
                + [_timed(1, "resume_staggered", 400.0, ok=False)]
                + [_timed(1, "resume_staggered", 4.0) for _ in range(10)])
        v = band_verdict(recs, 1, staggered_p90_max_seconds=10)
        self.assertTrue(v["rules"]["B"]["passed"])
        self.assertFalse(v["passed"])  # the failed op trips rule A

    def test_rule_b_no_samples_fails(self):
        recs = [_timed(1, "resume_staggered", 4.0, offset_s=100.0)]
        v = band_verdict(recs, 1, calm_start_seconds=900,
                         staggered_p90_max_seconds=10)
        self.assertFalse(v["rules"]["B"]["passed"])
        self.assertIsNone(v["rules"]["B"]["value"])

    def test_rule_c_judges_calm_staggered_suspends_at_p90(self):
        fast = [_timed(1, "suspend", 2.0) for _ in range(10)]
        v = band_verdict(fast, 1, staggered_suspend_p90_max_seconds=6)
        self.assertTrue(v["passed"])
        self.assertEqual(v["rules"]["C"]["value"], 2.0)
        tail = ([_timed(1, "suspend", 2.0) for _ in range(8)]
                + [_timed(1, "suspend", 40.0) for _ in range(2)])
        v = band_verdict(tail, 1, staggered_suspend_p90_max_seconds=6)
        self.assertFalse(v["passed"])
        self.assertEqual(v["failed_rules"], ["C"])
        self.assertEqual(v["rules"]["C"]["value"], 40.0)

    def test_rule_c_ignores_herd_suspends(self):
        herd_wave = [_timed(1, "suspend", 90.0, offset_s=0.0, cohort="herd")
                     for _ in range(10)]
        calm = [_timed(1, "suspend", 2.0) for _ in range(10)]
        v = band_verdict(herd_wave + calm, 1, calm_start_seconds=600,
                         staggered_suspend_p90_max_seconds=6)
        self.assertTrue(v["rules"]["C"]["passed"])
        self.assertEqual(v["rules"]["C"]["samples"], 10)

    def test_rule_c_ignores_staggered_suspends_inside_the_herd_window(self):
        early = [_timed(1, "suspend", 60.0, offset_s=120.0) for _ in range(10)]
        calm = [_timed(1, "suspend", 2.0, offset_s=900.0) for _ in range(10)]
        v = band_verdict(early + calm, 1, calm_start_seconds=600,
                         staggered_suspend_p90_max_seconds=6)
        self.assertTrue(v["rules"]["C"]["passed"])

    def test_rule_c_no_samples_fails(self):
        only_herd = [_timed(1, "suspend", 2.0, cohort="herd")]
        v = band_verdict(only_herd, 1, staggered_suspend_p90_max_seconds=6)
        self.assertFalse(v["rules"]["C"]["passed"])
        self.assertIsNone(v["rules"]["C"]["value"])

    def test_all_rules_reported_together(self):
        recs = ([_timed(1, "resume_staggered", 4.0) for _ in range(10)]
                + [_timed(1, "suspend", 2.0) for _ in range(10)])
        v = band_verdict(recs, 1, staggered_p90_max_seconds=10,
                         staggered_suspend_p90_max_seconds=10)
        self.assertEqual(sorted(v["rules"]), ["A", "B", "C"])
        self.assertTrue(v["passed"])


class HerdRuleTest(unittest.TestCase):
    """Rule D: herd resume p50, off unless a bar is given."""

    def test_off_by_default(self):
        recs = [_timed(1, "resume_herd", 300.0, offset_s=0.0, cohort="herd")]
        self.assertNotIn("D", band_verdict(recs, 1)["rules"])

    def test_pass_and_fail_on_p90(self):
        fast = [_timed(1, "resume_herd", 25.0, offset_s=0.0, cohort="herd")
                for _ in range(10)]
        v = band_verdict(fast, 1, herd_resume_p90_max_seconds=60)
        self.assertTrue(v["passed"])
        self.assertEqual(v["rules"]["D"]["value"], 25.0)
        slow = [_timed(1, "resume_herd", 79.0, offset_s=0.0, cohort="herd")
                for _ in range(10)]
        v = band_verdict(slow, 1, herd_resume_p90_max_seconds=60)
        self.assertFalse(v["passed"])
        self.assertEqual(v["failed_rules"], ["D"])

    def test_ignores_staggered_and_no_samples_fails(self):
        stag = [_timed(1, "resume_staggered", 300.0) for _ in range(10)]
        v = band_verdict(stag, 1, herd_resume_p90_max_seconds=60)
        self.assertFalse(v["rules"]["D"]["passed"])
        self.assertIsNone(v["rules"]["D"]["value"])


class NextProbeTest(unittest.TestCase):
    def test_midpoint(self):
        self.assertEqual(next_probe(352, 528, 44), 440)

    def test_within_resolution_stops(self):
        self.assertIsNone(next_probe(440, 484, 44))

    def test_no_bracket(self):
        self.assertIsNone(next_probe(None, 528, 44))
        self.assertIsNone(next_probe(352, None, 44))


class FakeWorld:
    """Drives a Ladder with a fake clock, population, pool and verdicts."""

    def __init__(self, cfg, verdicts):
        self.ladder = Ladder()
        self.now = 1000.0
        self.ladder.configure(cfg, self.now)
        self.alive = 0
        self.warmed = True
        self.pool_clean = True
        self.verdicts = verdicts   # n -> passed
        self.targets = []
        self.last_warm = None      # time of last warm-up op (None = none yet)
        self.cpu = [1.0, 2.0]

    def tick(self):
        out = self.ladder.tick(
            self.now, self.alive, self.warmed,
            lambda: self.pool_clean,
            lambda bid: {"passed": self.verdicts[self.ladder.label_n],
                         "missed_slots": 0 if self.verdicts[self.ladder.label_n] else 3,
                         "failed_ops": 0, "ops": 10},
            self.last_warm,
            lambda: self.cpu,
        )
        self.targets.append(out)
        return out

    def advance(self, seconds):
        self.now += seconds

    def settle_population(self):
        """Pretend locust reached the target and everyone is warmed."""
        self.alive = self.ladder.target
        self.warmed = True


def _cfg(points, cycle=1800.0, cycles=1, lead=90.0, barrier_max=1800.0,
         bisect=False, resolution=44, settle=120.0):
    return LadderConfig(points=points, cycle_seconds=cycle, cycles_per_point=cycles,
                        lead_seconds=lead, barrier_max_seconds=barrier_max,
                        bisect=bisect, bisect_resolution=resolution,
                        settle_seconds=settle)


class LadderTest(unittest.TestCase):
    def _run_pause_to_band(self, w):
        """Tick until the ladder enters BAND (pool clean, population settled)."""
        w.settle_population()
        for _ in range(10):
            w.advance(POOL_CHECK_INTERVAL_S)
            w.tick()
            if w.ladder.phase == BAND:
                return
        self.fail("ladder never entered BAND")

    def test_warmup_waits_for_population_and_pool(self):
        w = FakeWorld(_cfg([352, 528]), {352: True, 528: True})
        self.assertEqual(w.ladder.phase, WARMUP)
        self.assertEqual(w.tick(), (352, 50.0))
        # population not there yet -> still warm-up
        w.alive = 100
        w.advance(POOL_CHECK_INTERVAL_S)
        w.tick()
        self.assertEqual(w.ladder.phase, WARMUP)
        # population there but pool busy -> still warm-up
        w.settle_population()
        w.pool_clean = False
        w.advance(POOL_CHECK_INTERVAL_S)
        w.tick()
        self.assertEqual(w.ladder.phase, WARMUP)
        # clean -> band starts with a fresh epoch
        w.pool_clean = True
        w.advance(POOL_CHECK_INTERVAL_S)
        w.tick()
        self.assertEqual(w.ladder.phase, BAND)
        self.assertAlmostEqual(w.ladder.epoch, w.now + 90.0 - 1800.0)
        self.assertEqual(w.ladder.cycle_index(w.now), 1)

    def _end_band_and_drain(self, w):
        """Advance past the band's clock, then tick until the drain stage
        has taken the verdict (pool clean)."""
        w.advance(90.0 + 2 * 1800.0)
        w.tick()  # -> PAUSE, drain stage
        for _ in range(10):
            if not w.ladder.verdict_pending or w.ladder.phase == DONE:
                return
            w.advance(POOL_CHECK_INTERVAL_S)
            w.tick()
        self.fail("drain never completed")

    def test_band_end_drains_before_verdict(self):
        w = FakeWorld(_cfg([352, 528]), {352: True, 528: True})
        self._run_pause_to_band(w)
        w.advance(90.0 + 1800.0 - 1)   # last chime + almost one cycle
        w.tick()
        self.assertEqual(w.ladder.phase, BAND)
        w.advance(2)
        w.tick()
        # clock over: pause, but no verdict and no resize until pool clean
        self.assertEqual(w.ladder.phase, PAUSE)
        self.assertTrue(w.ladder.verdict_pending)
        self.assertEqual(w.ladder.target, 352)
        self.assertEqual(w.ladder.results, [])
        w.pool_clean = False
        w.advance(POOL_CHECK_INTERVAL_S)
        self.assertEqual(w.tick(), (352, 50.0))
        self.assertEqual(w.ladder.results, [])
        # late routines done -> verdict taken, next band prepared
        w.pool_clean = True
        w.advance(POOL_CHECK_INTERVAL_S)
        self.assertEqual(w.tick(), (528, 50.0))
        self.assertFalse(w.ladder.verdict_pending)
        self.assertEqual(w.ladder.band_id, 2)
        self.assertEqual(w.ladder.results[0].n, 352)
        self.assertTrue(w.ladder.results[0].passed)

    def test_full_ladder_then_done_without_bisect(self):
        w = FakeWorld(_cfg([352, 528, 704]), {352: True, 528: True, 704: False})
        for _ in range(3):
            self._run_pause_to_band(w)
            self._end_band_and_drain(w)
        self.assertEqual(w.ladder.phase, DONE)
        self.assertEqual([r.n for r in w.ladder.results], [352, 528, 704])
        self.assertEqual(w.ladder.last_pass, 528)
        self.assertEqual(w.ladder.first_fail, 704)
        self.assertIsNone(w.tick())

    def test_bisect_probes_midpoints_shrinking_only_after_drain(self):
        verdicts = {352: True, 528: True, 704: False, 616: False, 572: True}
        w = FakeWorld(_cfg([352, 528, 704], bisect=True, resolution=44), verdicts)
        for _ in range(3):
            self._run_pause_to_band(w)
            self._end_band_and_drain(w)
        # after 704 fails and drains: shrink to probe 616 happens at once
        self.assertEqual(w.ladder.phase, PAUSE)
        self.assertEqual(w.ladder.target, 616)
        self.assertEqual(w.ladder.label_n, 616)
        # run probe 616 (fails) -> probe 572 (passes) -> within 44 -> done
        self._run_pause_to_band(w)
        self._end_band_and_drain(w)
        self.assertEqual(w.ladder.first_fail, 616)
        self.assertEqual(w.ladder.target, 572)
        self._run_pause_to_band(w)
        self._end_band_and_drain(w)
        self.assertEqual(w.ladder.phase, DONE)
        s = w.ladder.summary()
        self.assertEqual(s["knee"]["last_pass"], 572)
        self.assertEqual(s["knee"]["first_fail"], 616)
        self.assertEqual(s["knee"]["probes"], [616, 572])
        self.assertEqual([b["kind"] for b in s["bands"]],
                         ["ladder", "ladder", "ladder", "probe", "probe"])

    def test_bisect_with_all_pass_reports_no_bracket(self):
        w = FakeWorld(_cfg([352, 528], bisect=True), {352: True, 528: True})
        for _ in range(2):
            self._run_pause_to_band(w)
            self._end_band_and_drain(w)
        self.assertEqual(w.ladder.phase, DONE)
        self.assertIn("all bands passed", w.ladder.knee_note)

    def test_drain_cap_forces_verdict(self):
        w = FakeWorld(_cfg([352, 528], barrier_max=600.0), {352: True, 528: True})
        self._run_pause_to_band(w)
        w.advance(90.0 + 2 * 1800.0)
        w.tick()
        w.pool_clean = False           # something stays stuck
        w.advance(601.0)
        w.tick()
        self.assertFalse(w.ladder.verdict_pending)
        self.assertEqual(len(w.ladder.results), 1)
        self.assertEqual(w.ladder.target, 528)

    def test_warm_stage_waits_for_settle_after_last_warmup_op(self):
        w = FakeWorld(_cfg([352], settle=120.0), {352: True})
        w.settle_population()
        w.last_warm = w.now            # a warm-up op just finished
        w.advance(POOL_CHECK_INTERVAL_S)
        w.tick()
        self.assertEqual(w.ladder.phase, WARMUP)   # not settled yet
        w.advance(60.0)
        w.tick()
        self.assertEqual(w.ladder.phase, WARMUP)   # 70 s < 120 s
        w.advance(60.0)
        w.tick()
        self.assertEqual(w.ladder.phase, BAND)     # 130 s >= 120 s

    def test_cpu_at_band_start_is_recorded(self):
        w = FakeWorld(_cfg([352]), {352: True})
        w.cpu = [12.3, 4.56]
        self._run_pause_to_band(w)
        self._end_band_and_drain(w)
        self.assertEqual(w.ladder.results[0].cpu_at_start, [12.3, 4.6])
        self.assertEqual(w.ladder.summary()["bands"][0]["cpu_at_start"], [12.3, 4.6])

    def test_barrier_cap_starts_band_as_suspect(self):
        w = FakeWorld(_cfg([352], barrier_max=600.0), {352: True})
        w.alive = 352
        w.warmed = False          # warm-up never completes
        w.advance(601.0)
        w.tick()
        self.assertEqual(w.ladder.phase, BAND)
        self._end_band_and_drain(w)
        self.assertTrue(w.ladder.results[0].suspect)

    def test_from_options_defaults_resolution_to_quarter_workers(self):
        class Opts:
            points = "2x,3x"
            workers = 176
            cycle_seconds = 1800
            cycles_per_point = 1
            first_boundary_lead_seconds = 90
            barrier_max_seconds = 1800
            bisect = "true"
            bisect_resolution = 0

        cfg = LadderConfig.from_options(Opts())
        self.assertEqual(cfg.points, [352, 528])
        self.assertEqual(cfg.bisect_resolution, 44)
        self.assertTrue(cfg.bisect)


if __name__ == "__main__":
    unittest.main()
