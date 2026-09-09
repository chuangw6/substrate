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

"""OpenClaw actor-density cycle test (see benchmarking/openclaw-density/).

Each locust user is one actor living the capacity math model's lifecycle:
resume, stay active for a dwell, suspend, sleep until its slot in the next
cycle. 25% of users (the herd cohort) resume at the same instant on every
cycle boundary; the rest resume at evenly spaced offsets. Run it with
shapes/openclaw_density.py, which runs each --points count as a clean band
(see common/openclaw_ladder.py): new actors boot and suspend during the
pause before their band, and a band's clock only starts once every actor
is verifiably suspended.

Request names carry the band's actor count, so locust's stats split per
band without post-processing:

    boot[n=352]              first-ever resume of a fresh actor (pause)
    suspend_warmup[n=352]    the suspend right after that boot (pause)
    practice_resume[n=352]  practice cycle: resume, idle an active period, suspend (pause)
    practice_suspend[n=352] the suspend that ends the practice cycle (pause)
    resume_herd[n=352]      measured resume, herd cohort
    resume_staggered[n=352]  measured resume, staggered cohort
    suspend[n=352]           measured suspend back to snapshot
    cycle_overrun[n=352]     resume+dwell+suspend missed the cycle period

Latencies are client-observed wall time from the first RPC attempt to
success, so a resume that waits for a free worker reports the wait. On the
master, a watcher greenlet samples the density nodes and the worker pods
(kubernetes API; skipped with a warning when unavailable) and a test-stop
hook writes summary.json / model_params.json / node_ts.csv into
$BENCHMARK_EXTRA_DIR for runner.py to upload.
"""

import bisect
import json
import logging
import os
import time

import gevent
import grpc
from gevent.lock import BoundedSemaphore
from locust import User, events, task
from locust.argument_parser import LocustArgumentParser
from locust.runners import LocalRunner, MasterRunner, WorkerRunner

from common import ateapi_pb2, ateapi_pb2_grpc
from common.ateapi_channel import ateapi_channel
from common.atespace import ensure_atespace
from common.grpc_setup import init_grpc_gevent
from common.metrics import init_metrics, update_user_count
from common.openclaw_ladder import DONE, ladder
from common.openclaw_math import band_verdict, cohort_and_offset, parse_points, percentiles

logger = logging.getLogger(__name__)

init_grpc_gevent()
init_metrics()

# gRPC codes ateapi returns while a resume/suspend contends for a worker or
# races a state transition; anything else is a hard failure.
# RESOURCE_EXHAUSTED is the big one: "no free workers available" is the
# normal worker-wait signal during a herd, and waiting through it IS the
# latency this benchmark measures.
CONTENTION_CODES = frozenset({
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.FAILED_PRECONDITION,
    grpc.StatusCode.ABORTED,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
})
RETRY_SLEEP_S = 2.0
WARMUP_BOOT_DEADLINE_S = 900.0
WARMUP_ATTEMPTS = 3

# Shared run state. One locust process runs all users of this test
# (openclaw_cycle.py is in runner.py's PYTHON_TESTS), so module state is the
# coordination channel. The band clock itself lives in
# common.openclaw_ladder (shared with the shape).
_state: dict = {
    "next_index": 0,
    "free_slots": [],     # indices returned by stopped users, reused first
    "booted": set(),      # indices whose actor has ever booted
    "watcher": None,
    "warm_sem": None,
    "pool_stub": None,
    "last_warm_op_at": None,  # wall time the most recent warm-up op finished
}
_records: list[dict] = []
_alive: list[int] = []            # sorted indices of running users
_users: dict[int, "OpenClawUser"] = {}


@events.init_command_line_parser.add_listener
def _(parser: LocustArgumentParser) -> None:
    parser.add_argument(
        "--cycle-seconds", type=float, default=1800.0,
        env_var="OPENCLAW_CYCLE_SECONDS",
        help="Cycle period; 1800 is the model's exact timing",
    )
    parser.add_argument(
        "--active-dwell-seconds", type=float, default=180.0,
        env_var="OPENCLAW_ACTIVE_DWELL_SECONDS",
        help="Active window per cycle (the model's 3 minutes)",
    )
    parser.add_argument(
        "--herd-fraction", type=float, default=0.25,
        env_var="OPENCLAW_HERD_FRACTION",
        help="Fraction of users that resume simultaneously at each cycle "
             "boundary; the rest are spread evenly",
    )
    parser.add_argument(
        "--points", type=str, default="2x,3x,4x",
        env_var="OPENCLAW_POINTS",
        help="Band actor counts, smallest first, as absolute counts or worker "
             "multipliers (2x,3x,4x) resolved against --workers",
    )
    parser.add_argument(
        "--workers", type=int, default=0,
        env_var="OPENCLAW_WORKERS",
        help="Measured worker count. Required when --points uses "
             "multipliers; echoed into summary.json",
    )
    parser.add_argument(
        "--cycles-per-point", type=int, default=1,
        env_var="OPENCLAW_CYCLES_PER_POINT",
        help="Measured cycles per band (boots happen before the band, so "
             "one clean cycle is enough)",
    )
    parser.add_argument(
        "--first-boundary-lead-seconds", type=float, default=90.0,
        env_var="OPENCLAW_FIRST_BOUNDARY_LEAD_SECONDS",
        help="Gap between a band's start and its first chime",
    )
    parser.add_argument(
        "--barrier-max-seconds", type=float, default=3600.0,
        env_var="OPENCLAW_BARRIER_MAX_SECONDS",
        help="Longest a pause may wait for warm-up + all-suspended before "
             "starting the next band anyway (marked suspect)",
    )
    parser.add_argument(
        "--warmup-settle-seconds", type=float, default=120.0,
        env_var="OPENCLAW_WARMUP_SETTLE_SECONDS",
        help="Quiet time required after the last warm-up operation before a "
             "band may start: the physical work behind a suspend (snapshot "
             "upload, sandbox teardown, disk writeback) outlives the API state",
    )
    parser.add_argument(
        "--warmup-concurrency", type=int, default=64,
        env_var="OPENCLAW_WARMUP_CONCURRENCY",
        help="Max simultaneous warm-up boots during a pause; paces the "
             "boot storm so retries don't exhaust the control plane",
    )
    parser.add_argument(
        "--practice-dwell-seconds", type=float, default=180.0,
        env_var="OPENCLAW_PRACTICE_DWELL_SECONDS",
        help="After its warm-up boot+suspend, each new actor does one "
             "unmeasured practice cycle (resume, idle this long, suspend) "
             "before its first band. OpenClaw keeps starting for ~45 s "
             "after its health check first passes; an actor suspended in "
             "that window resumes into the rest of its startup and answers "
             "nothing for 30-50 s. The idle period lets startup finish so "
             "the snapshot is of an idle process. 0 disables",
    )
    parser.add_argument(
        "--calm-start-seconds", type=float, default=900.0,
        env_var="OPENCLAW_CALM_START_SECONDS",
        help="Rule B window: only staggered resumes whose slot is at least "
             "this many seconds after the chime count, i.e. outside the "
             "herd's restore storm and checkpoint wave",
    )
    parser.add_argument(
        "--staggered-p90-max-seconds", type=float, default=10.0,
        env_var="OPENCLAW_STAGGERED_P90_MAX_SECONDS",
        help="Rule B bar: a band fails when the p90 of calm-window staggered "
             "resumes exceeds this (twice the idle-node resume). "
             "Negative disables the rule",
    )
    parser.add_argument(
        "--staggered-suspend-p90-max-seconds", type=float, default=10.0,
        env_var="OPENCLAW_STAGGERED_SUSPEND_P90_MAX_SECONDS",
        help="Rule C bar: a band fails when the p90 of calm-window staggered "
             "suspends (same actors as rule B) exceeds this. Negative disables",
    )
    parser.add_argument(
        "--herd-resume-p90-max-seconds", type=float, default=-1.0,
        env_var="OPENCLAW_HERD_RESUME_P90_MAX_SECONDS",
        help="Rule D bar: a band fails when the herd's resume p90 exceeds "
             "this. Off by default (negative) so the herd, whose cost is a "
             "known property of a synchronized wake-up, does not decide the "
             "knee unless asked",
    )
    parser.add_argument(
        "--bisect", type=str, default="false",
        env_var="OPENCLAW_BISECT",
        help="After the ladder, probe midpoints between the last passing "
             "and first failing band until the knee is pinned (true/false)",
    )
    parser.add_argument(
        "--bisect-resolution", type=int, default=0,
        env_var="OPENCLAW_BISECT_RESOLUTION",
        help="Stop bisecting when pass and fail are this close in actors "
             "(default: workers / 4)",
    )
    parser.add_argument(
        "--template-namespace", type=str, default="benchmark-openclaw",
        help="ActorTemplate namespace for the actors",
    )
    parser.add_argument(
        "--template-name", type=str, default="openclaw",
        help="ActorTemplate name for the actors",
    )
    parser.add_argument(
        "--openclaw-atespace", type=str, default="openclaw-density",
        help="Atespace holding the benchmark's actors",
    )
    parser.add_argument(
        "--workload-namespace", type=str, default="benchmark-openclaw",
        help="Namespace of the worker pods, for the node/pod watcher",
    )
    parser.add_argument(
        "--density-node-label", type=str,
        default="bench.substrate.dev/pool=openclaw-density",
        help="Label selector for the density nodes, for the node watcher",
    )
    parser.add_argument(
        "--stagger-window", type=str, default="remaining",
        env_var="OPENCLAW_STAGGER_WINDOW",
        help="Where staggered wakes land: 'remaining' (model prose: after "
             "the herd, clear of the next boundary) or 'full' (model "
             "formula: whole cycle, uncoordinated-arrival semantics)",
    )
    parser.add_argument(
        "--delete-actors-on-stop", type=str, default="true",
        help="Delete every actor when the run ends (true/false). Actors of "
             "users removed mid-run (bisection shrink) are kept suspended",
    )


def _record(op: str, cohort: str, elapsed_s: float, ok: bool, code: str,
            phase: str) -> None:
    _records.append({
        "t": time.time(), "op": op, "cohort": cohort, "n": ladder.label_n,
        "band_id": ladder.band_id, "phase": phase,
        "elapsed_s": round(elapsed_s, 4), "ok": ok, "code": code,
    })


def _fire(name: str, elapsed_s: float, exception: Exception | None,
          user_class: str) -> None:
    events.request.fire(
        request_type="grpc",
        name=name,
        response_time=elapsed_s * 1000.0,
        response_length=0,
        exception=exception,
        user_class=user_class,
    )


class OpenClawUser(User):
    """One simulated OpenClaw actor on the model's wake/sleep cycle."""

    host = "api.ate-system.svc.cluster.local:443"

    def on_start(self) -> None:
        update_user_count(1, self.__class__.__name__)
        opts = self.environment.parsed_options
        # Smallest free slot first, so a shrink-then-regrow (bisection)
        # reuses suspended actors instead of creating new ones.
        if _state["free_slots"]:
            self.index = _state["free_slots"].pop(0)
        else:
            self.index = _state["next_index"]
            _state["next_index"] += 1
        self.warmed = False

        self.channel = ateapi_channel(self.host)
        self.stub = ateapi_pb2_grpc.ControlStub(self.channel)
        self.atespace = opts.openclaw_atespace
        ensure_atespace(self.stub, self.__class__.__name__, self.atespace)

        self.actor_name = f"oc-{self.index:04d}"
        self.actor_ref = ateapi_pb2.ObjectRef(
            atespace=self.atespace, name=self.actor_name
        )
        self._create_actor()

        bisect.insort(_alive, self.index)
        _users[self.index] = self
        if self.index not in _state["booted"]:
            self._warm_up()
        self.warmed = True

    def _create_actor(self) -> None:
        opts = self.environment.parsed_options
        try:
            self.stub.CreateActor(
                ateapi_pb2.CreateActorRequest(
                    actor=ateapi_pb2.Actor(
                        metadata=ateapi_pb2.ResourceMetadata(
                            atespace=self.atespace, name=self.actor_name
                        ),
                        actor_template_namespace=opts.template_namespace,
                        actor_template_name=opts.template_name,
                    )
                )
            )
        except grpc.RpcError as e:
            if e.code() != grpc.StatusCode.ALREADY_EXISTS:
                raise

    def _recover_actor(self, why: str) -> None:
        """A failed checkpoint (`runsc checkpoint` exit 128, ~1% of suspends)
        leaves the actor wedged in SUSPENDING, pinning its worker; nothing
        can resume it and the pool can never be clean. DeleteActor with
        any_state clears it without a worker bounce; recreate and start
        the warm-up over so the band still gets a fully warmed actor."""
        logger.warning("recovering wedged actor %s (%s): delete + recreate",
                       self.actor_name, why)
        try:
            self.stub.DeleteActor(ateapi_pb2.DeleteActorRequest(
                actor=self.actor_ref, any_state=True,
            ))
        except grpc.RpcError as e:
            if e.code() != grpc.StatusCode.NOT_FOUND:
                logger.warning("recover: delete %s failed: %s", self.actor_name, e)
        gevent.sleep(2)
        self._create_actor()
        _state["booted"].discard(self.index)

    def _warm_up(self) -> None:
        """Boot once and suspend, so the band's first resume is a restore.
        Runs during a pause, paced by --warmup-concurrency. A failed step
        recovers the actor (see _recover_actor) before retrying, so a
        wedged actor cannot stall the pause."""
        opts = self.environment.parsed_options
        cls = self.__class__.__name__
        if _state["warm_sem"] is None:
            _state["warm_sem"] = BoundedSemaphore(max(1, opts.warmup_concurrency))
        with _state["warm_sem"]:
            for attempt in range(1, WARMUP_ATTEMPTS + 1):
                # A retry's boot must not wait out the full deadline on an
                # actor that is wedged: a fresh actor boots in minutes.
                deadline = WARMUP_BOOT_DEADLINE_S if attempt == 1 else 300.0
                ok, elapsed, code = self._retrying(
                    self.stub.ResumeActor,
                    ateapi_pb2.ResumeActorRequest(actor=self.actor_ref),
                    time.time() + deadline,
                )
                _fire(f"boot[n={ladder.label_n}]", elapsed,
                      None if ok else RuntimeError(f"boot: {code}"), cls)
                _record("boot", "warmup", elapsed, ok, code, "warmup")
                _state["last_warm_op_at"] = time.time()
                if not ok:
                    self._recover_actor(f"boot failed: {code}")
                    continue
                _state["booted"].add(self.index)
                ok, elapsed, code = self._retrying(
                    self.stub.SuspendActor,
                    ateapi_pb2.SuspendActorRequest(actor=self.actor_ref),
                    time.time() + WARMUP_BOOT_DEADLINE_S,
                )
                _fire(f"suspend_warmup[n={ladder.label_n}]", elapsed,
                      None if ok else RuntimeError(f"suspend: {code}"), cls)
                _record("suspend_warmup", "warmup", elapsed, ok, code, "warmup")
                _state["last_warm_op_at"] = time.time()
                if not ok:
                    self._recover_actor(f"suspend failed: {code}")
                    continue
                if opts.practice_dwell_seconds <= 0 or self._practice(cls):
                    return
                self._recover_actor("practice failed")
            logger.error("warm-up failed for %s after %d attempts",
                         self.actor_name, WARMUP_ATTEMPTS)

    def _practice(self, cls: str) -> bool:
        """One unmeasured practice cycle: resume, idle for the model's active
        period, suspend. OpenClaw answers its health check ~2 s after start
        but keeps starting for ~45 s (event loop blocked ~30 s, then an
        update check, pricing fetches, and an actor bootstrap). A warm-up
        that suspends right after the health check captures that
        half-started process, and its first measured resume spends 30-50 s
        finishing startup before it serves anything. An idle period before
        the suspend makes the snapshot one of an idle process, which
        resumes in ~2 s. Returns False if either op failed."""
        opts = self.environment.parsed_options
        ok, elapsed, code = self._retrying(
            self.stub.ResumeActor,
            ateapi_pb2.ResumeActorRequest(actor=self.actor_ref),
            time.time() + WARMUP_BOOT_DEADLINE_S,
        )
        _fire(f"practice_resume[n={ladder.label_n}]", elapsed,
              None if ok else RuntimeError(f"practice resume: {code}"), cls)
        _record("practice_resume", "warmup", elapsed, ok, code, "warmup")
        _state["last_warm_op_at"] = time.time()
        if not ok:
            return False
        gevent.sleep(opts.practice_dwell_seconds)
        ok, elapsed, code = self._retrying(
            self.stub.SuspendActor,
            ateapi_pb2.SuspendActorRequest(actor=self.actor_ref),
            time.time() + WARMUP_BOOT_DEADLINE_S,
        )
        _fire(f"practice_suspend[n={ladder.label_n}]", elapsed,
              None if ok else RuntimeError(f"practice suspend: {code}"), cls)
        _record("practice_suspend", "warmup", elapsed, ok, code, "warmup")
        _state["last_warm_op_at"] = time.time()
        return ok

    def on_stop(self) -> None:
        update_user_count(-1, self.__class__.__name__)
        pos = bisect.bisect_left(_alive, self.index)
        if pos < len(_alive) and _alive[pos] == self.index:
            _alive.pop(pos)
        _users.pop(self.index, None)
        bisect.insort(_state["free_slots"], self.index)
        opts = self.environment.parsed_options
        final = ladder.phase == DONE
        if final and opts.delete_actors_on_stop.lower() == "true":
            for attempt in range(5):
                try:
                    self.stub.DeleteActor(
                        ateapi_pb2.DeleteActorRequest(
                            actor=self.actor_ref, any_state=True
                        )
                    )
                    break
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.NOT_FOUND:
                        break
                    if attempt == 4:
                        logger.warning(
                            "cleanup DeleteActor(%s) failed after retries: %s",
                            self.actor_name, e,
                        )
                    else:
                        gevent.sleep(5)
        # Mid-run removal (bisection shrink) happens only during a pause,
        # after the pool checker confirmed this actor is suspended; it is
        # kept for reuse by a later probe.
        self.channel.close()

    def _retrying(self, rpc, request, deadline: float):
        """Call `rpc` until success, a non-contention error, or `deadline`.

        Returns (ok, elapsed_s, code). Elapsed spans first attempt to final
        outcome, so a resume that waits through worker contention reports
        the wait as its latency — that queueing is the measurement.
        """
        start = time.time()
        while True:
            try:
                rpc(request)
                return True, time.time() - start, "OK"
            except grpc.RpcError as e:
                code = e.code()
                if code not in CONTENTION_CODES or time.time() >= deadline:
                    return (False, time.time() - start,
                            code.name if code else "UNKNOWN")
            gevent.sleep(min(RETRY_SLEEP_S, max(0.0, deadline - time.time())))
            if time.time() >= deadline:
                return False, time.time() - start, "DEADLINE"

    def _heal_after_failure(self, why: str) -> None:
        """After a failed measured op, make sure the actor is not wedged.
        Anything other than SUSPENDED/RUNNING (e.g. SUSPENDING after a bad
        checkpoint) is recovered now and re-warmed during the next pause,
        so the pause's drain is never held hostage by a broken actor."""
        try:
            a = self.stub.GetActor(ateapi_pb2.GetActorRequest(actor=self.actor_ref))
            state = a.status.state
        except grpc.RpcError as e:
            state = None if e.code() == grpc.StatusCode.NOT_FOUND else ateapi_pb2.ACTOR_STATE_SUSPENDED
        if state in (ateapi_pb2.ACTOR_STATE_SUSPENDED, ateapi_pb2.ACTOR_STATE_RUNNING):
            return
        self._recover_actor(f"{why}; state={ateapi_pb2.ActorState.Name(state) if state is not None else 'NOT_FOUND'}")
        self.warmed = False

    @task
    def run_cycle(self) -> None:
        opts = self.environment.parsed_options
        cycle = opts.cycle_seconds
        if not ladder.in_band:
            if not self.warmed:
                # Recovered mid-band: re-warm during the pause so the next
                # band's first resume is a restore again.
                self._warm_up()
                self.warmed = True
                return
            gevent.sleep(2)
            return
        epoch = ladder.epoch
        band_id = ladder.band_id
        n = ladder.label_n
        now = time.time()
        k = ladder.cycle_index(now)
        if k > opts.cycles_per_point:
            gevent.sleep(2)  # this band's chimes are spent; wait for the pause
            return

        # Cohort and offset from this user's rank among the alive users.
        # The population is fixed for the whole band (changes only happen
        # during pauses), so ranks are stable within a band.
        rank = bisect.bisect_left(_alive, self.index)
        cohort, offset = cohort_and_offset(
            rank, len(_alive), opts.herd_fraction, cycle,
            opts.active_dwell_seconds, opts.stagger_window,
        )
        self.cohort = cohort
        slot = epoch + k * cycle + offset
        gevent.sleep(max(0.0, slot - time.time()))
        if ladder.epoch != epoch:  # band ended while we slept
            return
        deadline = slot + cycle
        cls = self.__class__.__name__

        op = f"resume_{cohort}"
        ok, elapsed, code = self._retrying(
            self.stub.ResumeActor,
            ateapi_pb2.ResumeActorRequest(actor=self.actor_ref),
            deadline,
        )
        exc = None if ok else RuntimeError(f"ResumeActor: {code}")
        _fire(f"{op}[n={n}]", elapsed, exc, cls)
        _records.append({
            "t": time.time(), "op": op, "cohort": cohort, "n": n,
            "band_id": band_id, "phase": "band", "offset_s": round(offset, 1),
            "elapsed_s": round(elapsed, 4), "ok": ok, "code": code,
        })
        if not ok:
            _fire(f"cycle_overrun[n={n}]", time.time() - slot,
                  RuntimeError(f"resume failed/late: {code}"), cls)
            _records.append({
                "t": time.time(), "op": "cycle_overrun", "cohort": cohort,
                "n": n, "band_id": band_id, "phase": "band",
                "offset_s": round(offset, 1),
                "elapsed_s": round(time.time() - slot, 4), "ok": False,
                "code": code,
            })
            self._heal_after_failure(f"resume failed: {code}")
            return

        gevent.sleep(
            min(opts.active_dwell_seconds, max(0.0, deadline - time.time()))
        )

        ok, elapsed, code = self._retrying(
            self.stub.SuspendActor,
            ateapi_pb2.SuspendActorRequest(actor=self.actor_ref),
            # Grace of one extra cycle: abandoning the suspend would leak
            # the worker; the overrun below still charges the miss.
            deadline + cycle,
        )
        exc = None if ok else RuntimeError(f"SuspendActor: {code}")
        _fire(f"suspend[n={n}]", elapsed, exc, cls)
        _records.append({
            "t": time.time(), "op": "suspend", "cohort": cohort, "n": n,
            "band_id": band_id, "phase": "band", "offset_s": round(offset, 1),
            "elapsed_s": round(elapsed, 4), "ok": ok, "code": code,
        })

        if not ok or time.time() > deadline:
            _fire(f"cycle_overrun[n={n}]", time.time() - slot,
                  RuntimeError("cycle exceeded" if ok else
                               f"suspend failed: {code}"), cls)
            _records.append({
                "t": time.time(), "op": "cycle_overrun", "cohort": cohort,
                "n": n, "band_id": band_id, "phase": "band",
                "offset_s": round(offset, 1),
                "elapsed_s": round(time.time() - slot, 4), "ok": False,
                "code": code if not ok else "LATE",
            })
        if not ok:
            self._heal_after_failure(f"suspend failed: {code}")


# ---------------------------------------------------------------------------
# Master-side hooks for the shape: population, warm-up, pool state, verdict.


def _pool_clean(environment) -> bool | None:
    """True when every actor in the atespace is SUSPENDED and no worker
    carries an assignment (reclaim complete on the worker side). None when
    the check itself failed."""
    opts = environment.parsed_options
    try:
        if _state["pool_stub"] is None:
            _state["pool_stub"] = ateapi_pb2_grpc.ControlStub(
                ateapi_channel(OpenClawUser.host)
            )
        stub = _state["pool_stub"]
        token = ""
        total = 0
        not_suspended = 0
        while True:
            resp = stub.ListActors(ateapi_pb2.ListActorsRequest(
                atespace=opts.openclaw_atespace, page_size=1000,
                page_token=token,
            ))
            for a in resp.actors:
                total += 1
                if a.status.state != ateapi_pb2.ACTOR_STATE_SUSPENDED:
                    not_suspended += 1
            token = resp.next_page_token
            if not token:
                break
        assigned = 0
        workers = 0
        token = ""
        while True:
            wresp = stub.ListWorkers(ateapi_pb2.ListWorkersRequest(
                page_size=1000, page_token=token,
            ))
            for w in wresp.workers:
                workers += 1
                if w.status.HasField("assignment"):
                    assigned += 1
            token = wresp.next_page_token
            if not token:
                break
        clean = not_suspended == 0 and assigned == 0
        logger.info("pool check: %d actors, %d not suspended; %d workers, %d "
                    "assigned -> %s", total, not_suspended, workers, assigned,
                    "CLEAN" if clean else "busy")
        return clean
    except Exception as e:
        logger.warning("pool check failed: %s", e)
        return None


def _verdict_kwargs(opts) -> dict:
    """Rule B/C settings for band_verdict; a negative bar disables a rule."""
    return {
        "calm_start_seconds": opts.calm_start_seconds,
        "staggered_p90_max_seconds": (
            opts.staggered_p90_max_seconds
            if opts.staggered_p90_max_seconds >= 0 else None),
        "staggered_suspend_p90_max_seconds": (
            opts.staggered_suspend_p90_max_seconds
            if opts.staggered_suspend_p90_max_seconds >= 0 else None),
        "herd_resume_p90_max_seconds": (
            opts.herd_resume_p90_max_seconds
            if opts.herd_resume_p90_max_seconds >= 0 else None),
    }


def _install_hooks(environment) -> None:
    opts = environment.parsed_options
    environment.openclaw_hooks = {
        "alive": lambda: len(_alive),
        "all_warmed": lambda: all(u.warmed for u in _users.values()),
        "pool_clean": lambda: _pool_clean(environment),
        "verdict": lambda band_id: band_verdict(
            _records, band_id, **_verdict_kwargs(opts)),
        "last_warm_activity": lambda: _state["last_warm_op_at"],
        "node_cpu": lambda: (
            list(_state["watcher"].latest_cpu_cores.values())
            if _state.get("watcher") else []
        ),
    }


# ---------------------------------------------------------------------------
# Master-side node/pod watcher and summary output.


def _extra_dir() -> str:
    d = os.environ.get("BENCHMARK_EXTRA_DIR", "/tmp/openclaw-extra")
    os.makedirs(d, exist_ok=True)
    return d


def _parse_cpu(q: str) -> float:
    """Kubernetes CPU quantity -> millicores."""
    if q.endswith("n"):
        return float(q[:-1]) / 1e6
    if q.endswith("u"):
        return float(q[:-1]) / 1e3
    if q.endswith("m"):
        return float(q[:-1])
    return float(q) * 1000.0


def _parse_mem(q: str) -> float:
    """Kubernetes memory quantity -> bytes (binary suffixes only, which is
    what metrics.k8s.io emits)."""
    units = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40}
    for suffix, mult in units.items():
        if q.endswith(suffix):
            return float(q[: -len(suffix)]) * mult
    return float(q)


class _Watcher:
    """Samples the density nodes + worker pods every 10s into node_ts.csv,
    and counts hard failures (worker restarts, OOMKilled) for summary.json.
    All kubernetes usage is lazy so the test still runs (without node
    telemetry) where the client library or RBAC is missing."""

    def __init__(self, node_label: str, namespace: str):
        self.node_label = node_label
        self.namespace = namespace
        self.greenlet = None
        self.hard_failures: dict[str, int] = {}
        self.path = os.path.join(_extra_dir(), "node_ts.csv")
        self._restarts: dict[str, int] = {}
        self.latest_cpu_cores: dict[str, float] = {}

    def start(self) -> None:
        try:
            from kubernetes import client, config  # noqa: F401

            config.load_incluster_config()
            self.core = client.CoreV1Api()
            self.custom = client.CustomObjectsApi()
        except Exception as e:
            logger.warning("node watcher disabled: %s", e)
            return
        with open(self.path, "w") as f:
            f.write("ts,node,node_cpu_mcores,node_mem_bytes,"
                    "workers_ready,worker_restarts,oomkilled\n")
        self.greenlet = gevent.spawn(self._loop)

    def stop(self) -> None:
        if self.greenlet is not None:
            self.greenlet.kill(block=False)

    def _bump(self, key: str, count: int = 1) -> None:
        self.hard_failures[key] = self.hard_failures.get(key, 0) + count

    def _sample(self) -> None:
        nodes = self.core.list_node(label_selector=self.node_label).items
        node_rows = []
        for node in nodes:
            name = node.metadata.name
            cpu_m, mem_b = "", ""
            try:
                metrics = self.custom.get_cluster_custom_object(
                    "metrics.k8s.io", "v1beta1", "nodes", name
                )
                cores = _parse_cpu(metrics['usage']['cpu']) / 1000.0
                self.latest_cpu_cores[name] = cores
                cpu_m = f"{cores * 1000:.0f}"
                mem_b = f"{_parse_mem(metrics['usage']['memory']):.0f}"
            except Exception as e:
                logger.debug("node metrics: %s", e)
            node_rows.append((name, cpu_m, mem_b))
        if not node_rows:
            node_rows = [("", "", "")]

        ready = restarts = oom = 0
        pods = self.core.list_namespaced_pod(self.namespace).items
        for pod in pods:
            for cond in pod.status.conditions or []:
                if cond.type == "Ready" and cond.status == "True":
                    ready += 1
            for cs in pod.status.container_statuses or []:
                restarts += cs.restart_count
                prev = self._restarts.get(pod.metadata.name)
                if prev is not None and cs.restart_count > prev:
                    term = cs.last_state.terminated
                    reason = term.reason if term else "unknown"
                    key = ("worker_oomkilled" if reason == "OOMKilled"
                           else "worker_restart")
                    self._bump(key, cs.restart_count - prev)
                    if reason == "OOMKilled":
                        oom += cs.restart_count - prev
                self._restarts[pod.metadata.name] = cs.restart_count

        with open(self.path, "a") as f:
            for name, cpu_m, mem_b in node_rows:
                f.write(f"{time.time():.0f},{name},{cpu_m},{mem_b},"
                        f"{ready},{restarts},{oom}\n")

    def _loop(self) -> None:
        while True:
            try:
                self._sample()
            except Exception as e:
                logger.warning("node watcher sample failed: %s", e)
            gevent.sleep(10)


def _write_summary(environment) -> None:
    opts = environment.parsed_options

    def aggregate(phase: str) -> dict:
        per_band: dict = {}
        for r in _records:
            if r["phase"] != phase:
                continue
            point = per_band.setdefault(r["band_id"], {"n": r["n"], "ops": {}})
            names = [r["op"]]
            # Rules B and C's population, reported beside the full cohort.
            if (r.get("cohort") == "staggered"
                    and r.get("offset_s", 0.0) >= opts.calm_start_seconds):
                if r["op"] == "resume_staggered":
                    names.append("resume_staggered_calm")
                elif r["op"] == "suspend":
                    names.append("suspend_staggered_calm")
            for name in names:
                op = point["ops"].setdefault(name, {"ok": [], "failed": 0})
                if r["ok"]:
                    op["ok"].append(r["elapsed_s"])
                else:
                    op["failed"] += 1
        return {
            str(bid): {
                "n": p["n"],
                **{op: {**percentiles(d["ok"]), "failed": d["failed"]}
                   for op, d in p["ops"].items()},
            }
            for bid, p in sorted(per_band.items())
        }

    watcher = _state.get("watcher")
    ladder_summary = ladder.summary()
    # Backward-compatible "points" view keyed by n (measured bands only;
    # a probe that repeats an n overwrites the earlier entry).
    points: dict = {}
    for bid, entry in aggregate("band").items():
        n = entry.pop("n")
        points[str(n)] = entry
    summary = {
        "config": {
            "cycle_seconds": opts.cycle_seconds,
            "active_dwell_seconds": opts.active_dwell_seconds,
            "herd_fraction": opts.herd_fraction,
            "stagger_window": opts.stagger_window,
            "points": opts.points,
            "workers": opts.workers,
            "cycles_per_point": opts.cycles_per_point,
            "bisect": opts.bisect,
        },
        "hard_failures": watcher.hard_failures if watcher else {},
        "bands": ladder_summary["bands"],
        "knee": ladder_summary["knee"],
        "points": points,
        "warmup_by_band": aggregate("warmup"),
    }
    extra = _extra_dir()
    with open(os.path.join(extra, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    # The measured inputs of the capacity math model, for re-running it
    # with substrate-real numbers. Snapshot object size is not visible
    # from here; read it off the GCS snapshot location after the run.
    ops_all: dict[str, list[float]] = {}
    for r in _records:
        if r["ok"] and r["phase"] == "band":
            ops_all.setdefault(r["op"], []).append(r["elapsed_s"])
    with open(os.path.join(extra, "model_params.json"), "w") as f:
        json.dump({
            "workers": opts.workers,
            "cycle_seconds": opts.cycle_seconds,
            "active_dwell_seconds": opts.active_dwell_seconds,
            "herd_fraction": opts.herd_fraction,
            "stagger_window": opts.stagger_window,
            "op_durations": {
                op: percentiles(v) for op, v in sorted(ops_all.items())
            },
            "snapshot_size_bytes": None,
        }, f, indent=2)
    logger.info("Wrote summary.json / model_params.json to %s", extra)


@events.test_start.add_listener
def _(environment, **kwargs) -> None:
    if isinstance(environment.runner, WorkerRunner):
        return
    opts = environment.parsed_options
    parse_points(opts.points, opts.workers)  # fail fast on a bad ladder
    _install_hooks(environment)
    if isinstance(environment.runner, (LocalRunner, MasterRunner)):
        watcher = _Watcher(opts.density_node_label, opts.workload_namespace)
        watcher.start()
        _state["watcher"] = watcher


def _sweep_actors(environment) -> None:
    """Delete every actor left in the atespace. on_stop only covers users
    alive at the end; actors parked by bisection shrinks are not."""
    opts = environment.parsed_options
    if opts.delete_actors_on_stop.lower() != "true":
        return
    try:
        stub = _state["pool_stub"] or ateapi_pb2_grpc.ControlStub(
            ateapi_channel(OpenClawUser.host)
        )
        token = ""
        names = []
        while True:
            resp = stub.ListActors(ateapi_pb2.ListActorsRequest(
                atespace=opts.openclaw_atespace, page_size=1000, page_token=token,
            ))
            names += [a.metadata.name for a in resp.actors]
            token = resp.next_page_token
            if not token:
                break
        failed = 0
        for name in names:
            try:
                stub.DeleteActor(ateapi_pb2.DeleteActorRequest(
                    actor=ateapi_pb2.ObjectRef(atespace=opts.openclaw_atespace, name=name),
                    any_state=True,
                ))
            except grpc.RpcError as e:
                if e.code() != grpc.StatusCode.NOT_FOUND:
                    failed += 1
        logger.info("final sweep: %d actors listed, %d delete failures", len(names), failed)
    except Exception as e:
        logger.warning("final actor sweep failed: %s", e)


@events.test_stop.add_listener
def _(environment, **kwargs) -> None:
    if isinstance(environment.runner, WorkerRunner):
        return
    watcher = _state.get("watcher")
    if watcher is not None:
        watcher.stop()
    if _records:
        _write_summary(environment)
    _sweep_actors(environment)
