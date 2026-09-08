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

"""Clean-room band ladder for the OpenClaw density test.

Runs each --points actor count as its own band, smallest first, with a
verified pause between bands: the pause adds and warms the next band's
actors, then waits until every actor is suspended (every worker free)
before starting the next band's clock. No band ever sees another band's
traffic. With --bisect true, after the ladder the shape keeps probing the
midpoint between the last passing and first failing band until the knee
is pinned to --bisect-resolution actors.

The state machine lives in common/openclaw_ladder.py; this file is the
thin locust adapter. Load it with the test:

    file: /app/tests/openclaw_cycle.py,/app/shapes/openclaw_density.py

The shape owns the run's duration; locust ignores -t when a shape is
present, so the run ends when the ladder (and bisection) completes.
"""

import logging

from locust import LoadTestShape

from common.openclaw_ladder import LadderConfig, ladder, wall_clock

logger = logging.getLogger(__name__)


class OpenClawDensityShape(LoadTestShape):
    _configured = False

    def tick(self):
        env = self.runner.environment
        if not self._configured:
            ladder.configure(LadderConfig.from_options(env.parsed_options), wall_clock())
            self._configured = True
        # The test module registers these on the environment at test_start;
        # until then (the very first ticks) the pause simply waits.
        hooks = getattr(env, "openclaw_hooks", None)
        if hooks is None:
            return (ladder.target, 50.0)
        return ladder.tick(
            wall_clock(),
            hooks["alive"](),
            hooks["all_warmed"](),
            hooks["pool_clean"],
            hooks["verdict"],
            hooks["last_warm_activity"](),
            hooks["node_cpu"],
        )
