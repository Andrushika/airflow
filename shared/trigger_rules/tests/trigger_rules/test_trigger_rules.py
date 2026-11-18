# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import pytest

from airflow_shared.trigger_rules import UpstreamStates, evaluate_direct_upstreams, evaluate_setup_constraint


def _states(success=0, skipped=0, failed=0, upstream_failed=0) -> UpstreamStates:
    done = success + skipped + failed + upstream_failed
    return UpstreamStates(success, skipped, failed, upstream_failed, 0, done, 0, 0)


@pytest.mark.parametrize(
    ("trigger_rule", "states", "new_state", "satisfied"),
    [
        pytest.param("all_success", _states(success=2), None, True, id="all-success-met"),
        pytest.param(
            "all_success", _states(success=1, failed=1), "upstream_failed", False, id="all-success-failed"
        ),
        pytest.param(
            "all_success", _states(success=1, skipped=1), "skipped", False, id="all-success-skipped"
        ),
        pytest.param("one_failed", _states(success=2), "skipped", False, id="one-failed-none"),
        pytest.param(
            "none_failed_min_one_success", _states(skipped=2), "skipped", False, id="nfmos-all-skipped"
        ),
        pytest.param("all_done", _states(success=1), None, False, id="all-done-waiting"),
    ],
)
def test_evaluate_direct_upstreams(trigger_rule, states, new_state, satisfied):
    decision = evaluate_direct_upstreams(
        trigger_rule=trigger_rule,
        states=states,
        upstream=2,
        upstream_setup=0,
        upstream_task_ids={"a", "b"},
        map_index=-1,
        flag_upstream_failed=True,
    )
    assert decision.new_state == new_state
    assert (decision.failure_reason is None) is satisfied


def test_evaluate_setup_constraint_marks_upstream_failed():
    decision = evaluate_setup_constraint(
        states=_states(failed=1),
        upstream=1,
        relevant_setups={"setup": object()},
        upstream_task_ids={"work"},
        map_index=-1,
        flag_upstream_failed=True,
    )
    assert decision.new_state == "upstream_failed"
    assert decision.failure_reason is not None
