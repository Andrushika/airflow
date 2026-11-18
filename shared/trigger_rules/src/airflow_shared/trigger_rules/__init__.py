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
"""
Decide what a task instance's trigger rule says, given counts of its upstream states.

Counting the upstream task instances is left to the caller; these functions only hold the
decision table, so the scheduler and a runner without a metadata database can share it.
States are returned as their string values.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import NamedTuple

UPSTREAM_FAILED = "upstream_failed"
SKIPPED = "skipped"
REMOVED = "removed"


class UpstreamStates(NamedTuple):
    """
    How many relevant upstream task instances are in each finished state.

    ``done`` counts every finished upstream. The ``*_setup`` counts are included in the plain counts.
    """

    success: int
    skipped: int
    failed: int
    upstream_failed: int
    removed: int
    done: int
    success_setup: int
    skipped_setup: int


class TriggerRuleDecision(NamedTuple):
    """
    The outcome of one evaluation.

    ``new_state`` is the state to set on the task instance, if any. ``failure_reason`` is set when the
    rule is not satisfied, whether or not a new state was chosen.
    """

    new_state: str | None
    failure_reason: str | None


def evaluate_setup_constraint(
    *,
    states: UpstreamStates,
    upstream: int,
    relevant_setups: Mapping[str, object],
    upstream_task_ids: Collection[str],
    map_index: int,
    flag_upstream_failed: bool,
) -> TriggerRuleDecision:
    """
    Evaluate the indirect setups of a task instance.

    :param states: Counts over the indirect setups only.
    :param upstream: How many indirect setup task instances are expected.
    :param relevant_setups: All relevant setups, shown in the failure reason.
    :param upstream_task_ids: The task's direct upstream task ids, shown in the failure reason.
    :param map_index: The task instance's map index.
    :param flag_upstream_failed: Whether to choose a new state.
    """
    new_state = None
    if flag_upstream_failed:
        if states.upstream_failed or states.failed:
            new_state = UPSTREAM_FAILED
        elif states.skipped:
            new_state = SKIPPED
        elif states.removed and states.success and map_index > -1:
            if map_index >= states.success:
                new_state = REMOVED

    failure_reason = None
    non_successes = upstream - states.success
    if map_index > -1:
        non_successes -= states.removed
    if non_successes > 0:
        failure_reason = (
            f"All setup tasks must complete successfully. Relevant setups: {relevant_setups}: "
            f"upstream_states={states}, "
            f"upstream_task_ids={upstream_task_ids}"
        )
    return TriggerRuleDecision(new_state, failure_reason)


def evaluate_direct_upstreams(
    *,
    trigger_rule: str,
    states: UpstreamStates,
    upstream: int,
    upstream_setup: int,
    upstream_task_ids: Collection[str],
    map_index: int,
    flag_upstream_failed: bool,
) -> TriggerRuleDecision:
    """
    Evaluate a task instance's trigger rule against its direct upstreams.

    :param trigger_rule: The task's trigger rule. Any ``str`` enum member or plain string works.
    :param states: Counts over the relevant direct upstream task instances.
    :param upstream: How many relevant direct upstream task instances are expected.
    :param upstream_setup: How many of those are setups.
    :param upstream_task_ids: The task's direct upstream task ids.
    :param map_index: The task instance's map index.
    :param flag_upstream_failed: Whether to choose a new state.
    """
    success = states.success
    skipped = states.skipped
    failed = states.failed
    upstream_failed = states.upstream_failed
    removed = states.removed
    done = states.done
    success_setup = states.success_setup
    skipped_setup = states.skipped_setup
    trigger_rule_str = getattr(trigger_rule, "value", trigger_rule)

    upstream_done = done >= upstream

    new_state = None
    if flag_upstream_failed:
        if trigger_rule == "all_success":
            if upstream_failed or failed:
                new_state = UPSTREAM_FAILED
            elif skipped:
                new_state = SKIPPED
            elif removed and success and map_index > -1:
                if map_index >= success:
                    new_state = REMOVED
        elif trigger_rule == "all_failed":
            if success or skipped:
                new_state = SKIPPED
        elif trigger_rule == "one_success":
            if upstream_done and done == skipped:
                # if upstream is done and all are skipped mark as skipped
                new_state = SKIPPED
            elif upstream_done and success <= 0:
                # if upstream is done and there are no success mark as upstream failed
                new_state = UPSTREAM_FAILED
        elif trigger_rule == "one_failed":
            if upstream_done and not (failed or upstream_failed):
                new_state = SKIPPED
        elif trigger_rule == "one_done":
            if upstream_done and not (failed or success):
                new_state = SKIPPED
        elif trigger_rule == "none_failed":
            if upstream_failed or failed:
                new_state = UPSTREAM_FAILED
        elif trigger_rule == "none_failed_min_one_success":
            if upstream_failed or failed:
                new_state = UPSTREAM_FAILED
            elif skipped == upstream:
                new_state = SKIPPED
            elif upstream_done and success == 0:
                new_state = UPSTREAM_FAILED
        elif trigger_rule == "none_skipped":
            if skipped:
                new_state = SKIPPED
        elif trigger_rule == "all_skipped":
            if success or failed or upstream_failed:
                new_state = SKIPPED
        elif trigger_rule == "all_done_min_one_success":
            # For this trigger rule, skipped tasks are not considered "done"
            non_skipped_done = success + failed + upstream_failed + removed
            non_skipped_upstream = upstream - skipped

            if skipped > 0:
                # There are skipped tasks, so not all tasks are "done" for this rule
                new_state = SKIPPED
            elif non_skipped_done >= non_skipped_upstream and success == 0:
                # All non-skipped tasks are done but no successes
                new_state = UPSTREAM_FAILED
        elif trigger_rule == "all_done_setup_success":
            if upstream_done and upstream_setup and skipped_setup >= upstream_setup:
                # when there is an upstream setup and they have all skipped, then skip
                new_state = SKIPPED
            elif upstream_done and upstream_setup and success_setup == 0:
                # when there is an upstream setup, if none succeeded, mark upstream failed
                # if at least one setup ran, we'll let it run
                new_state = UPSTREAM_FAILED

    failure_reason = None
    if trigger_rule == "one_success":
        if success <= 0:
            failure_reason = (
                f"Task's trigger rule '{trigger_rule_str}' requires one upstream task success, "
                f"but none were found. upstream_states={states}, "
                f"upstream_task_ids={upstream_task_ids}"
            )
    elif trigger_rule == "one_failed":
        if not failed and not upstream_failed:
            failure_reason = (
                f"Task's trigger rule '{trigger_rule_str}' requires one upstream task failure, "
                f"but none were found. upstream_states={states}, "
                f"upstream_task_ids={upstream_task_ids}"
            )
    elif trigger_rule == "one_done":
        if success + failed <= 0:
            failure_reason = (
                f"Task's trigger rule '{trigger_rule_str}' "
                "requires at least one upstream task failure or success "
                f"but none were failed or success. upstream_states={states}, "
                f"upstream_task_ids={upstream_task_ids}"
            )
    elif trigger_rule == "all_success":
        num_failures = upstream - success - removed
        if num_failures > 0:
            failure_reason = (
                f"Task's trigger rule '{trigger_rule_str}' requires all upstream tasks to have "
                f"succeeded, but found {num_failures} non-success(es). "
                f"upstream_states={states}, "
                f"upstream_task_ids={upstream_task_ids}"
            )
    elif trigger_rule == "all_failed":
        num_success = upstream - failed - upstream_failed - removed
        if num_success > 0:
            failure_reason = (
                f"Task's trigger rule '{trigger_rule_str}' requires all upstream tasks "
                f"to have failed, but found {num_success} non-failure(s). "
                f"upstream_states={states}, "
                f"upstream_task_ids={upstream_task_ids}"
            )
    elif trigger_rule == "all_done":
        if not upstream_done:
            failure_reason = (
                f"Task's trigger rule '{trigger_rule_str}' requires all upstream tasks to have "
                f"completed, but found {len(upstream_task_ids) - done} task(s) that were "
                f"not done. upstream_states={states}, "
                f"upstream_task_ids={upstream_task_ids}"
            )
    elif trigger_rule == "none_failed":
        num_failures = upstream - success - skipped - removed
        if num_failures > 0:
            failure_reason = (
                f"Task's trigger rule '{trigger_rule_str}' requires all upstream tasks to have "
                f"succeeded or been skipped, but found {num_failures} non-success(es). "
                f"upstream_states={states}, "
                f"upstream_task_ids={upstream_task_ids}"
            )
    elif trigger_rule == "none_failed_min_one_success":
        num_failures = upstream - success - skipped - removed
        if num_failures > 0:
            failure_reason = (
                f"Task's trigger rule '{trigger_rule_str}' requires all upstream tasks to have "
                f"succeeded or been skipped, but found {num_failures} non-success(es). "
                f"upstream_states={states}, "
                f"upstream_task_ids={upstream_task_ids}"
            )
        elif success <= 0:
            failure_reason = (
                f"Task's trigger rule '{trigger_rule_str}' requires at least one upstream task "
                f"success, but none were found. upstream_states={states}, "
                f"upstream_task_ids={upstream_task_ids}"
            )
    elif trigger_rule == "none_skipped":
        if not upstream_done or (skipped > 0):
            failure_reason = (
                f"Task's trigger rule '{trigger_rule_str}' requires all upstream tasks to not "
                f"have been skipped, but found {skipped} task(s) skipped. "
                f"upstream_states={states}, "
                f"upstream_task_ids={upstream_task_ids}"
            )
    elif trigger_rule == "all_skipped":
        num_non_skipped = upstream - skipped
        if num_non_skipped > 0:
            failure_reason = (
                f"Task's trigger rule '{trigger_rule_str}' requires all upstream tasks to have been "
                f"skipped, but found {num_non_skipped} task(s) in non skipped state. "
                f"upstream_states={states}, "
                f"upstream_task_ids={upstream_task_ids}"
            )
    elif trigger_rule == "all_done_setup_success":
        if not upstream_done:
            failure_reason = (
                f"Task's trigger rule '{trigger_rule_str}' requires all upstream tasks to have "
                f"completed, but found {len(upstream_task_ids) - done} task(s) that were not done. "
                f"upstream_states={states}, "
                f"upstream_task_ids={upstream_task_ids}"
            )
        elif upstream_setup and not success_setup:
            failure_reason = (
                f"Task's trigger rule '{trigger_rule_str}' requires at least one upstream setup task "
                f"be successful, but found {upstream_setup - success_setup} task(s) that were "
                f"not. upstream_states={states}, "
                f"upstream_task_ids={upstream_task_ids}"
            )
    elif trigger_rule == "all_done_min_one_success":
        # For this trigger rule, skipped tasks are not considered "done"
        non_skipped_done = success + failed + upstream_failed
        non_skipped_upstream = upstream - skipped - removed

        if skipped > 0:
            failure_reason = (
                f"Task's trigger rule '{trigger_rule_str}' requires all non-skipped upstream tasks to have "
                f"completed, but found {skipped} skipped task(s). "
                f"upstream_states={states}, "
                f"upstream_task_ids={upstream_task_ids}"
            )
        elif non_skipped_done < non_skipped_upstream:
            failure_reason = (
                f"Task's trigger rule '{trigger_rule_str}' requires all non-skipped upstream tasks to have "
                f"completed, but found {non_skipped_upstream - non_skipped_done} task(s) that were not done. "
                f"upstream_states={states}, "
                f"upstream_task_ids={upstream_task_ids}"
            )
        elif success == 0:
            failure_reason = (
                f"Task's trigger rule '{trigger_rule_str}' requires all non-skipped upstream tasks to have "
                f"completed and at least one upstream task has succeeded, but found "
                f"{success} successful task(s). upstream_states={states}, "
                f"upstream_task_ids={upstream_task_ids}"
            )
    else:
        failure_reason = f"No strategy to evaluate trigger rule '{trigger_rule_str}'."
    return TriggerRuleDecision(new_state, failure_reason)
