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
Run a whole Dag without a metadata database and without the scheduler.

Every task runs through the real Task SDK runner against one shared in-memory Execution API.
What the scheduler normally decides from database rows is decided here from a dictionary:
which task instances exist, whether their trigger rule lets them run, and the final run state.
"""

from __future__ import annotations

import asyncio
import math
import time
from typing import Any

from airflow.sdk.definitions._internal.types import NOTSET
from airflow.sdk.execution_time.comms import DeferTask
from airflow.sdk.execution_time.supervisor import run_task_in_process
from airflow.sdk.module_loading import import_string
from airflow.sdk.serde import deserialize, serialize
from testing_prototype.dag_runner import DagResult, TaskOutcome
from testing_prototype.fake_api import InMemoryExecutionAPI
from testing_prototype.task_runner import RETURN_KEY, _ti_model

TIKey = tuple[str, int]
FINISHED = {"success", "failed", "skipped", "upstream_failed"}
MAX_ATTEMPTS = 50


def _rule(op: Any) -> str:
    return str(getattr(op.trigger_rule, "value", op.trigger_rule))


def decide(rule: str, states: list[str | None], setup_states: list[str | None]) -> str:
    """
    Port of the scheduler's trigger-rule evaluation.

    ``states`` holds one entry per upstream task instance; ``None`` means not finished yet.
    Returns ``run``, ``skipped``, ``upstream_failed`` or ``wait``.
    """
    upstream = len(states)
    success = states.count("success")
    skipped = states.count("skipped")
    failed = states.count("failed")
    upstream_failed = states.count("upstream_failed")
    done = success + skipped + failed + upstream_failed
    upstream_done = done >= upstream
    upstream_setup = len(setup_states)
    success_setup = setup_states.count("success")
    skipped_setup = setup_states.count("skipped")
    non_skipped_done = success + failed + upstream_failed
    non_skipped_upstream = upstream - skipped

    new_state = None
    if rule == "all_success":
        if upstream_failed or failed:
            new_state = "upstream_failed"
        elif skipped:
            new_state = "skipped"
    elif rule == "all_failed":
        if success or skipped:
            new_state = "skipped"
    elif rule == "one_success":
        if upstream_done and done == skipped:
            new_state = "skipped"
        elif upstream_done and success <= 0:
            new_state = "upstream_failed"
    elif rule == "one_failed":
        if upstream_done and not (failed or upstream_failed):
            new_state = "skipped"
    elif rule == "one_done":
        if upstream_done and not (failed or success):
            new_state = "skipped"
    elif rule == "none_failed":
        if upstream_failed or failed:
            new_state = "upstream_failed"
    elif rule == "none_failed_min_one_success":
        if upstream_failed or failed:
            new_state = "upstream_failed"
        elif skipped == upstream:
            new_state = "skipped"
        elif upstream_done and success == 0:
            new_state = "upstream_failed"
    elif rule == "none_skipped":
        if skipped:
            new_state = "skipped"
    elif rule == "all_skipped":
        if success or failed or upstream_failed:
            new_state = "skipped"
    elif rule == "all_done_min_one_success":
        if skipped > 0:
            new_state = "skipped"
        elif non_skipped_done >= non_skipped_upstream and success == 0:
            new_state = "upstream_failed"
    elif rule == "all_done_setup_success":
        if upstream_done and upstream_setup and skipped_setup >= upstream_setup:
            new_state = "skipped"
        elif upstream_done and upstream_setup and success_setup == 0:
            new_state = "upstream_failed"
    if new_state:
        return new_state

    not_yet = {
        "one_success": success <= 0,
        "one_failed": not failed and not upstream_failed,
        "one_done": success + failed <= 0,
        "all_success": upstream - success > 0,
        "all_failed": upstream - failed - upstream_failed > 0,
        "all_done": not upstream_done,
        "none_failed": upstream - success - skipped > 0,
        "none_failed_min_one_success": upstream - success - skipped > 0 or success <= 0,
        "none_skipped": not upstream_done or skipped > 0,
        "all_skipped": upstream - skipped > 0,
        "all_done_setup_success": not upstream_done or bool(upstream_setup and not success_setup),
        "all_done_min_one_success": skipped > 0 or non_skipped_done < non_skipped_upstream or success == 0,
    }
    return "wait" if not_yet.get(rule, False) else "run"


class _Run:
    """
    One Dag run held in a dictionary.

    Mirrors the scheduler loop: every round takes a snapshot of the task instance states, decides
    from that snapshot which instances change state or become runnable, then runs the runnable ones.
    """

    def __init__(self, dag: Any, api: InMemoryExecutionAPI):
        self.dag = dag
        self.api = api
        self.states: dict[TIKey, str | None] = {(op.task_id, -1): None for op in dag.tasks}
        self.tries: dict[TIKey, int] = {}
        self.errors: dict[TIKey, BaseException] = {}
        self.order: list[TIKey] = []
        self.expanded: set[str] = set()
        self.batch: set[TIKey] = set()

    def _is_mapped(self, op: Any) -> bool:
        return hasattr(op, "_get_specified_expand_input")

    def _states_of(self, snapshot: dict[TIKey, str | None], task_ids: Any) -> tuple[list, list]:
        states: list[str | None] = []
        setup_states: list[str | None] = []
        for task_id in sorted(task_ids):
            found = [state for key, state in snapshot.items() if key[0] == task_id]
            states += found
            if self.dag.get_task(task_id).is_setup:
                setup_states += found
        return states, setup_states

    def _xcom(self, task_id: str, key: str = RETURN_KEY) -> Any:
        prefix = (self.dag.dag_id, self.api.run_id, task_id, key)
        indexes = sorted(k[4] for k in self.api.xcoms if k[:4] == prefix and k[4] >= 0)
        if indexes:
            return [deserialize(self.api.xcoms[(*prefix, index)]) for index in indexes]
        return deserialize(self.api.xcoms[(*prefix, -1)])

    def _length(self, value: Any) -> int:
        kind = type(value).__name__
        if kind == "MapXComArg":
            return self._length(value.arg)
        if kind == "ZipXComArg":
            sizes = [self._length(arg) for arg in value.args]
            return min(sizes) if isinstance(value.fillvalue, type(NOTSET)) else max(sizes)
        if kind == "ConcatXComArg":
            return sum(self._length(arg) for arg in value.args)
        if hasattr(value, "operator") and hasattr(value, "key"):
            return len(self._xcom(value.operator.task_id, value.key))
        if isinstance(value, (list, tuple, dict)):
            return len(value)
        raise NotImplementedError(f"cannot size mapped argument of type {type(value).__name__}")

    def _map_length(self, op: Any) -> int:
        value = op._get_specified_expand_input().value
        if isinstance(value, dict):
            return math.prod(self._length(item) for item in value.values())
        return self._length(value)

    def _fire_trigger(self, msg: DeferTask) -> Any:
        trigger = import_string(msg.classpath)(**deserialize(msg.trigger_kwargs))

        async def first_event() -> Any:
            async for event in trigger.run():
                return event.payload
            return None

        return asyncio.run(asyncio.wait_for(first_event(), timeout=30))

    def _execute(self, key: TIKey) -> None:
        op = self.dag.get_task(key[0])
        try_number, reschedules, resume = 1, 0, None
        max_tries = int(op.retries or 0)
        for _ in range(MAX_ATTEMPTS):
            self.api.max_tries = max_tries
            self.api.should_retry = try_number <= max_tries
            self.api.task_reschedule_count = reschedules
            self.api.next_method = resume[0] if resume else None
            self.api.next_kwargs = serialize(resume[1]) if resume else None
            before = len(self.api.skipped_downstream)
            outcome = run_task_in_process(
                ti=_ti_model(op, self.api, try_number=try_number, map_index=key[1]),
                task=op,
                client=self.api.client(),
            )
            state = str(getattr(outcome.state, "value", outcome.state))
            for entry in self.api.skipped_downstream[before:]:
                self._mark_skipped(entry)
            if state == "up_for_retry":
                try_number += 1
                resume = None
            elif state == "up_for_reschedule":
                reschedules += 1
            elif state == "deferred" and isinstance(outcome.msg, DeferTask):
                kwargs = dict(deserialize(outcome.msg.next_kwargs or {}))  # type: ignore[arg-type]
                kwargs["event"] = self._fire_trigger(outcome.msg)
                resume = (outcome.msg.next_method, kwargs)
            else:
                self.states[key] = state
                self.tries[key] = try_number
                if outcome.error:
                    self.errors[key] = outcome.error
                self.order.append(key)
                return
        raise RuntimeError(f"{key} did not reach a final state in {MAX_ATTEMPTS} attempts")

    def _mark_skipped(self, entry: Any) -> None:
        key = (entry, -1) if isinstance(entry, str) else (entry[0], -1 if entry[1] is None else entry[1])
        # An instance already picked for this round still runs, as it does in the scheduler loop.
        if key in self.states and self.states[key] is None and key not in self.batch:
            self.states[key] = "skipped"

    def _round(self) -> bool:
        snapshot = dict(self.states)
        selected: list[TIKey] = []
        changed = False
        for op in self.dag.topological_sort():
            for key in [k for k, state in snapshot.items() if k[0] == op.task_id and state is None]:
                states, setup_states = self._states_of(snapshot, op.upstream_task_ids)
                verdict = decide(_rule(op), states, setup_states) if states else "run"
                if verdict == "run" and op.is_teardown:
                    scope, _ = self._states_of(snapshot, op.get_flat_relative_ids(upstream=True))
                    if any(state is None for state in scope):
                        verdict = "wait"
                if verdict == "wait":
                    continue
                changed = True
                if verdict != "run":
                    self.states[key] = verdict
                elif self._is_mapped(op) and op.task_id not in self.expanded:
                    self.expanded.add(op.task_id)
                    length = self._map_length(op)
                    if length == 0:
                        self.states[key] = "skipped"
                    else:
                        del self.states[key]
                        for index in range(length):
                            self.states[(op.task_id, index)] = None
                            selected.append((op.task_id, index))
                else:
                    selected.append(key)
        self.batch = set(selected)
        for key in selected:
            self._execute(key)
        self.batch = set()
        return changed

    def run(self) -> str:
        while self._round():
            pass
        return self._final_state()

    def _final_state(self) -> str:
        dag = self.dag

        def is_effective_leaf(op: Any) -> bool:
            for task_id in op.downstream_task_ids:
                down = dag.get_task(task_id)
                if not down.is_teardown or down.on_failure_fail_dagrun:
                    return False
            return not op.is_teardown or op.on_failure_fail_dagrun

        leaf_ids = {op.task_id for op in dag.tasks if is_effective_leaf(op)}
        if not leaf_ids:
            leaf_ids = {op.task_id for op in dag.tasks if not op.downstream_task_ids}
        if any(state is None for state in self.states.values()):
            return "failed"
        leaf_states = [state for key, state in self.states.items() if key[0] in leaf_ids]
        if any(state in ("failed", "upstream_failed") for state in leaf_states):
            return "failed"
        return "success"


def _name(key: TIKey) -> str:
    return key[0] if key[1] < 0 else f"{key[0]}[{key[1]}]"


def run_dag_in_memory(
    dag: Any,
    *,
    logical_date: str | None = "2026-01-01T00:00:00Z",
    conf: dict[str, Any] | None = None,
    connections: dict[str, dict[str, Any]] | None = None,
    variables: dict[str, str] | None = None,
) -> DagResult:
    """Execute one run of ``dag`` in memory and return the same result shape as ``run_dag``."""
    api = InMemoryExecutionAPI(dag_id=dag.dag_id, logical_date=logical_date, conf=conf)
    api.connections.update(connections or {})
    api.variables.update(variables or {})
    run = _Run(dag, api)
    started = time.monotonic()
    state = run.run()
    seconds = time.monotonic() - started

    tasks: dict[str, TaskOutcome] = {}
    for key, ti_state in run.states.items():
        if ti_state is None:
            continue
        pushed = {
            xcom_key[3]: deserialize(value)
            for xcom_key, value in api.xcoms.items()
            if xcom_key[2] == key[0] and xcom_key[4] == key[1]
        }
        tasks[_name(key)] = TaskOutcome(
            state=ti_state,
            return_value=pushed.get(RETURN_KEY),
            xcoms=pushed,
            try_number=run.tries.get(key, 0),
        )
    result = DagResult(state=state, tasks=tasks, order=[_name(key) for key in run.order], seconds=seconds)
    result.errors = {_name(key): error for key, error in run.errors.items()}  # type: ignore[attr-defined]
    result.unhandled = sorted(set(api.unhandled))  # type: ignore[attr-defined]
    return result
