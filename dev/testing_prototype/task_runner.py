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
"""Run or render a single task with no metadata database."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import psutil
import structlog

from airflow.sdk.api.datamodels._generated import TaskInstance
from airflow.sdk.execution_time.comms import DeferTask
from airflow.sdk.execution_time.supervisor import (
    InProcessSupervisorComms,
    InProcessTestSupervisor,
    run_task_in_process,
    set_supervisor_comms,
)
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.sdk.serde import deserialize, serialize
from testing_prototype.fake_api import InMemoryExecutionAPI

RETURN_KEY = "return_value"


@dataclass
class Deferral:
    """What a task asked for when it deferred."""

    trigger: str
    trigger_kwargs: Any
    method: str
    kwargs: dict[str, Any]


@dataclass
class TaskResult:
    """Outcome of one task attempt."""

    state: str
    return_value: Any = None
    xcoms: dict[str, Any] = field(default_factory=dict)
    error: BaseException | None = None
    deferral: Deferral | None = None
    skipped_downstream: list[Any] = field(default_factory=list)
    outlet_events: list[Any] = field(default_factory=list)
    rendered_fields: dict[str, Any] = field(default_factory=dict)
    api: InMemoryExecutionAPI | None = None


def _operator(task: Any) -> Any:
    """Accept an operator or the ``XComArg`` a ``@task`` call returns."""
    return getattr(task, "operator", task)


def _build_api(
    op: Any,
    *,
    upstream: dict[str, Any] | None,
    upstream_mapped: dict[str, list[Any]] | None,
    connections: dict[str, dict[str, Any]] | None,
    variables: dict[str, str] | None,
    try_number: int,
    logical_date: str | None,
    conf: dict[str, Any] | None,
    resume: tuple[str, dict[str, Any]] | None,
    reschedule_count: int,
) -> InMemoryExecutionAPI:
    max_tries = int(op.retries or 0)
    api = InMemoryExecutionAPI(
        dag_id=op.dag_id,
        logical_date=logical_date,
        conf=conf,
        max_tries=max_tries,
        should_retry=try_number <= max_tries,
        next_method=resume[0] if resume else None,
        next_kwargs=serialize(resume[1]) if resume else None,
        task_reschedule_count=reschedule_count,
    )
    for task_id, value in (upstream or {}).items():
        api.xcoms[(op.dag_id, api.run_id, task_id, RETURN_KEY, -1)] = serialize(value)
    for task_id, values in (upstream_mapped or {}).items():
        for index, value in enumerate(values):
            api.xcoms[(op.dag_id, api.run_id, task_id, RETURN_KEY, index)] = serialize(value)
    api.connections.update(connections or {})
    api.variables.update(variables or {})
    return api


def _ti_model(op: Any, api: InMemoryExecutionAPI, *, try_number: int, map_index: int) -> TaskInstance:
    return TaskInstance(
        id=uuid.uuid4(),
        task_id=op.task_id,
        dag_id=op.dag_id,
        run_id=api.run_id,
        try_number=try_number,
        map_index=map_index,
        dag_version_id=uuid.uuid4(),
    )


def run_task(
    task: Any,
    *,
    upstream: dict[str, Any] | None = None,
    upstream_mapped: dict[str, list[Any]] | None = None,
    connections: dict[str, dict[str, Any]] | None = None,
    variables: dict[str, str] | None = None,
    try_number: int = 1,
    map_index: int = -1,
    logical_date: str | None = "2026-01-01T00:00:00Z",
    conf: dict[str, Any] | None = None,
    resume: tuple[str, dict[str, Any]] | None = None,
    reschedule_count: int = 0,
) -> TaskResult:
    """
    Execute one attempt of ``task`` through the Task SDK runner against an in-memory API.

    :param upstream: return values of upstream tasks, keyed by task id.
    :param upstream_mapped: return values of mapped upstream tasks, one per map index.
    :param resume: ``(method_name, kwargs)`` to resume a deferred task, as the triggerer would.
    """
    op = _operator(task)
    api = _build_api(
        op,
        upstream=upstream,
        upstream_mapped=upstream_mapped,
        connections=connections,
        variables=variables,
        try_number=try_number,
        logical_date=logical_date,
        conf=conf,
        resume=resume,
        reschedule_count=reschedule_count,
    )
    ti = _ti_model(op, api, try_number=try_number, map_index=map_index)
    outcome = run_task_in_process(ti=ti, task=op, client=api.client())

    own = {
        key[3]: deserialize(value)
        for key, value in api.xcoms.items()
        if key[2] == op.task_id and key[4] == map_index
    }
    deferral = None
    if isinstance(outcome.msg, DeferTask):
        deferral = Deferral(
            trigger=outcome.msg.classpath,
            trigger_kwargs=outcome.msg.trigger_kwargs,
            method=outcome.msg.next_method,
            kwargs=deserialize(outcome.msg.next_kwargs or {}),  # type: ignore[arg-type]
        )
    return TaskResult(
        state=str(getattr(outcome.state, "value", outcome.state)),
        return_value=own.get(RETURN_KEY),
        xcoms=own,
        error=outcome.error,
        deferral=deferral,
        skipped_downstream=api.skipped_downstream,
        outlet_events=list(getattr(outcome.msg, "outlet_events", None) or []),
        rendered_fields=api.rendered_fields,
        api=api,
    )


def render_task(
    task: Any,
    *,
    upstream: dict[str, Any] | None = None,
    variables: dict[str, str] | None = None,
    logical_date: str | None = "2026-01-01T00:00:00Z",
    conf: dict[str, Any] | None = None,
) -> Any:
    """Return a copy of the operator with its template fields rendered, without executing it."""
    op = _operator(task)
    api = _build_api(
        op,
        upstream=upstream,
        upstream_mapped=None,
        connections=None,
        variables=variables,
        try_number=1,
        logical_date=logical_date,
        conf=conf,
        resume=None,
        reschedule_count=0,
    )
    model = _ti_model(op, api, try_number=1, map_index=-1)
    client = api.client()
    supervisor = InProcessTestSupervisor(
        id=model.id,
        pid=os.getpid(),
        process=psutil.Process(),
        process_log=structlog.get_logger(logger_name="task").bind(),
        client=client,
    )
    supervisor.comms = InProcessSupervisorComms(supervisor=supervisor)
    with set_supervisor_comms(supervisor.comms):
        supervisor.ti = model  # type: ignore[assignment]
        start = datetime.now(tz=UTC)
        ti_context = client.task_instances.start(model.id, os.getpid(), start)
        ti = RuntimeTaskInstance.model_construct(
            **model.model_dump(exclude_unset=True),
            task=op,
            _ti_context_from_server=ti_context,
            max_tries=ti_context.max_tries,
            start_date=start,
        )
        context = ti.get_template_context()
        ti.task = ti.task.prepare_for_execution()
        context["task"] = ti.task
        ti.render_templates(context=context, jinja_env=ti.task.dag.get_template_env())
        return ti.task
