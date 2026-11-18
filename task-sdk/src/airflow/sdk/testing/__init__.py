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
Run one task for real in a test, without a metadata database.

.. note:: |experimental|
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

import attrs
from uuid6 import uuid7

from airflow.sdk import timezone
from airflow.sdk.api.datamodels._generated import (
    AssetEventDagRunReference,
    AssetReferenceAssetEventDagRun,
    ConnectionResponse,
    DagRun,
    DagRunState,
    DagRunType,
    TaskInstance,
    TaskInstanceState,
)
from airflow.sdk.definitions._internal.types import NOTSET, ArgNotSet
from airflow.sdk.definitions.xcom_arg import PlainXComArg, XComArg
from airflow.sdk.execution_time.comms import DeferTask, SucceedTask, XComResult
from airflow.sdk.execution_time.supervisor import run_task_in_process
from airflow.sdk.execution_time.xcom import XCom
from airflow.sdk.module_loading import import_string
from airflow.sdk.serde import deserialize
from airflow.sdk.testing._execution_api import InMemoryExecutionAPI

if TYPE_CHECKING:
    from airflow.sdk import Asset, BaseOperator, Connection
    from airflow.sdk.definitions.mappedoperator import MappedOperator
    from airflow.triggers.base import BaseTrigger  # noqa: SDK002

__all__ = ["TaskRunResult", "run_task"]

_RETURN_KEY = "return_value"


@attrs.define(frozen=True)
class TaskRunResult:
    """
    What happened when :func:`run_task` ran a task.

    .. note:: |experimental|

    :param state: The state the task instance ended in.
    :param return_value: The return value the task stored as an XCom, if any.
    :param xcoms: Every XCom the task instance stored, by key.
    :param error: The exception the task raised, if any.
    :param skipped_downstream: The downstream tasks the task asked to skip, as task ids, or
        ``(task_id, map_index)`` pairs for mapped task instances.
    :param outlet_events: The asset events the task emitted, as the Execution API received them.
    :param trigger: The trigger the task deferred to, if it deferred.
    :param next_method: The method the task resumes with after the trigger fires.
    :param next_kwargs: The keyword arguments the task resumes with.
    """

    state: TaskInstanceState
    return_value: Any = None
    xcoms: dict[str, Any] = attrs.field(factory=dict)
    error: BaseException | None = None
    skipped_downstream: list[str | tuple[str, int]] = attrs.field(factory=list)
    outlet_events: list[dict[str, Any]] = attrs.field(factory=list)
    trigger: BaseTrigger | None = None
    next_method: str | None = None
    next_kwargs: dict[str, Any] | None = None


def run_task(
    task: BaseOperator | MappedOperator | XComArg,
    *,
    upstream: Mapping[str, Any] | None = None,
    mapped_upstream: Mapping[str, Sequence[Any]] | None = None,
    variables: Mapping[str, str] | None = None,
    connections: Iterable[Connection] = (),
    conf: Mapping[str, Any] | None = None,
    logical_date: datetime | None | ArgNotSet = NOTSET,
    map_index: int = -1,
    try_number: int = 1,
    triggering_asset_events: Mapping[Asset, Sequence[Mapping[str, Any]]] | None = None,
) -> TaskRunResult:
    """
    Run one task instance through the real Task SDK runner, answering its requests in memory.

    The task renders its templates, pulls XComs, reads Variables and Connections and stores its
    results as it does in a Dag run, but no metadata database, scheduler or API server is
    involved. A request the in-memory API does not support raises :class:`NotImplementedError`.

    .. note:: |experimental|

    :param task: The operator, mapped operator, or the result of calling a ``@task`` function.
    :param upstream: Return values of upstream tasks, by task id.
    :param mapped_upstream: Return values of mapped upstream tasks, one per map index, by task id.
    :param variables: Variable values, by key.
    :param connections: Connections the task can look up by ``conn_id``.
    :param conf: The Dag run's ``conf``.
    :param logical_date: The run's logical date. ``None`` is a run without one; if not given, now.
    :param map_index: The map index of the task instance to run.
    :param try_number: The attempt to run, which decides between ``up_for_retry`` and ``failed``.
    :param triggering_asset_events: The ``extra`` of each asset event that triggered the run.
    """
    if isinstance(task, PlainXComArg):
        operator = task.operator
    elif isinstance(task, XComArg):
        raise TypeError(f"Pass the operator or the result of calling a @task function, not {task!r}")
    else:
        operator = task
    now = timezone.utcnow()
    if isinstance(logical_date, ArgNotSet):
        logical_date = now
    run_after = logical_date or now
    events = [
        AssetEventDagRunReference(
            asset=AssetReferenceAssetEventDagRun(name=asset.name, uri=asset.uri, extra=asset.extra),
            extra=dict(extra),
            source_task_id=None,
            source_dag_id=None,
            source_run_id=None,
            source_map_index=None,
            source_aliases=[],
            timestamp=now,
        )
        for asset, extras in (triggering_asset_events or {}).items()
        for extra in extras
    ]
    dag_run = DagRun(
        dag_id=operator.dag_id,
        run_id=f"manual__{run_after.isoformat()}",
        logical_date=logical_date,
        data_interval_start=logical_date,
        data_interval_end=logical_date,
        run_after=run_after,
        start_date=now,
        end_date=None,
        run_type=DagRunType.ASSET_TRIGGERED if events else DagRunType.MANUAL,
        state=DagRunState.RUNNING,
        conf=dict(conf) if conf is not None else None,
        consumed_asset_events=events,
        partition_key=None,
    )
    max_tries = operator.retries or 0
    api = InMemoryExecutionAPI(dag_run, max_tries=max_tries, should_retry=try_number <= max_tries)
    _store_upstream_values(api, dag_run.run_id, upstream or {}, mapped_upstream or {})
    api.variables.update(variables or {})
    for conn in connections:
        api.connections[conn.conn_id] = ConnectionResponse.model_validate(attrs.asdict(conn))

    ti = TaskInstance(
        id=uuid7(),
        task_id=operator.task_id,
        dag_id=operator.dag_id,
        run_id=dag_run.run_id,
        try_number=try_number,
        map_index=map_index,
        dag_version_id=uuid7(),
    )
    outcome = run_task_in_process(ti=ti, task=operator, client=api.client())
    if api.unhandled:
        raise NotImplementedError(
            f"The in-memory Execution API does not support: {sorted(set(api.unhandled))}"
        )

    xcoms = {
        key: XCom.deserialize_value(XComResult(key=key, value=value))
        for key, stored in api.xcoms.items()
        for (task_id, index), value in stored.items()
        if task_id == operator.task_id and index == map_index
    }
    result = TaskRunResult(
        state=TaskInstanceState(outcome.state),
        return_value=xcoms.get(_RETURN_KEY),
        xcoms=xcoms,
        error=outcome.error,
        skipped_downstream=api.skipped_downstream,
    )
    if isinstance(outcome.msg, SucceedTask):
        return attrs.evolve(result, outlet_events=list(outcome.msg.outlet_events or []))
    if isinstance(outcome.msg, DeferTask):
        trigger_kwargs = cast("dict[str, Any]", deserialize(cast("dict", outcome.msg.trigger_kwargs or {})))
        return attrs.evolve(
            result,
            trigger=import_string(outcome.msg.classpath)(**trigger_kwargs),
            next_method=outcome.msg.next_method,
            next_kwargs=cast("dict[str, Any]", deserialize(outcome.msg.next_kwargs or {})),
        )
    return result


def _store_upstream_values(
    api: InMemoryExecutionAPI,
    run_id: str,
    upstream: Mapping[str, Any],
    mapped_upstream: Mapping[str, Sequence[Any]],
) -> None:
    stored = api.xcoms.setdefault(_RETURN_KEY, {})
    for task_id, value in upstream.items():
        stored[(task_id, -1)] = XCom.serialize_value(value, key=_RETURN_KEY, task_id=task_id, run_id=run_id)
    for task_id, values in mapped_upstream.items():
        for index, value in enumerate(values):
            stored[(task_id, index)] = XCom.serialize_value(
                value, key=_RETURN_KEY, task_id=task_id, run_id=run_id, map_index=index
            )
