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
"""Run a whole Dag through ``dag.test()`` on a throwaway SQLite database and return plain results."""

from __future__ import annotations

import signal
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy import select

from airflow.sdk.serde import deserialize

RETURN_KEY = "return_value"
TIMINGS: dict[str, float] = {}
_database_ready = False


@dataclass
class TaskOutcome:
    """Final state and pushed values of one task instance."""

    state: str | None
    return_value: Any = None
    xcoms: dict[str, Any] = field(default_factory=dict)
    try_number: int = 0


@dataclass
class DagResult:
    """Outcome of one Dag run, detached from the ORM session that produced it."""

    state: str
    tasks: dict[str, TaskOutcome]
    order: list[str]
    seconds: float

    @property
    def states(self) -> dict[str, str | None]:
        return {key: outcome.state for key, outcome in self.tasks.items()}


def ensure_database() -> None:
    """Create the metadata schema once per process, straight from the ORM models."""
    global _database_ready
    if _database_ready:
        return
    from airflow.utils import db

    started = time.monotonic()
    db.resetdb()
    TIMINGS["database setup"] = time.monotonic() - started
    _database_ready = True


def sync_dag(dag: Any, bundle_name: str = "testing") -> None:
    """Write an in-memory Dag to the database so a Dag run can be created for it."""
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.serialization.definitions.dag import SerializedDAG
    from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG
    from airflow.utils.session import create_session

    with create_session() as session:
        session.merge(DagBundleModel(name=bundle_name))
        session.flush()
        SerializedDAG.bulk_write_to_db(bundle_name, None, [dag], session=session)
        SerializedDagModel.write_dag(
            LazyDeserializedDAG(data=DagSerialization.to_dict(dag)), bundle_name, None, session=session
        )


@contextmanager
def _time_limit(seconds: int) -> Iterator[None]:
    def _expired(signum: int, frame: Any) -> None:
        raise TimeoutError(f"Dag run did not finish within {seconds}s")

    previous = signal.signal(signal.SIGALRM, _expired)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def _key(task_id: str, map_index: int) -> str:
    return task_id if map_index < 0 else f"{task_id}[{map_index}]"


def run_dag(
    dag: Any,
    *,
    logical_date: datetime | None = None,
    conf: dict[str, Any] | None = None,
    timeout: int = 60,
) -> DagResult:
    """Execute one run of ``dag`` and collect per-task states, XComs and execution order."""
    from airflow.models.xcom import XComModel
    from airflow.sdk import timezone
    from airflow.utils.session import create_session

    ensure_database()
    sync_dag(dag)
    started = time.monotonic()
    with _time_limit(timeout):
        dag_run = dag.test(logical_date=logical_date or timezone.utcnow(), run_conf=conf)
    seconds = time.monotonic() - started

    with create_session() as session:
        instances = dag_run.get_task_instances(session=session)
        rows = session.scalars(
            select(XComModel).where(XComModel.dag_id == dag.dag_id, XComModel.run_id == dag_run.run_id)
        )
        pushed: dict[str, dict[str, Any]] = {}
        for row in rows:
            pushed.setdefault(_key(row.task_id, row.map_index), {})[row.key] = deserialize(row.value)
        tasks = {
            _key(ti.task_id, ti.map_index): TaskOutcome(
                state=str(ti.state) if ti.state else None,
                return_value=pushed.get(_key(ti.task_id, ti.map_index), {}).get(RETURN_KEY),
                xcoms=pushed.get(_key(ti.task_id, ti.map_index), {}),
                try_number=ti.try_number,
            )
            for ti in instances
        }
        ran = sorted((ti for ti in instances if ti.start_date), key=lambda ti: ti.start_date)
        order = [_key(ti.task_id, ti.map_index) for ti in ran]
        state = str(dag_run.state)
    return DagResult(state=state, tasks=tasks, order=order, seconds=seconds)
