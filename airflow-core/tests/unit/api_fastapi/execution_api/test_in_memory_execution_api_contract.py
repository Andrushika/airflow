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
Contract tests between this tree's Execution API and the in-memory one behind ``airflow.sdk.testing``.

Each scenario runs one task through the real Task SDK runner twice: against the real routes and
database, and through ``run_task``. The outcomes must be identical, so a difference means the
in-memory API answers a request differently from the server.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import delete, select

from airflow.models.connection import Connection
from airflow.models.task_state_store import TaskStateStoreModel
from airflow.models.variable import Variable
from airflow.models.xcom import XCOM_RETURN_KEY, XComModelV2
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger
from airflow.sdk import DAG, Asset, BaseOperator, Connection as SDKConnection, Variable as SDKVariable, task
from airflow.sdk.api.datamodels._generated import TaskInstance as TIModel
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk.execution_time.supervisor import run_task_in_process
from airflow.sdk.serde import deserialize, serialize
from airflow.sdk.testing import run_task
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.db import clear_db_connections, clear_db_variables

pytestmark = pytest.mark.db_test

LOGICAL_DATE = datetime(2026, 3, 5, tzinfo=UTC)


class Wait(BaseOperator):
    def execute(self, context):
        self.defer(trigger=TimeDeltaTrigger(timedelta(minutes=5)), method_name="finish")

    def finish(self, context, event=None):
        return event


def run_details():
    @task
    def read(**context):
        SDKVariable.set("written", "yes")
        return [
            {"when": context["logical_date"]},
            context["ds"],
            context["params"]["limit"],
            context["dag_run"].conf,
            SDKVariable.get("table"),
            SDKVariable.get("written"),
            SDKConnection.get("warehouse").host,
        ]

    return read(), {
        "conf": {"limit": 99},
        "variables": {"table": "orders"},
        "connections": {"warehouse": "wh"},
    }


def upstream_argument():
    @task
    def extract():
        return [1]

    @task
    def total(values):
        return sum(values)

    return total(extract()), {"upstream": {"extract": [10, 20, 30]}}


def missing_connection():
    @task
    def read():
        return SDKConnection.get("nope").host

    return read(), {}


def skip():
    @task
    def nothing():
        raise AirflowSkipException("no data")

    return nothing(), {}


def failure():
    @task
    def boom():
        raise ValueError("bad")

    return boom(), {}


def retry():
    @task(retries=1)
    def boom():
        raise ValueError("bad")

    return boom(), {}


def branch():
    @task.branch
    def choose():
        return "path_a"

    op = choose()
    op >> [EmptyOperator(task_id="path_a"), EmptyOperator(task_id="path_b")]
    return op, {}


def short_circuit():
    @task.short_circuit
    def has_data():
        return False

    op = has_data()
    op >> EmptyOperator(task_id="after")
    return op, {}


def mapped_literal():
    @task
    def double(x):
        return x * 2

    return double.expand(x=[1, 2, 3]), {"map_index": 1}


def pull_from_mapped_upstream():
    @task
    def extract(x):
        return x

    @task
    def total(**context):
        return sum(context["ti"].xcom_pull(task_ids="extract"))

    extract.expand(x=[0, 0, 0]) >> (op := total())
    return op, {"mapped_upstream": {"extract": [1, 2, 3]}}


def multiple_outputs():
    @task(multiple_outputs=True)
    def stats(**context):
        context["ti"].xcom_push(key="extra", value=7)
        return {"rows": 3}

    return stats(), {}


def task_state_store():
    @task
    def checkpoint(**context):
        context["task_state_store"].set("job_id", "abc-123")
        return context["task_state_store"].get("job_id")

    return checkpoint(), {}


def asset_outlet():
    @task(outlets=[Asset("s3://bucket/orders")])
    def publish(*, outlet_events):
        outlet_events[Asset("s3://bucket/orders")].extra = {"rows": 3}

    return publish(), {}


def deferral():
    return Wait(task_id="wait"), {}


SCENARIOS = [
    run_details,
    upstream_argument,
    missing_connection,
    skip,
    failure,
    retry,
    branch,
    short_circuit,
    mapped_literal,
    pull_from_mapped_upstream,
    multiple_outputs,
    task_state_store,
    asset_outlet,
    deferral,
]


def run_in_memory(build) -> dict[str, Any]:
    with DAG("contract", schedule=None, params={"limit": 10}):
        op, kwargs = build()
    connections = [
        SDKConnection(conn_id=conn_id, conn_type="postgres", host=host)
        for conn_id, host in kwargs.pop("connections", {}).items()
    ]
    result = run_task(op, logical_date=LOGICAL_DATE, connections=connections, **kwargs)
    return {
        "state": result.state.value,
        "xcoms": result.xcoms,
        "error": type(result.error),
        "skipped": sorted(t if isinstance(t, str) else t[0] for t in result.skipped_downstream),
    }


def run_against_server(build, dag_maker, session, task_sdk_client) -> dict[str, Any]:
    with dag_maker("contract", schedule=None, params={"limit": 10}, serialized=True):
        op, kwargs = build()
    operator = getattr(op, "operator", op)
    dag_run = dag_maker.create_dagrun(logical_date=LOGICAL_DATE, conf=kwargs.get("conf"))
    tis = {(ti.task_id, ti.map_index): ti for ti in dag_run.get_task_instances(session=session)}
    for task_id, value in kwargs.get("upstream", {}).items():
        session.add(
            XComModelV2(key=XCOM_RETURN_KEY, value=serialize(value), task_instance_id=tis[task_id, -1].id)
        )
    for task_id, values in kwargs.get("mapped_upstream", {}).items():
        for index, value in enumerate(values):
            session.add(
                XComModelV2(
                    key=XCOM_RETURN_KEY, value=serialize(value), task_instance_id=tis[task_id, index].id
                )
            )
    for key, value in kwargs.get("variables", {}).items():
        session.add(Variable(key=key, val=value))
    for conn_id, host in kwargs.get("connections", {}).items():
        session.add(Connection(conn_id=conn_id, conn_type="postgres", host=host))
    ti = tis[operator.task_id, kwargs.get("map_index", -1)]
    ti.state = TaskInstanceState.QUEUED
    ti.try_number = 1
    session.commit()

    outcome = run_task_in_process(
        ti=TIModel(
            id=ti.id,
            task_id=ti.task_id,
            dag_id=ti.dag_id,
            run_id=ti.run_id,
            try_number=ti.try_number,
            map_index=ti.map_index,
            dag_version_id=ti.dag_version_id,
        ),
        task=operator,
        client=task_sdk_client,
    )
    session.expire_all()
    rows = session.scalars(select(XComModelV2).where(XComModelV2.task_instance_id == ti.id)).all()
    states = {(t.task_id, t.map_index): t.state for t in dag_run.get_task_instances(session=session)}
    return {
        "state": states[ti.task_id, ti.map_index],
        "xcoms": {row.key: deserialize(row.value) for row in rows},
        "error": type(outcome.error),
        "skipped": sorted(
            t for t in operator.downstream_task_ids if states[t, -1] == TaskInstanceState.SKIPPED
        ),
    }


@pytest.fixture(autouse=True)
def _clean_db(session):
    clear_db_variables()
    clear_db_connections(add_default_connections_back=False)
    session.execute(delete(TaskStateStoreModel))
    yield
    clear_db_variables()
    clear_db_connections(add_default_connections_back=False)


@pytest.mark.parametrize("build", SCENARIOS, ids=lambda build: build.__name__)
def test_in_memory_matches_server(build, dag_maker, session, task_sdk_client):
    in_memory = run_in_memory(build)

    server = run_against_server(build, dag_maker, session, task_sdk_client)

    assert in_memory == server
