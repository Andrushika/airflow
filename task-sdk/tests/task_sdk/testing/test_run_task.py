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

from datetime import UTC, datetime, timedelta

import pytest

from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.triggers.temporal import DateTimeTrigger, TimeDeltaTrigger
from airflow.sdk import DAG, Asset, BaseOperator, Connection, TaskInstanceState, Variable, task
from airflow.sdk.testing import run_task

LOGICAL_DATE = datetime(2026, 3, 5, tzinfo=UTC)


class WaitThenFinish(BaseOperator):
    def execute(self, context):
        self.defer(trigger=TimeDeltaTrigger(timedelta(minutes=5)), method_name="finish", kwargs={"tag": "x"})

    def finish(self, context, event=None, tag=None):
        return tag


def run_details():
    @task
    def read(**context):
        ti = context["ti"]
        return [context["ds"], context["dag_run"].conf, ti.try_number, ti.map_index]

    return read(), {"logical_date": LOGICAL_DATE, "conf": {"limit": 99}, "try_number": 2}


def mapped_over_upstream():
    @task
    def extract():
        return [1]

    @task
    def double(x):
        return x * 2

    return double.expand(x=extract()), {"upstream": {"extract": [10, 20]}, "map_index": 1}


def connection_fields():
    @task
    def read():
        conn = Connection.get("warehouse")
        return [conn.host, conn.login, conn.port, conn.extra_dejson]

    warehouse = Connection(
        conn_id="warehouse", conn_type="postgres", host="wh", login="me", port=5432, extra='{"ssl": true}'
    )
    return read(), {"connections": [warehouse]}


def async_task():
    @task
    async def fetch():
        return "async-ok"

    return fetch(), {}


def triggering_asset_events():
    @task
    def read(**context):
        events = context["triggering_asset_events"][Asset("s3://in")]
        return [context["dag_run"].run_type, [event.extra for event in events]]

    return read(), {"triggering_asset_events": {Asset("s3://in"): [{"rows": 1}, {"rows": 2}]}}


@pytest.mark.parametrize(
    ("build", "expected"),
    [
        pytest.param(run_details, ["2026-03-05", {"limit": 99}, 2, -1], id="run-details"),
        pytest.param(mapped_over_upstream, 40, id="mapped-over-upstream"),
        pytest.param(connection_fields, ["wh", "me", 5432, {"ssl": True}], id="connection-fields"),
        pytest.param(async_task, "async-ok", id="async-task"),
        pytest.param(
            triggering_asset_events,
            ["asset_triggered", [{"rows": 1}, {"rows": 2}]],
            id="triggering-asset-events",
        ),
    ],
)
def test_return_value(build, expected):
    with DAG("return_value", schedule=None):
        op, kwargs = build()

    result = run_task(op, **kwargs)

    assert result.state == TaskInstanceState.SUCCESS
    assert result.return_value == expected


def test_sensor_reschedules():
    with DAG("sensor", schedule=None):
        op = PythonSensor(task_id="not_yet", python_callable=lambda: False, mode="reschedule")

    assert run_task(op).state == TaskInstanceState.UP_FOR_RESCHEDULE


def test_deferral():
    with DAG("deferral", schedule=None):
        op = WaitThenFinish(task_id="wait")

    result = run_task(op)

    assert result.state == TaskInstanceState.DEFERRED
    assert isinstance(result.trigger, DateTimeTrigger)
    assert (result.next_method, result.next_kwargs) == ("finish", {"tag": "x"})


def test_outlet_events():
    with DAG("outlet_events", schedule=None):

        @task(outlets=[Asset("s3://out")])
        def publish(*, outlet_events):
            outlet_events[Asset("s3://out")].extra = {"rows": 3}

        op = publish()

    assert run_task(op).outlet_events == [
        {"dest_asset_key": {"name": "s3://out", "uri": "s3://out/"}, "extra": {"rows": 3}}
    ]


def test_unsupported_request_raises():
    with DAG("unsupported", schedule=None):

        @task
        def delete():
            Variable.delete("threshold")

        op = delete()

    with pytest.raises(NotImplementedError, match="DELETE variables/threshold"):
        run_task(op)


def test_derived_xcom_arg_is_rejected():
    with DAG("derived", schedule=None):

        @task
        def extract():
            return [1]

        derived = extract().map(str)

    with pytest.raises(TypeError, match="not"):
        run_task(derived)
