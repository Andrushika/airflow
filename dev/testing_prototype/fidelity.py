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
Run the same task through the in-memory API and the real Execution API, and compare.

The real side is ``dag.test()`` on SQLite, so the task talks to the genuine API server and
database. Any difference is a place where the in-memory API has drifted from the server.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Asset, Connection, Variable, task, timezone
from airflow.sdk.exceptions import AirflowSkipException
from testing_prototype.dag_runner import run_dag
from testing_prototype.task_runner import run_task

LOGICAL_DATE = datetime(2026, 3, 5, tzinfo=timezone.utc)
Case = tuple[DAG, str, dict[str, Any]]


def c_returns_value() -> Case:
    with DAG("f_returns", schedule=None) as dag:

        @task
        def hello() -> dict[str, Any]:
            return {"a": 1, "when": datetime(2026, 1, 2, tzinfo=timezone.utc)}

        hello()
    return dag, "hello", {}


def c_taskflow_argument() -> Case:
    with DAG("f_arg", schedule=None) as dag:

        @task
        def extract() -> list[int]:
            return [10, 20, 30]

        @task
        def total(values: list[int]) -> int:
            return sum(values)

        total(extract())
    return dag, "total", {}


def c_templates() -> Case:
    with DAG("f_templates", schedule=None, params={"limit": 10}) as dag:
        BashOperator(task_id="echo", bash_command="echo {{ ds }} {{ params.limit }} {{ var.value.table }}")
    return dag, "echo", {"variables": {"table": "orders"}}


def c_context_values() -> Case:
    with DAG("f_context", schedule=None, params={"limit": 10}) as dag:

        @task
        def read(**context: Any) -> list[Any]:
            ti = context["ti"]
            return [
                context["ds"],
                context["params"]["limit"],
                ti.try_number,
                ti.map_index,
                context["run_id"] != "",
            ]

        read()
    return dag, "read", {}


def c_skip() -> Case:
    with DAG("f_skip", schedule=None) as dag:

        @task
        def nothing() -> None:
            raise AirflowSkipException("no data")

        nothing()
    return dag, "nothing", {}


def c_fail() -> Case:
    with DAG("f_fail", schedule=None) as dag:

        @task
        def boom() -> None:
            raise ValueError("bad")

        boom()
    return dag, "boom", {}


def c_fail_after_retries() -> Case:
    with DAG("f_retries", schedule=None) as dag:

        @task(retries=1, retry_delay=timedelta(seconds=0))
        def boom() -> None:
            raise ValueError("bad")

        boom()
    return dag, "boom", {"try_number": 2}


def c_branch() -> Case:
    with DAG("f_branch", schedule=None) as dag:

        @task.branch
        def choose() -> str:
            return "path_a"

        choose() >> [EmptyOperator(task_id="path_a"), EmptyOperator(task_id="path_b")]
    return dag, "choose", {}


def c_short_circuit() -> Case:
    with DAG("f_short", schedule=None) as dag:

        @task.short_circuit
        def has_data() -> bool:
            return False

        has_data() >> EmptyOperator(task_id="after")
    return dag, "has_data", {}


def c_mapped() -> Case:
    with DAG("f_mapped", schedule=None) as dag:

        @task
        def double(x: int) -> int:
            return x * 2

        double.expand(x=[1, 2, 3])
    return dag, "double[1]", {"map_index": 1}


def c_multiple_outputs() -> Case:
    with DAG("f_multi", schedule=None) as dag:

        @task(multiple_outputs=True)
        def stats(**context: Any) -> dict[str, int]:
            context["ti"].xcom_push(key="extra", value=7)
            return {"rows": 3, "bad": 1}

        stats()
    return dag, "stats", {}


def c_variable_and_connection() -> Case:
    with DAG("f_secrets", schedule=None) as dag:

        @task
        def read() -> list[Any]:
            return [Variable.get("table"), Connection.get("warehouse").host]

        read()
    return (
        dag,
        "read",
        {
            "variables": {"table": "orders"},
            "connections": {"warehouse": {"conn_type": "postgres", "host": "wh"}},
        },
    )


def c_missing_connection() -> Case:
    with DAG("f_missing", schedule=None) as dag:

        @task
        def read() -> Any:
            return Connection.get("nope").host

        read()
    return dag, "read", {}


def c_async() -> Case:
    with DAG("f_async", schedule=None) as dag:

        @task
        async def fetch() -> str:
            return "async-ok"

        fetch()
    return dag, "fetch", {}


def c_task_state_store() -> Case:
    with DAG("f_store", schedule=None) as dag:

        @task
        def checkpoint(**context: Any) -> Any:
            store = context["task_state_store"]
            store.set("job_id", "abc-123")
            return store.get("job_id")

        checkpoint()
    return dag, "checkpoint", {}


def c_asset_outlet() -> Case:
    with DAG("f_asset", schedule=None) as dag:

        @task(outlets=[Asset("s3://bucket/orders.parquet")])
        def publish() -> int:
            return 1

        publish()
    return dag, "publish", {}


CASES: list[Callable[[], Case]] = [
    c_returns_value,
    c_taskflow_argument,
    c_templates,
    c_context_values,
    c_skip,
    c_fail,
    c_fail_after_retries,
    c_branch,
    c_short_circuit,
    c_mapped,
    c_multiple_outputs,
    c_variable_and_connection,
    c_missing_connection,
    c_async,
    c_task_state_store,
    c_asset_outlet,
]


def _publish_secrets(kwargs: dict[str, Any]) -> None:
    """Give the real run the same variables and connections through environment variables."""
    for key, value in kwargs.get("variables", {}).items():
        os.environ[f"AIRFLOW_VAR_{key.upper()}"] = value
    for conn_id, fields in kwargs.get("connections", {}).items():
        os.environ[f"AIRFLOW_CONN_{conn_id.upper()}"] = Connection(conn_id=conn_id, **fields).get_uri()


def _clear_secrets(kwargs: dict[str, Any]) -> None:
    for key in kwargs.get("variables", {}):
        os.environ.pop(f"AIRFLOW_VAR_{key.upper()}", None)
    for conn_id in kwargs.get("connections", {}):
        os.environ.pop(f"AIRFLOW_CONN_{conn_id.upper()}", None)


def compare(case: Callable[[], Case]) -> list[str]:
    dag, target, kwargs = case()
    task_id = target.split("[")[0]
    operator = dag.task_dict[task_id]

    _publish_secrets(kwargs)
    try:
        real_run = run_dag(dag, logical_date=LOGICAL_DATE)
    finally:
        _clear_secrets(kwargs)
    real = real_run.tasks[target]
    upstream = {tid: real_run.tasks[tid].return_value for tid in operator.upstream_task_ids}
    fake = run_task(operator, upstream=upstream, logical_date=LOGICAL_DATE.isoformat(), **kwargs)

    differences = []
    if fake.state != real.state:
        differences.append(f"state: in-memory={fake.state!r} real={real.state!r}")
    if fake.return_value != real.return_value:
        differences.append(f"return value: in-memory={fake.return_value!r} real={real.return_value!r}")
    if sorted(fake.xcoms) != sorted(real.xcoms):
        differences.append(f"xcom keys: in-memory={sorted(fake.xcoms)} real={sorted(real.xcoms)}")
    real_skipped = sorted(
        tid for tid in operator.downstream_task_ids if real_run.states.get(tid) == "skipped"
    )
    fake_skipped = sorted(str(t if isinstance(t, str) else t[0]) for t in fake.skipped_downstream)
    if fake_skipped != real_skipped:
        differences.append(f"downstream skipped: in-memory={fake_skipped} real={real_skipped}")
    if fake.api and fake.api.unhandled:
        differences.append(f"in-memory API missing: {sorted(set(fake.api.unhandled))}")
    return differences


def main() -> int:
    same = different = 0
    for case in CASES:
        name = case.__name__.removeprefix("c_")
        try:
            differences = compare(case)
        except Exception as exc:
            differences = [f"harness {type(exc).__name__}: {str(exc).splitlines()[0][:220]}"]
        same, different = same + (not differences), different + bool(differences)
        print(f"{'SAME' if not differences else 'DIFF'}  {name}")
        for line in differences:
            print(f"      x {line}")
    print(f"\n{same} identical, {different} different")
    return 1 if different else 0


if __name__ == "__main__":
    sys.exit(main())
