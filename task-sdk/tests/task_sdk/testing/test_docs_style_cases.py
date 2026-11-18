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
Tests written the way the best-practices docs suggest pass, while the task fails for real.

Each test first makes the docs-style call, then runs the same task with ``run_task``. Every call
gets a freshly built Dag, because the docs-style call changes the operator it is given.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest import mock

import jinja2
import pytest

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskInstanceState, task
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.sdk.testing import run_task

LOGICAL_DATE = datetime(2021, 9, 13, tzinfo=UTC)


def build_echo_dag() -> DAG:
    with DAG("echo", schedule=None) as dag:
        BashOperator(task_id="echo", bash_command="echo {{ ds }}")
    return dag


def test_ds_without_a_logical_date():
    op = build_echo_dag().get_task("echo")
    op.render_template_fields(context={"ds": "2021-09-13"})
    assert op.bash_command == "echo 2021-09-13"

    result = run_task(build_echo_dag().get_task("echo"), logical_date=None)

    assert result.state == TaskInstanceState.FAILED
    assert isinstance(result.error, jinja2.UndefinedError)


def build_partition_dag() -> DAG:
    def partition(**context):
        return context["execution_date"].strftime("%Y%m%d")

    with DAG("partition", schedule=None) as dag:
        PythonOperator(task_id="partition", python_callable=partition)
    return dag


def test_execution_date_in_context():
    op = build_partition_dag().get_task("partition")
    assert op.execute(context={"execution_date": LOGICAL_DATE}) == "20210913"

    result = run_task(build_partition_dag().get_task("partition"), logical_date=LOGICAL_DATE)

    assert result.state == TaskInstanceState.FAILED
    assert isinstance(result.error, KeyError)


def build_total_dag() -> DAG:
    def total(**context):
        return sum(context["ti"].xcom_pull(task_ids="extrct"))

    with DAG("total", schedule=None) as dag:

        @task
        def extract():
            return [1, 2, 3]

        extract() >> PythonOperator(task_id="total", python_callable=total)
    return dag


def test_xcom_pull_with_a_typo():
    ti = mock.create_autospec(RuntimeTaskInstance, instance=True)
    ti.xcom_pull.return_value = [1, 2, 3]
    assert build_total_dag().get_task("total").execute(context={"ti": ti}) == 6

    result = run_task(build_total_dag().get_task("total"), upstream={"extract": [1, 2, 3]})

    assert result.state == TaskInstanceState.FAILED
    assert isinstance(result.error, TypeError)


class Report:
    def __init__(self, rows):
        self.rows = rows


def build_report_dag() -> DAG:
    with DAG("report", schedule=None) as dag:

        @task
        def build_report():
            return Report(rows=3)

        build_report()
    return dag


def test_return_value_xcom_cannot_store():
    assert build_report_dag().get_task("build_report").execute(context={}).rows == 3

    result = run_task(build_report_dag().get_task("build_report"))

    assert result.state == TaskInstanceState.FAILED
    assert isinstance(result.error, TypeError)


def build_stats_dag() -> DAG:
    with DAG("stats", schedule=None) as dag:

        @task(multiple_outputs=True)
        def stats():
            return [("rows", 3)]

        stats()
    return dag


def test_multiple_outputs_returning_a_list():
    assert build_stats_dag().get_task("stats").execute(context={}) == [("rows", 3)]

    result = run_task(build_stats_dag().get_task("stats"))

    assert result.state == TaskInstanceState.FAILED
    assert isinstance(result.error, TypeError)


def build_size_dag() -> DAG:
    with DAG("size", schedule=None) as dag:

        @task
        def files():
            return ["a.csv", "b.csv"]

        @task
        def size(names):
            return len(names)

        size.expand(names=files())
    return dag


@pytest.mark.parametrize("map_index", [0, 1])
def test_mapped_task_gets_one_element(map_index):
    assert build_size_dag().get_task("size").python_callable(["a.csv", "b.csv"]) == 2

    result = run_task(
        build_size_dag().get_task("size"), upstream={"files": ["a.csv", "b.csv"]}, map_index=map_index
    )

    assert result.state == TaskInstanceState.SUCCESS
    assert result.return_value == 5
