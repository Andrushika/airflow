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
Seven tests written the way the docs suggest, next to a real run and an in-memory run of the same task.

Every path builds a fresh Dag, because the docs-style test changes the operator it tests.
"""

from __future__ import annotations

import datetime as dt
import sys
from unittest.mock import MagicMock

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, task
from testing_prototype.dag_runner import ensure_database, run_dag, sync_dag
from testing_prototype.task_runner import run_task


def outcome(fn) -> str:
    try:
        value = fn()
    except BaseException as exc:
        return f"RAISES {type(exc).__name__}: {str(exc).splitlines()[0][:120]}"
    return f"passes -> {value!r}"[:170]


def real(build, task_id, **kw) -> str:
    def go():
        r = run_dag(build(), **kw)
        t = r.tasks[task_id]
        return f"run={r.state} task={t.state} value={t.return_value!r}"

    return outcome(go).replace("passes -> ", "")


def fake(build, task_id, **kw) -> str:
    def go():
        r = run_task(build().get_task(task_id), **kw)
        err = f" | {type(r.error).__name__}: {str(r.error).splitlines()[0][:100]}" if r.error else ""
        return f"task={r.state} value={r.return_value!r}{err}"

    return outcome(go).replace("passes -> ", "")


def show(title: str, rows: dict[str, str]) -> None:
    print(f"\n##### {title}")
    for k, v in rows.items():
        print(f"  {k:<30} {v}")


def a1():
    with DAG("k_a1", schedule=None) as dag:
        BashOperator(task_id="echo", bash_command="echo {{ ds }}")
    return dag


def a1_docs():
    op = a1().get_task("echo")
    op.render_template_fields(context={"ds": "2021-09-13"})
    return op.bash_command


def a1_real_no_date():
    ensure_database()
    dag = a1()
    sync_dag(dag)
    dr = dag.test()
    ti = dr.get_task_instances()[0]
    return f"run={dr.state} task={ti.state} logical_date={dr.logical_date}"


def uses_execution_date(**context):
    return context["execution_date"].strftime("%Y%m%d")


def a2():
    with DAG("k_a2", schedule=None) as dag:
        PythonOperator(task_id="partition", python_callable=uses_execution_date)
    return dag


def total(**context):
    values = context["ti"].xcom_pull(task_ids="extrct")  # typo in the upstream task id
    return sum(values)


def a3():
    with DAG("k_a3", schedule=None) as dag:

        @task
        def extract():
            return [1, 2, 3]

        extract() >> PythonOperator(task_id="total", python_callable=total)
    return dag


def a3_docs():
    ti = MagicMock()
    ti.xcom_pull.return_value = [1, 2, 3]
    return a3().get_task("total").execute(context={"ti": ti})


class Report:
    def __init__(self, rows: int):
        self.rows = rows


def b1():
    with DAG("k_b1", schedule=None) as dag:

        @task
        def build_report():
            return Report(rows=3)

        build_report()
    return dag


def b2():
    with DAG("k_b2", schedule=None) as dag:

        @task(multiple_outputs=True)
        def stats():
            return [("rows", 3)]

        stats()
    return dag


def b3():
    with DAG("k_b3", schedule=None) as dag:

        @task.branch
        def choose():
            return "full_laod"  # typo

        choose() >> [EmptyOperator(task_id="full_load"), EmptyOperator(task_id="incremental_load")]
    return dag


def b4():
    with DAG("k_b4", schedule=None) as dag:

        @task
        def files():
            return ["a.csv", "b.csv"]

        @task
        def size(names):
            return len(names)  # the author expects the whole list

        size.expand(names=files())
    return dag


def main() -> int:
    show(
        "A1 template {{ ds }}",
        {
            "docs-style test": outcome(a1_docs),
            "real run, date given": real(a1, "echo", logical_date=dt.datetime(2026, 3, 5, tzinfo=dt.UTC)),
            "real run, dag.test() no date": outcome(a1_real_no_date).replace("passes -> ", ""),
            "in-memory, date given": fake(a1, "echo", logical_date="2026-03-05T00:00:00Z"),
            "in-memory, no logical date": fake(a1, "echo", logical_date=None),
        },
    )
    show(
        "A2 context['execution_date']",
        {
            "docs-style test": outcome(
                lambda: (
                    a2().get_task("partition").execute(context={"execution_date": dt.datetime(2021, 9, 13)})
                )
            ),
            "real run": real(a2, "partition"),
            "in-memory": fake(a2, "partition"),
        },
    )
    show(
        "A3 xcom_pull typo, mocked ti",
        {
            "docs-style test": outcome(a3_docs),
            "real run": real(a3, "total"),
            "in-memory": fake(a3, "total", upstream={"extract": [1, 2, 3]}),
        },
    )
    show(
        "B1 return value not serializable",
        {
            "docs-style test": outcome(
                lambda: type(b1().get_task("build_report").python_callable()).__name__
            ),
            "real run": real(b1, "build_report"),
            "in-memory": fake(b1, "build_report"),
        },
    )
    show(
        "B2 multiple_outputs with a list",
        {
            "docs-style test": outcome(lambda: b2().get_task("stats").python_callable()),
            "real run": real(b2, "stats"),
            "in-memory": fake(b2, "stats"),
        },
    )
    show(
        "B3 branch to a missing task id",
        {
            "docs-style test": outcome(lambda: b3().get_task("choose").python_callable()),
            "real run": real(b3, "choose"),
            "in-memory": fake(b3, "choose"),
        },
    )
    show(
        "B4 mapped task argument",
        {
            "docs-style test": outcome(lambda: b4().get_task("size").python_callable(["a.csv", "b.csv"])),
            "real run": outcome(
                lambda: {k: (v.state, v.return_value) for k, v in run_dag(b4()).tasks.items()}
            ).replace("passes -> ", ""),
            "in-memory": fake(b4, "size", upstream={"files": ["a.csv", "b.csv"]}, map_index=0),
        },
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
