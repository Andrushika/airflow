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
Use ``dag.test()`` the way a user would in a test, on a fresh ``AIRFLOW_HOME``.

Run it directly, not as a module of this package: the package would prepare the home for it.
"""

from __future__ import annotations

import os
import sys
import tempfile
import textwrap
import time
from pathlib import Path

HOME = Path(tempfile.mkdtemp(prefix="kaxil_home_"))
(HOME / "dags").mkdir()
os.environ["AIRFLOW_HOME"] = str(HOME)
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{HOME / 'airflow.db'}"
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"

from airflow.sdk import DAG, task  # noqa: E402


def short(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {str(exc).splitlines()[0][:130]}"


def step(label, fn):
    started = time.monotonic()
    try:
        out = fn()
    except BaseException as exc:
        out = "RAISES " + short(exc)
    print(f"RESULT {label:<46} {time.monotonic() - started:6.2f}s  {out}")


def in_test_dag():
    with DAG("in_test", schedule=None) as dag:

        @task
        def hello():
            return 1

        hello()
    return dag


step("1. no database prepared", lambda: in_test_dag().test().state)

from airflow.utils import db  # noqa: E402

step("   (airflow db migrate)", lambda: db.resetdb() or "done")
step("2. Dag defined inside the test", lambda: in_test_dag().test().state)

(HOME / "dags" / "orders.py").write_text(
    textwrap.dedent(
        """
        from datetime import timedelta
        from airflow.sdk import DAG, task

        with DAG("orders", schedule=None) as dag:
            @task
            def extract():
                return [10, 20, 30]

            @task
            def total(values):
                return sum(values)

            total(extract())

        with DAG("broken", schedule=None) as broken:
            @task
            def boom():
                raise ValueError("bad input row 17")

            boom()

        with DAG("flaky", schedule=None) as flaky:
            @task(retries=1, retry_delay=timedelta(seconds=8))
            def always_fails():
                raise ConnectionError("try again")

            always_fails()
        """
    )
)
sys.path.insert(0, str(HOME / "dags"))
import orders  # noqa: E402

step("3. Dag in the dags folder, first run", lambda: orders.dag.test().state)
step("   second run", lambda: orders.dag.test().state)
step("   third run", lambda: orders.dag.test().state)


def failed_task():
    dr = orders.broken.test()
    ti = dr.get_task_instances()[0]
    public = [a for a in dir(dr) if "error" in a.lower() or "exception" in a.lower()]
    public_ti = [a for a in dir(ti) if "error" in a.lower() or "exception" in a.lower()]
    return f"returned {type(dr).__name__} state={dr.state} ti.state={ti.state} error attrs on run={public} on ti={public_ti}"


step("4. task raises ValueError", failed_task)
step("5. retries=1, retry_delay=8s", lambda: orders.flaky.test().state)


def assert_on_tasks():
    from sqlalchemy import select

    from airflow.models.xcom import XComModel
    from airflow.sdk.serde import deserialize
    from airflow.utils.session import create_session

    dr = orders.dag.test()
    with create_session() as session:
        states = {ti.task_id: str(ti.state) for ti in dr.get_task_instances(session=session)}
        rows = session.scalars(
            select(XComModel).where(XComModel.run_id == dr.run_id, XComModel.dag_id == "orders")
        )
        values = {r.task_id: deserialize(r.value) for r in rows}
    return f"states={states} values={values}"


step("6. per-task states and values (needs ORM)", assert_on_tasks)
