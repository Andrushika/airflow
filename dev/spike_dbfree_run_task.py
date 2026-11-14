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
Spike: run tasks through ``run_task_in_process`` with an in-memory fake Execution API.

Throwaway experiment, not a design. It answers one question: can the existing
``client=`` seam on ``run_task_in_process`` run real task code with no metadata database?
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import uuid
from pathlib import Path

AIRFLOW_HOME = tempfile.mkdtemp(prefix="spike_airflow_home_")
os.environ["AIRFLOW_HOME"] = AIRFLOW_HOME
# ``import airflow`` builds a SQLAlchemy engine eagerly, so the URL has to be loadable. SQLite only
# creates its file on first connection, which makes "was the file created?" the test for DB access.
DB_FILE = Path(AIRFLOW_HOME) / "airflow.db"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{DB_FILE}"

import httpx2  # noqa: E402

from airflow.sdk import DAG, BaseOperator, Connection, task  # noqa: E402
from airflow.sdk.api.client import Client  # noqa: E402
from airflow.sdk.api.datamodels._generated import TaskInstance  # noqa: E402
from airflow.sdk.execution_time.supervisor import run_task_in_process  # noqa: E402


class FakeExecutionAPI:
    """Answers Execution API requests from plain dictionaries."""

    def __init__(self, *, xcoms=None, connections=None, variables=None, max_tries=0):
        self.xcoms: dict[tuple[str, str, str, str], object] = dict(xcoms or {})
        self.connections: dict[str, dict] = dict(connections or {})
        self.variables: dict[str, str] = dict(variables or {})
        self.max_tries = max_tries
        self.state_updates: list[dict] = []
        self.requests: list[str] = []
        self.unhandled: list[str] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        method, path = request.method, request.url.path.lstrip("/")
        self.requests.append(f"{method} {path}")

        if method == "PATCH" and re.fullmatch(r"task-instances/[^/]+/run", path):
            return httpx2.Response(
                200,
                json={
                    "dag_run": {
                        "dag_id": "spike",
                        "run_id": "test_run",
                        "logical_date": "2026-01-01T00:00:00Z",
                        "start_date": "2026-01-01T00:00:00Z",
                        "run_type": "manual",
                        "run_after": "2026-01-01T00:00:00Z",
                        "consumed_asset_events": [],
                        "data_interval_start": None,
                        "data_interval_end": None,
                        "end_date": None,
                        "state": "running",
                        "partition_key": None,
                    },
                    "max_tries": self.max_tries,
                    "should_retry": self.max_tries > 0,
                },
            )
        if method == "PATCH" and re.fullmatch(r"task-instances/[^/]+/state", path):
            self.state_updates.append(json.loads(request.content))
            return httpx2.Response(204)
        if method == "PUT" and re.fullmatch(r"task-instances/[^/]+/rtif", path):
            return httpx2.Response(201, json={"message": "ok"})
        if method == "GET" and re.fullmatch(r"task-instances/[^/]+/validate-inlets-and-outlets", path):
            return httpx2.Response(200, json={"inactive_assets": []})

        # ``xcom_pull`` without ``map_indexes`` reads one value per map index through the slice endpoint.
        if method == "GET" and (m := re.fullmatch(r"xcoms/([^/]+)/([^/]+)/([^/]+)/([^/]+)/slice", path)):
            key = (m.group(1), m.group(2), m.group(3), m.group(4))
            return httpx2.Response(200, json=[self.xcoms[key]] if key in self.xcoms else [])

        if m := re.fullmatch(r"xcoms/([^/]+)/([^/]+)/([^/]+)/([^/]+)", path):
            key = (m.group(1), m.group(2), m.group(3), m.group(4))
            if method == "GET":
                if key in self.xcoms:
                    return httpx2.Response(200, json={"key": key[3], "value": self.xcoms[key]})
                return httpx2.Response(404, json={"detail": {"reason": "not_found"}})
            if method == "POST":
                self.xcoms[key] = json.loads(request.content)
                return httpx2.Response(201, json={"message": "ok"})

        if method == "GET" and (m := re.fullmatch(r"connections/(.+)", path)):
            if conn := self.connections.get(m.group(1)):
                return httpx2.Response(200, json=conn)
            return httpx2.Response(404, json={"detail": {"reason": "not_found"}})

        if method == "GET" and (m := re.fullmatch(r"variables/(.+)", path)):
            if m.group(1) in self.variables:
                return httpx2.Response(200, json={"key": m.group(1), "value": self.variables[m.group(1)]})
            return httpx2.Response(404, json={"detail": {"reason": "not_found"}})

        # Fail loudly: a silent canned answer is how a fake drifts from the real server.
        self.unhandled.append(f"{method} {path}")
        return httpx2.Response(501, json={"detail": f"fake API does not implement {method} {path}"})


def run(task_obj, fake: FakeExecutionAPI, *, try_number: int = 1):
    client = Client(base_url=None, dry_run=True, token="", transport=httpx2.MockTransport(fake))
    client.base_url = "http://in-process.invalid./"
    ti = TaskInstance(
        id=uuid.uuid4(),
        task_id=task_obj.task_id,
        dag_id=task_obj.dag_id,
        run_id="test_run",
        try_number=try_number,
        dag_version_id=uuid.uuid4(),
    )
    return run_task_in_process(ti=ti, task=task_obj, client=client)


def report(title: str, result, fake: FakeExecutionAPI) -> None:
    print(f"\n=== {title}")
    print("  state:", result.state)
    print("  error:", repr(result.error))
    print(
        "  xcoms pushed:", {k[2] + "/" + k[3]: v for k, v in fake.xcoms.items() if k[2] == result.ti.task_id}
    )
    print("  state reported to API:", [u.get("state") for u in fake.state_updates])
    print("  API calls:", fake.requests)
    print("  unhandled calls:", fake.unhandled or "none")


class Hello(BaseOperator):
    def execute(self, context):
        return "hello"


class Boom(BaseOperator):
    def execute(self, context):
        raise ValueError("database unreachable")


with DAG("spike", schedule=None):
    hello = Hello(task_id="hello")

    @task
    def load_orders(**context):
        rows = context["ti"].xcom_pull(task_ids="extract")
        conn = Connection.get("warehouse")
        return {"rows": len(rows), "host": conn.host}

    load = load_orders().operator
    boom_no_retry = Boom(task_id="boom_no_retry")
    boom_retry = Boom(task_id="boom_retry", retries=2)


scenarios = [
    ("1. plain operator returns a value", hello, FakeExecutionAPI()),
    (
        "2. @task pulls upstream XCom and reads a connection",
        load,
        FakeExecutionAPI(
            xcoms={("spike", "test_run", "extract", "return_value"): [{"id": 1}, {"id": 2}]},
            connections={
                "warehouse": {
                    "conn_id": "warehouse",
                    "conn_type": "postgres",
                    "host": "warehouse.example.com",
                    "schema": None,
                    "login": None,
                    "password": None,
                    "port": 5432,
                    "extra": None,
                }
            },
        ),
    ),
    ("3. task fails, no retries left", boom_no_retry, FakeExecutionAPI()),
    ("4. task fails, retries remain", boom_retry, FakeExecutionAPI(max_tries=2)),
]

failures = 0
for title, task_obj, fake in scenarios:
    try:
        report(title, run(task_obj, fake), fake)
    except Exception as exc:
        failures += 1
        print(f"\n=== {title}\n  HARNESS ERROR: {type(exc).__name__}: {exc}")
        print("  API calls:", fake.requests)
        print("  unhandled calls:", fake.unhandled or "none")

print("\n=== metadata database check")
print(
    "  files created under AIRFLOW_HOME:",
    sorted(p.name for p in Path(AIRFLOW_HOME).rglob("*") if p.is_file()),
)
print("  database file created:", DB_FILE.exists())
print("  sqlalchemy imported:", "sqlalchemy" in sys.modules)
sys.exit(1 if failures else 0)
