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
"""Task-level scenarios: what a database-free single-task runner can and cannot prove."""

from __future__ import annotations

import gc
import os
import re
import sys
import threading
import time
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger
from airflow.sdk import DAG, Asset, BaseOperator, Connection, Variable, task
from airflow.sdk.exceptions import AirflowSkipException
from testing_prototype import DB_FILE
from testing_prototype.task_runner import TaskResult, render_task, run_task

Check = tuple[str, bool, str]
ENDPOINTS_USED: set[str] = set()
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f-]{27}")
WAREHOUSE = {"warehouse": {"conn_type": "postgres", "host": "warehouse.example.com", "port": 5432}}


def eq(label: str, actual: Any, expected: Any) -> Check:
    return (label, actual == expected, f"expected {expected!r}, got {actual!r}")


def clean(result: TaskResult) -> Check:
    unhandled = result.api.unhandled if result.api else []
    for request in result.api.requests if result.api else []:
        method, path = request.split(" ", 1)
        ENDPOINTS_USED.add(f"{method} {_UUID.sub('<id>', path).split('/')[0]}/...")
    return ("no unimplemented API calls", not unhandled, f"unhandled: {sorted(set(unhandled))}")


class Returns(BaseOperator):
    def __init__(self, value: Any = "hello", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.value = value

    def execute(self, context: Any) -> Any:
        return self.value


class Boom(BaseOperator):
    def execute(self, context: Any) -> Any:
        raise ValueError("database unreachable")


class Query(BaseOperator):
    template_fields = ("query",)
    executions = 0

    def __init__(self, query: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.query = query

    def execute(self, context: Any) -> Any:
        type(self).executions += 1
        return self.query


class WaitThenFinish(BaseOperator):
    def execute(self, context: Any) -> Any:
        self.defer(trigger=TimeDeltaTrigger(timedelta(minutes=5)), method_name="finish", kwargs={"tag": "x"})

    def finish(self, context: Any, event: Any = None, tag: str | None = None) -> Any:
        return {"event": event, "tag": tag}


def s_returns_value() -> list[Check]:
    with DAG("s_returns", schedule=None):
        op = Returns(task_id="hello")
    r = run_task(op)
    return [eq("state", r.state, "success"), eq("return value", r.return_value, "hello"), clean(r)]


def s_upstream_and_connection() -> list[Check]:
    with DAG("s_upstream", schedule=None):

        @task
        def load_orders(**context: Any) -> dict[str, Any]:
            rows = context["ti"].xcom_pull(task_ids="extract")
            return {"rows": len(rows), "host": Connection.get("warehouse").host}

        op = load_orders()
    r = run_task(op, upstream={"extract": [{"id": 1}, {"id": 2}]}, connections=WAREHOUSE)
    expected = {"rows": 2, "host": "warehouse.example.com"}
    return [eq("state", r.state, "success"), eq("return value", r.return_value, expected), clean(r)]


def s_taskflow_argument() -> list[Check]:
    with DAG("s_taskflow_arg", schedule=None):

        @task
        def extract() -> list[int]:
            return [1, 2, 3]

        @task
        def total(values: list[int]) -> int:
            return sum(values)

        op = total(extract())
    r = run_task(op, upstream={"extract": [10, 20, 30]})
    return [eq("state", r.state, "success"), eq("return value", r.return_value, 60), clean(r)]


def s_variable_in_code() -> list[Check]:
    with DAG("s_variable", schedule=None):

        @task
        def read() -> str:
            return Variable.get("threshold")

        op = read()
    r = run_task(op, variables={"threshold": "5"})
    return [eq("state", r.state, "success"), eq("return value", r.return_value, "5"), clean(r)]


def s_templates_at_run() -> list[Check]:
    with DAG("s_templates", schedule=None, params={"limit": 10}):
        op = Query(
            task_id="q",
            query="SELECT * FROM {{ var.value.table }} WHERE d='{{ ds }}' LIMIT {{ params.limit }}",
        )
    r = run_task(op, variables={"table": "orders"}, logical_date="2026-03-05T00:00:00Z")
    expected = "SELECT * FROM orders WHERE d='2026-03-05' LIMIT 10"
    return [
        eq("state", r.state, "success"),
        eq("rendered query returned", r.return_value, expected),
        eq("rendered fields reported", r.rendered_fields.get("query"), expected),
        clean(r),
    ]


def s_render_only() -> list[Check]:
    with DAG("s_render_only", schedule=None, params={"limit": 10}):
        op = Query(task_id="q", query="{{ ds }}/{{ params.limit }}/{{ ti.xcom_pull(task_ids='extract') }}")
    before = Query.executions
    rendered = render_task(op, upstream={"extract": "abc"}, logical_date="2026-03-05T00:00:00Z")
    return [
        eq("rendered field", rendered.query, "2026-03-05/10/abc"),
        eq("execute() not called", Query.executions, before),
        eq("original operator untouched", op.query.startswith("{{"), True),
    ]


def s_conf_overrides_params() -> list[Check]:
    with DAG("s_conf", schedule=None, params={"limit": 10}):

        @task
        def read(**context: Any) -> list[Any]:
            return [context["params"]["limit"], context["dag_run"].conf]

        op = read()
    r = run_task(op, conf={"limit": 99})
    return [eq("state", r.state, "success"), eq("params and conf", r.return_value, [99, {"limit": 99}])]


def s_skip() -> list[Check]:
    with DAG("s_skip", schedule=None):

        @task
        def nothing_new() -> None:
            raise AirflowSkipException("no new data")

        op = nothing_new()
    r = run_task(op)
    return [eq("state", r.state, "skipped"), clean(r)]


def s_fail_and_retry() -> list[Check]:
    with DAG("s_fail", schedule=None):
        no_retry = Boom(task_id="no_retry")
        with_retry = Boom(task_id="with_retry", retries=2)
    first = run_task(with_retry, try_number=1)
    last = run_task(with_retry, try_number=3)
    none = run_task(no_retry)
    return [
        eq("no retries -> failed", none.state, "failed"),
        eq("error type", type(none.error).__name__, "ValueError"),
        eq("attempt 1 of 3 -> up_for_retry", first.state, "up_for_retry"),
        eq("attempt 3 of 3 -> failed", last.state, "failed"),
        clean(first),
    ]


def s_try_number_visible() -> list[Check]:
    with DAG("s_try", schedule=None):

        @task(retries=3)
        def attempt(**context: Any) -> int:
            return context["ti"].try_number

        op = attempt()
    r = run_task(op, try_number=2)
    return [eq("try_number seen by task", r.return_value, 2)]


def s_branch() -> list[Check]:
    with DAG("s_branch", schedule=None):

        @task.branch
        def choose(**context: Any) -> str:
            return "path_a" if context["params"].get("mode") == "full" else "path_b"

        a, b = Returns(task_id="path_a"), Returns(task_id="path_b")
        op = choose()
        op >> [a, b]
    r = run_task(op, conf={"mode": "full"})
    skipped = sorted(str(t if isinstance(t, str) else t[0]) for t in r.skipped_downstream)
    return [
        eq("state", r.state, "success"),
        eq("no return value XCom, as on the real server", r.return_value, None),
        eq("downstream skipped", skipped, ["path_b"]),
        clean(r),
    ]


def s_short_circuit() -> list[Check]:
    with DAG("s_short", schedule=None):

        @task.short_circuit
        def has_data() -> bool:
            return False

        after = Returns(task_id="after")
        op = has_data()
        op >> after
    r = run_task(op)
    skipped = sorted(str(t if isinstance(t, str) else t[0]) for t in r.skipped_downstream)
    return [eq("state", r.state, "success"), eq("downstream skipped", skipped, ["after"]), clean(r)]


def s_sensor() -> list[Check]:
    with DAG("s_sensor", schedule=None):
        not_yet = PythonSensor(task_id="not_yet", python_callable=lambda: False, mode="reschedule")
        ready = PythonSensor(task_id="ready", python_callable=lambda: True, mode="reschedule")
    waiting, done = run_task(not_yet), run_task(ready)
    return [
        eq("condition false -> up_for_reschedule", waiting.state, "up_for_reschedule"),
        eq("condition true -> success", done.state, "success"),
        clean(waiting),
    ]


def s_deferrable() -> list[Check]:
    with DAG("s_defer", schedule=None):
        op = WaitThenFinish(task_id="wait")
    first = run_task(op)
    checks = [
        eq("state", first.state, "deferred"),
        eq(
            "trigger",
            first.deferral.trigger if first.deferral else None,
            TimeDeltaTrigger.__module__ + ".DateTimeTrigger",
        ),
        eq("resume method", first.deferral.method if first.deferral else None, "finish"),
        clean(first),
    ]
    if first.deferral:
        resumed = run_task(
            op, resume=(first.deferral.method, {**first.deferral.kwargs, "event": {"status": "fired"}})
        )
        checks += [
            eq("resumed state", resumed.state, "success"),
            eq("resumed return", resumed.return_value, {"event": {"status": "fired"}, "tag": "x"}),
        ]
    return checks


def s_mapped_literal() -> list[Check]:
    with DAG("s_mapped", schedule=None):

        @task
        def double(x: int) -> int:
            return x * 2

        op = double.expand(x=[1, 2, 3])
    r = run_task(op, map_index=1)
    return [eq("state", r.state, "success"), eq("return for map_index 1", r.return_value, 4), clean(r)]


def s_mapped_over_upstream() -> list[Check]:
    with DAG("s_mapped_up", schedule=None):

        @task
        def extract() -> list[int]:
            return [1]

        @task
        def double(x: int) -> int:
            return x * 2

        op = double.expand(x=extract())
    r = run_task(op, map_index=1, upstream={"extract": [10, 20]})
    return [eq("state", r.state, "success"), eq("return for map_index 1", r.return_value, 40), clean(r)]


def s_pull_from_mapped_upstream() -> list[Check]:
    with DAG("s_pull_mapped", schedule=None):

        @task
        def total(**context: Any) -> int:
            return sum(context["ti"].xcom_pull(task_ids="extract"))

        op = total()
    r = run_task(op, upstream_mapped={"extract": [1, 2, 3]})
    return [eq("state", r.state, "success"), eq("sum over map indexes", r.return_value, 6), clean(r)]


def s_asset_outlet() -> list[Check]:
    with DAG("s_asset", schedule=None):

        @task(outlets=[Asset("s3://bucket/orders.parquet")])
        def publish() -> int:
            return 1

        op = publish()
    r = run_task(op)
    reported = (r.api.state_updates[-1] if r.api and r.api.state_updates else {}).get("task_outlets", [])
    return [
        eq("state", r.state, "success"),
        eq("outlet reported to API", [o.get("uri") for o in reported], ["s3://bucket/orders.parquet"]),
        clean(r),
    ]


def s_task_state_store() -> list[Check]:
    with DAG("s_store", schedule=None):

        @task
        def checkpoint(**context: Any) -> Any:
            store = context["task_state_store"]
            store.set("job_id", "abc-123")
            return store.get("job_id")

        op = checkpoint()
    r = run_task(op)
    return [eq("state", r.state, "success"), eq("value read back", r.return_value, "abc-123"), clean(r)]


def s_async_task() -> list[Check]:
    with DAG("s_async", schedule=None):

        @task
        async def fetch() -> str:
            return "async-ok"

        op = fetch()
    r = run_task(op)
    return [eq("state", r.state, "success"), eq("return value", r.return_value, "async-ok"), clean(r)]


def s_multiple_outputs() -> list[Check]:
    with DAG("s_multi", schedule=None):

        @task(multiple_outputs=True)
        def stats(**context: Any) -> dict[str, int]:
            context["ti"].xcom_push(key="extra", value=7)
            return {"rows": 3, "bad": 1}

        op = stats()
    r = run_task(op)
    picked = {k: r.xcoms.get(k) for k in ("rows", "bad", "extra")}
    return [eq("state", r.state, "success"), eq("xcom keys", picked, {"rows": 3, "bad": 1, "extra": 7})]


def s_callbacks() -> list[Check]:
    calls: list[str] = []
    with DAG("s_callbacks", schedule=None):
        ok = Returns(task_id="ok", on_success_callback=lambda context: calls.append("success"))
        bad = Boom(task_id="bad", on_failure_callback=lambda context: calls.append("failure"))
    run_task(ok)
    run_task(bad)
    return [eq("callbacks fired", calls, ["success", "failure"])]


def s_execution_timeout() -> list[Check]:
    with DAG("s_timeout", schedule=None):

        @task(execution_timeout=timedelta(seconds=1))
        def slow() -> None:
            time.sleep(5)

        op = slow()
    started = time.monotonic()
    r = run_task(op)
    return [
        eq("state", r.state, "failed"),
        eq("error type", type(r.error).__name__, "AirflowTaskTimeout"),
        eq("stopped early", time.monotonic() - started < 4, True),
    ]


def s_missing_connection() -> list[Check]:
    with DAG("s_missing_conn", schedule=None):

        @task
        def connect() -> str | None:
            return Connection.get("nope").host

        op = connect()
    r = run_task(op)
    return [
        eq("state", r.state, "failed"),
        eq("error type", type(r.error).__name__, "AirflowNotFoundException"),
        clean(r),
    ]


def s_bash_operator() -> list[Check]:
    with DAG("s_bash", schedule=None):
        op = BashOperator(task_id="echo", bash_command="echo {{ ds }}")
    r = run_task(op, logical_date="2026-03-05T00:00:00Z")
    return [eq("state", r.state, "success"), eq("stdout returned", r.return_value, "2026-03-05"), clean(r)]


SCENARIOS: list[Callable[[], list[Check]]] = [
    s_returns_value,
    s_upstream_and_connection,
    s_taskflow_argument,
    s_variable_in_code,
    s_templates_at_run,
    s_render_only,
    s_conf_overrides_params,
    s_skip,
    s_fail_and_retry,
    s_try_number_visible,
    s_branch,
    s_short_circuit,
    s_sensor,
    s_deferrable,
    s_mapped_literal,
    s_mapped_over_upstream,
    s_pull_from_mapped_upstream,
    s_asset_outlet,
    s_task_state_store,
    s_async_task,
    s_multiple_outputs,
    s_callbacks,
    s_execution_timeout,
    s_missing_connection,
    s_bash_operator,
]


def isolation_and_speed(runs: int = 200) -> list[Check]:
    """Run two tasks repeatedly in one process; report the cost and what the runner leaves behind."""
    with DAG("s_isolation", schedule=None):
        ok, bad = Returns(task_id="ok"), Boom(task_id="bad")
    run_task(ok)
    time.sleep(1.5)
    gc.collect()
    fds, threads, cwd = len(os.listdir("/proc/self/fd")), threading.active_count(), os.getcwd()
    env = {k: v for k, v in os.environ.items() if not k.startswith("AIRFLOW_CTX_")}
    started = time.monotonic()
    states = {run_task(ok if index % 2 == 0 else bad).state for index in range(runs)}
    per_run_ms = (time.monotonic() - started) / runs * 1000
    held_fds, held_threads = len(os.listdir("/proc/self/fd")) - fds, threading.active_count() - threads
    time.sleep(1.5)
    gc.collect()
    left_fds, left_threads = len(os.listdir("/proc/self/fd")) - fds, threading.active_count() - threads
    print(f"    {runs} runs, {per_run_ms:.1f} ms per task run")
    print(f"    held right after the runs: {held_fds} file descriptors, {held_threads} threads")
    print(f"    still held after 1.5s and a GC pass: {left_fds} file descriptors, {left_threads} threads")
    print(
        f"    AIRFLOW_CTX_* variables left in the environment: {sum(k.startswith('AIRFLOW_CTX_') for k in os.environ)}"
    )
    return [
        eq("states stay correct", states, {"success", "failed"}),
        eq("resources are released eventually", (left_fds <= 2, left_threads <= 1), (True, True)),
        eq("working directory unchanged", os.getcwd(), cwd),
        eq(
            "other environment unchanged",
            {k: v for k, v in os.environ.items() if not k.startswith("AIRFLOW_CTX_")},
            env,
        ),
    ]


def main() -> int:
    passed = failed = 0
    for scenario in [*SCENARIOS, isolation_and_speed]:
        name = scenario.__name__.removeprefix("s_")
        try:
            checks = scenario()
        except Exception as exc:
            checks = [("harness", False, f"{type(exc).__name__}: {str(exc).splitlines()[0][:200]}")]
        ok = all(c[1] for c in checks)
        passed, failed = passed + ok, failed + (not ok)
        print(f"{'PASS' if ok else 'FAIL'}  {name}")
        for label, good, detail in checks:
            if not good:
                print(f"      x {label}: {detail}")
    print(f"\n{passed} passed, {failed} failed")
    print(f"endpoint families used ({len(ENDPOINTS_USED)}): {sorted(ENDPOINTS_USED)}")
    print(f"database file created: {DB_FILE.exists()}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
