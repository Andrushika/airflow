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
"""Dag-level scenarios: what ``dag.test()`` on a throwaway database can prove about task interaction."""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger
from airflow.sdk import DAG, Asset, BaseOperator, task
from airflow.sdk.definitions._internal.abstractoperator import TriggerRule
from testing_prototype import DB_FILE
from testing_prototype.dag_runner import TIMINGS, DagResult, run_dag

Check = tuple[str, bool, str]


def eq(label: str, actual: Any, expected: Any) -> Check:
    return (label, actual == expected, f"expected {expected!r}, got {actual!r}")


class WaitBriefly(BaseOperator):
    def execute(self, context: Any) -> Any:
        self.defer(trigger=TimeDeltaTrigger(timedelta(seconds=1)), method_name="finish")

    def finish(self, context: Any, event: Any = None) -> str:
        return "resumed"


def d_linear_chain() -> list[Check]:
    with DAG("d_linear", schedule=None) as dag:

        @task
        def extract() -> list[int]:
            return [10, 20, 30]

        @task
        def total(values: list[int]) -> int:
            return sum(values)

        @task
        def report(amount: int) -> str:
            return f"total={amount}"

        report(total(extract()))
    r = run_dag(dag)
    return [
        eq("run state", r.state, "success"),
        eq("order", r.order, ["extract", "total", "report"]),
        eq("final value", r.tasks["report"].return_value, "total=60"),
    ]


def d_branch_and_trigger_rule() -> list[Check]:
    with DAG("d_branch", schedule=None, params={"mode": "full"}) as dag:

        @task.branch
        def choose(**context: Any) -> str:
            return "full_load" if context["params"]["mode"] == "full" else "incremental_load"

        full = EmptyOperator(task_id="full_load")
        incremental = EmptyOperator(task_id="incremental_load")
        strict = EmptyOperator(task_id="notify_strict")
        tolerant = EmptyOperator(
            task_id="notify_tolerant", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )
        choose() >> [full, incremental]
        [full, incremental] >> strict
        [full, incremental] >> tolerant
    r = run_dag(dag)
    return [
        eq("run state", r.state, "success"),
        eq(
            "states",
            r.states,
            {
                "choose": "success",
                "full_load": "success",
                "incremental_load": "skipped",
                "notify_strict": "skipped",
                "notify_tolerant": "success",
            },
        ),
    ]


def d_upstream_failure_propagates() -> list[Check]:
    with DAG("d_failure", schedule=None) as dag:

        @task
        def boom() -> None:
            raise ValueError("bad input")

        @task
        def after() -> str:
            return "never"

        @task(trigger_rule=TriggerRule.ALL_DONE)
        def cleanup() -> str:
            return "cleaned"

        failed = boom()
        failed >> after()
        failed >> cleanup()
    r = run_dag(dag)
    return [
        eq("run state", r.state, "failed"),
        eq("states", r.states, {"boom": "failed", "after": "upstream_failed", "cleanup": "success"}),
    ]


def d_dynamic_mapping() -> list[Check]:
    with DAG("d_mapped", schedule=None) as dag:

        @task
        def files() -> list[str]:
            return ["a.csv", "b.csv", "c.csv"]

        @task
        def size(name: str) -> int:
            return len(name)

        @task
        def total(sizes: list[int]) -> int:
            return sum(sizes)

        total(size.expand(name=files()))
    r = run_dag(dag)
    return [
        eq("run state", r.state, "success"),
        eq(
            "mapped instances",
            sorted(k for k in r.tasks if k.startswith("size")),
            ["size[0]", "size[1]", "size[2]"],
        ),
        eq("reduce over mapped", r.tasks["total"].return_value, 15),
    ]


def d_retry_then_succeed() -> list[Check]:
    attempts: list[int] = []
    with DAG("d_retry", schedule=None) as dag:

        @task(retries=2, retry_delay=timedelta(seconds=0))
        def flaky(**context: Any) -> str:
            attempts.append(context["ti"].try_number)
            if len(attempts) < 2:
                raise ConnectionError("try again")
            return "ok"

        flaky()
    r = run_dag(dag)
    return [
        eq("run state", r.state, "success"),
        eq("attempts made", attempts, [1, 2]),
        eq("final try number", r.tasks["flaky"].try_number, 2),
    ]


def d_deferrable() -> list[Check]:
    with DAG("d_defer", schedule=None) as dag:
        waited = WaitBriefly(task_id="wait")

        @task
        def after() -> str:
            return "done"

        waited >> after()
    r = run_dag(dag)
    return [
        eq("run state", r.state, "success"),
        eq("resumed value", r.tasks["wait"].return_value, "resumed"),
        eq("downstream ran", r.tasks["after"].state, "success"),
    ]


def d_setup_teardown() -> list[Check]:
    with DAG("d_setup", schedule=None) as dag:

        @task
        def create_cluster() -> str:
            return "cluster-1"

        @task
        def work(cluster: str) -> None:
            raise RuntimeError("job failed")

        @task
        def delete_cluster(cluster: str) -> str:
            return f"deleted {cluster}"

        cluster = create_cluster()
        work(cluster) >> delete_cluster(cluster).as_teardown(setups=cluster)
    r = run_dag(dag)
    return [
        eq("teardown ran after failure", r.tasks["delete_cluster"].return_value, "deleted cluster-1"),
        eq("run state follows the work task", r.state, "failed"),
    ]


def d_asset_outlet() -> list[Check]:
    with DAG("d_asset", schedule=None) as dag:

        @task(outlets=[Asset("s3://bucket/orders.parquet")])
        def publish() -> int:
            return 1

        publish()
    r = run_dag(dag)
    return [eq("run state", r.state, "success")]


SCENARIOS: list[Callable[[], list[Check]]] = [
    d_linear_chain,
    d_branch_and_trigger_rule,
    d_upstream_failure_propagates,
    d_dynamic_mapping,
    d_retry_then_succeed,
    d_deferrable,
    d_setup_teardown,
    d_asset_outlet,
]


def repeated_runs(runs: int = 20) -> list[Check]:
    """Re-run one Dag many times to see the steady-state cost and whether runs interfere."""
    with DAG("d_repeat", schedule=None) as dag:

        @task
        def one() -> int:
            return 1

        @task
        def two(value: int) -> int:
            return value + 1

        two(one())
    results: list[DagResult] = [run_dag(dag) for _ in range(runs)]
    average = sum(r.seconds for r in results) / runs
    print(f"    {runs} runs of a two-task Dag, {average * 1000:.0f} ms per run")
    return [
        eq("every run succeeds", {r.state for r in results}, {"success"}),
        eq("every run sees its own result", {r.tasks["two"].return_value for r in results}, {2}),
    ]


def main() -> int:
    passed = failed = 0
    for scenario in [*SCENARIOS, repeated_runs]:
        name = scenario.__name__.removeprefix("d_")
        started = time.monotonic()
        try:
            checks = scenario()
        except Exception as exc:
            checks = [("harness", False, f"{type(exc).__name__}: {str(exc).splitlines()[0][:220]}")]
        ok = all(c[1] for c in checks)
        passed, failed = passed + ok, failed + (not ok)
        print(f"{'PASS' if ok else 'FAIL'}  {name}  ({time.monotonic() - started:.2f}s)")
        for label, good, detail in checks:
            if not good:
                print(f"      x {label}: {detail}")
    print(f"\n{passed} passed, {failed} failed")
    print(f"database setup: {TIMINGS.get('database setup', 0):.2f}s, file created: {DB_FILE.exists()}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
