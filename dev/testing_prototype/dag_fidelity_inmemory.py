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
Run the same Dag through the real scheduler path and through the in-memory runner, and compare.

The real side is ``dag.test()`` on SQLite: real scheduler decisions, real API server, real database.
The in-memory side uses no database. Any difference in a task state, a return value or the run
state is printed.
"""

from __future__ import annotations

import itertools
import sys
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger
from airflow.sdk import DAG, Asset, BaseOperator, TaskGroup, Variable, task, task_group, timezone
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.sdk.exceptions import AirflowSkipException
from testing_prototype import DB_FILE
from testing_prototype.dag_inmemory import run_dag_in_memory
from testing_prototype.dag_runner import run_dag

LOGICAL_DATE = datetime(2026, 3, 5, tzinfo=timezone.utc)
RULES = [
    "all_success",
    "all_failed",
    "all_done",
    "all_done_min_one_success",
    "all_skipped",
    "one_success",
    "one_failed",
    "one_done",
    "none_failed",
    "none_failed_min_one_success",
    "none_skipped",
    "always",
]
OUTCOMES = ["success", "failed", "skipped", "upstream_failed"]


class WaitBriefly(BaseOperator):
    def execute(self, context: Any) -> Any:
        self.defer(trigger=TimeDeltaTrigger(timedelta(seconds=1)), method_name="finish")

    def finish(self, context: Any, event: Any = None) -> str:
        return "resumed"


def _outcome_task(dag_prefix: str, name: str, outcome: str) -> Any:
    """A task that ends in the requested state."""
    if outcome == "upstream_failed":

        @task(task_id=f"{name}_cause")
        def cause() -> None:
            raise ValueError("cause")

        @task(task_id=name)
        def blocked() -> int:
            return 1

        result = blocked()
        cause() >> result
        return result

    @task(task_id=name)
    def run() -> int:
        if outcome == "failed":
            raise ValueError("failed on purpose")
        if outcome == "skipped":
            raise AirflowSkipException("skipped on purpose")
        return 1

    return run()


def rule_matrix(first: str, second: str) -> Callable[[], DAG]:
    def build() -> DAG:
        with DAG(f"m_{first}_{second}", schedule=None) as dag:
            a = _outcome_task(dag.dag_id, "a", first)
            b = _outcome_task(dag.dag_id, "b", second)
            for rule in RULES:
                [a, b] >> EmptyOperator(task_id=f"rule_{rule}", trigger_rule=rule)
        return dag

    build.__name__ = f"rules: upstream {first} + {second}"
    return build


def linear_chain() -> DAG:
    with DAG("i_linear", schedule=None) as dag:

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
    return dag


def branch_and_trigger_rule() -> DAG:
    with DAG("i_branch", schedule=None, params={"mode": "full"}) as dag:

        @task.branch
        def choose(**context: Any) -> str:
            return "full_load" if context["params"]["mode"] == "full" else "incremental_load"

        full = EmptyOperator(task_id="full_load")
        incremental = EmptyOperator(task_id="incremental_load")
        choose() >> [full, incremental]
        [full, incremental] >> EmptyOperator(task_id="notify_strict")
        [full, incremental] >> EmptyOperator(
            task_id="notify_tolerant", trigger_rule="none_failed_min_one_success"
        )
    return dag


def branch_to_several() -> DAG:
    with DAG("i_branch_many", schedule=None) as dag:

        @task.branch
        def choose() -> list[str]:
            return ["a", "c"]

        a, b, c = (EmptyOperator(task_id=name) for name in "abc")
        choose() >> [a, b, c]
        a >> EmptyOperator(task_id="after_a")
        b >> EmptyOperator(task_id="after_b")
        [a, b, c] >> EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")
    return dag


def short_circuit() -> DAG:
    with DAG("i_short", schedule=None) as dag:

        @task.short_circuit
        def has_data() -> bool:
            return False

        first = EmptyOperator(task_id="first")
        second = EmptyOperator(task_id="second")
        always = EmptyOperator(task_id="always", trigger_rule="always")
        has_data() >> first >> second >> always
    return dag


def short_circuit_respecting_rules() -> DAG:
    with DAG("i_short_rules", schedule=None) as dag:

        @task.short_circuit(ignore_downstream_trigger_rules=False)
        def has_data() -> bool:
            return False

        first = EmptyOperator(task_id="first")
        done = EmptyOperator(task_id="cleanup", trigger_rule="all_done")
        has_data() >> first >> done
    return dag


def upstream_failure() -> DAG:
    with DAG("i_failure", schedule=None) as dag:

        @task
        def boom() -> None:
            raise ValueError("bad input")

        @task
        def after() -> str:
            return "never"

        @task
        def after_after() -> str:
            return "never"

        @task(trigger_rule="all_done")
        def cleanup() -> str:
            return "cleaned"

        failed = boom()
        failed >> after() >> after_after()
        failed >> cleanup()
    return dag


def dynamic_mapping() -> DAG:
    with DAG("i_mapped", schedule=None) as dag:

        @task
        def files() -> list[str]:
            return ["a.csv", "bb.csv", "ccc.csv"]

        @task
        def size(name: str) -> int:
            return len(name)

        @task
        def total(sizes: list[int]) -> int:
            return sum(sizes)

        total(size.expand(name=files()))
    return dag


def mapping_literal_and_partial() -> DAG:
    with DAG("i_mapped_literal", schedule=None) as dag:

        @task
        def scale(value: int, factor: int) -> int:
            return value * factor

        @task
        def collect(values: list[int]) -> list[int]:
            return sorted(values)

        collect(scale.partial(factor=10).expand(value=[1, 2, 3]))
    return dag


def mapping_cross_product() -> DAG:
    with DAG("i_mapped_cross", schedule=None) as dag:

        @task
        def pair(letter: str, number: int) -> str:
            return f"{letter}{number}"

        @task
        def collect(values: list[str]) -> list[str]:
            return sorted(values)

        collect(pair.expand(letter=["a", "b"], number=[1, 2, 3]))
    return dag


def mapping_over_nothing() -> DAG:
    with DAG("i_mapped_empty", schedule=None) as dag:

        @task
        def files() -> list[str]:
            return []

        @task
        def size(name: str) -> int:
            return len(name)

        @task
        def strict(sizes: list[int]) -> int:
            return len(sizes)

        @task(trigger_rule="none_failed")
        def tolerant() -> str:
            return "ran"

        sizes = size.expand(name=files())
        strict(sizes)
        sizes >> tolerant()
    return dag


def mapping_with_one_failure() -> DAG:
    with DAG("i_mapped_failure", schedule=None) as dag:

        @task
        def check(value: int) -> int:
            if value == 2:
                raise ValueError("bad row")
            return value

        @task
        def strict(values: list[int]) -> int:
            return sum(values)

        @task(trigger_rule="all_done")
        def cleanup() -> str:
            return "cleaned"

        checked = check.expand(value=[1, 2, 3])
        strict(checked)
        checked >> cleanup()
    return dag


def mapping_chain() -> DAG:
    with DAG("i_mapped_chain", schedule=None) as dag:

        @task
        def numbers() -> list[int]:
            return [1, 2, 3]

        @task
        def double(value: int) -> int:
            return value * 2

        @task
        def plus_one(value: int) -> int:
            return value + 1

        @task
        def collect(values: list[int]) -> list[int]:
            return sorted(values)

        collect(plus_one.expand(value=double.expand(value=numbers())))
    return dag


RETRY_ATTEMPTS: dict[str, int] = {}


def retry_then_succeed() -> DAG:
    with DAG("i_retry", schedule=None) as dag:

        @task(retries=2, retry_delay=timedelta(seconds=0))
        def flaky(**context: Any) -> str:
            if context["ti"].try_number < 2:
                raise ConnectionError("try again")
            return f"ok on try {context['ti'].try_number}"

        @task
        def after(value: str) -> str:
            return value.upper()

        after(flaky())
    return dag


def retries_exhausted() -> DAG:
    with DAG("i_retry_exhausted", schedule=None) as dag:

        @task(retries=1, retry_delay=timedelta(seconds=0))
        def always_fails() -> None:
            raise ConnectionError("still down")

        @task(trigger_rule="one_failed")
        def alert() -> str:
            return "paged"

        always_fails() >> alert()
    return dag


def deferrable() -> DAG:
    with DAG("i_defer", schedule=None) as dag:
        waited = WaitBriefly(task_id="wait")

        @task
        def after() -> str:
            return "done"

        waited >> after()
    return dag


def sensor_reschedule() -> DAG:
    with DAG("i_sensor", schedule=None) as dag:

        @task.sensor(poke_interval=1, mode="reschedule", timeout=30)
        def ready() -> PokeReturnValue:
            return PokeReturnValue(is_done=True, xcom_value="file.csv")

        @task
        def load(name: str) -> str:
            return f"loaded {name}"

        load(ready())
    return dag


def setup_teardown_work_fails() -> DAG:
    with DAG("i_setup", schedule=None) as dag:

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
    return dag


def setup_teardown_work_succeeds_teardown_fails() -> DAG:
    with DAG("i_teardown_fails", schedule=None) as dag:

        @task
        def create_cluster() -> str:
            return "cluster-1"

        @task
        def work(cluster: str) -> str:
            return "worked"

        @task
        def delete_cluster(cluster: str) -> None:
            raise RuntimeError("could not delete")

        cluster = create_cluster()
        work(cluster) >> delete_cluster(cluster).as_teardown(setups=cluster)
    return dag


def setup_fails() -> DAG:
    with DAG("i_setup_fails", schedule=None) as dag:

        @task
        def create_cluster() -> str:
            raise RuntimeError("no quota")

        @task
        def work(cluster: str) -> str:
            return "worked"

        @task
        def delete_cluster(cluster: str) -> str:
            return f"deleted {cluster}"

        cluster = create_cluster()
        work(cluster) >> delete_cluster(cluster).as_teardown(setups=cluster)
    return dag


def multiple_outputs_downstream() -> DAG:
    with DAG("i_multi", schedule=None) as dag:

        @task(multiple_outputs=True)
        def stats() -> dict[str, int]:
            return {"rows": 3, "bad": 1}

        @task
        def ratio(rows: int, bad: int) -> float:
            return bad / rows

        result = stats()
        ratio(result["rows"], result["bad"])
    return dag


def variable_between_tasks() -> DAG:
    with DAG("i_variable", schedule=None) as dag:

        @task
        def remember() -> None:
            Variable.set("i_variable_marker", "from-first-task")

        @task
        def recall() -> str:
            return Variable.get("i_variable_marker")

        remember() >> recall()
    return dag


def asset_outlet() -> DAG:
    with DAG("i_asset", schedule=None) as dag:

        @task(outlets=[Asset("s3://bucket/orders.parquet")])
        def publish() -> int:
            return 1

        publish()
    return dag


def skip_propagates() -> DAG:
    with DAG("i_skip", schedule=None) as dag:

        @task
        def nothing() -> None:
            raise AirflowSkipException("no data today")

        @task
        def after() -> str:
            return "never"

        @task(trigger_rule="none_failed")
        def report() -> str:
            return "reported"

        skipped = nothing()
        skipped >> after() >> report()
    return dag


def plain_task_group() -> DAG:
    with DAG("h_group", schedule=None) as dag:

        @task
        def start() -> int:
            return 1

        with TaskGroup("etl") as etl:

            @task
            def extract(seed: int) -> list[int]:
                return [seed, seed + 1]

            @task
            def load(rows: list[int]) -> int:
                return len(rows)

            load(extract(start()))
        etl >> EmptyOperator(task_id="done")
    return dag


def expand_kwargs() -> DAG:
    with DAG("h_expand_kwargs", schedule=None) as dag:

        @task
        def jobs() -> list[dict[str, Any]]:
            return [{"name": "a", "size": 1}, {"name": "b", "size": 2}]

        @task
        def run(name: str, size: int) -> str:
            return name * size

        @task
        def collect(values: list[str]) -> list[str]:
            return sorted(values)

        collect(run.expand_kwargs(jobs()))
    return dag


def xcomarg_map() -> DAG:
    with DAG("h_map", schedule=None) as dag:

        @task
        def numbers() -> list[int]:
            return [1, 2, 3]

        @task
        def show(value: int) -> int:
            return value

        @task
        def collect(values: list[int]) -> list[int]:
            return sorted(values)

        collect(show.expand(value=numbers().map(lambda x: x * 10)))
    return dag


def xcomarg_zip() -> DAG:
    with DAG("h_zip", schedule=None) as dag:

        @task
        def letters() -> list[str]:
            return ["a", "b"]

        @task
        def numbers() -> list[int]:
            return [1, 2]

        @task
        def join(pair: tuple) -> str:
            return f"{pair[0]}{pair[1]}"

        @task
        def collect(values: list[str]) -> list[str]:
            return sorted(values)

        collect(join.expand(pair=letters().zip(numbers())))
    return dag


def mapped_task_group() -> DAG:
    with DAG("h_mapped_group", schedule=None) as dag:

        @task_group
        def process(value: int):
            @task
            def double(v: int) -> int:
                return v * 2

            @task
            def plus_one(v: int) -> int:
                return v + 1

            return plus_one(double(value))

        @task
        def collect(values: list[int]) -> list[int]:
            return sorted(values)

        collect(process.expand(value=[1, 2, 3]))
    return dag


def mapped_with_skips() -> DAG:
    with DAG("h_mapped_skip", schedule=None) as dag:

        @task
        def keep_even(value: int) -> int:
            if value % 2:
                raise AirflowSkipException("odd")
            return value

        @task(trigger_rule="none_failed")
        def collect(values: list[int]) -> list[int]:
            return sorted(values)

        collect(keep_even.expand(value=[1, 2, 3, 4]))
    return dag


def nested_branches() -> DAG:
    with DAG("h_nested_branch", schedule=None) as dag:

        @task.branch
        def outer() -> str:
            return "inner"

        @task.branch
        def inner() -> str:
            return "leaf_b"

        skipped_side = EmptyOperator(task_id="other_side")
        leaf_a, leaf_b = EmptyOperator(task_id="leaf_a"), EmptyOperator(task_id="leaf_b")
        chosen = inner()
        outer() >> [chosen, skipped_side]
        chosen >> [leaf_a, leaf_b]
        [leaf_a, leaf_b, skipped_side] >> EmptyOperator(
            task_id="join", trigger_rule="none_failed_min_one_success"
        )
    return dag


def wide_fan_out() -> DAG:
    with DAG("h_wide", schedule=None) as dag:

        @task
        def numbers() -> list[int]:
            return list(range(40))

        @task
        def square(value: int) -> int:
            return value * value

        @task
        def total(values: list[int]) -> int:
            return sum(values)

        total(square.expand(value=numbers()))
    return dag


SCENARIOS: list[Callable[[], DAG]] = [
    linear_chain,
    branch_and_trigger_rule,
    branch_to_several,
    short_circuit,
    short_circuit_respecting_rules,
    upstream_failure,
    skip_propagates,
    dynamic_mapping,
    mapping_literal_and_partial,
    mapping_cross_product,
    mapping_over_nothing,
    mapping_with_one_failure,
    mapping_chain,
    retry_then_succeed,
    retries_exhausted,
    deferrable,
    sensor_reschedule,
    setup_teardown_work_fails,
    setup_teardown_work_succeeds_teardown_fails,
    setup_fails,
    multiple_outputs_downstream,
    variable_between_tasks,
    asset_outlet,
    plain_task_group,
    expand_kwargs,
    xcomarg_map,
    xcomarg_zip,
    mapped_with_skips,
    nested_branches,
    wide_fan_out,
    mapped_task_group,
]
# Shapes the in-memory runner does not handle yet. They are reported, not hidden.
KNOWN_GAPS = {"mapped_task_group"}
MATRIX = [rule_matrix(a, b) for a, b in itertools.combinations_with_replacement(OUTCOMES, 2)]


def compare(build: Callable[[], DAG]) -> tuple[list[str], int, float, float]:
    real = run_dag(build(), logical_date=LOGICAL_DATE)
    memory = run_dag_in_memory(build(), logical_date=LOGICAL_DATE.isoformat())
    differences = []
    if real.state != memory.state:
        differences.append(f"run state: real={real.state} in-memory={memory.state}")
    for name in sorted(set(real.tasks) | set(memory.tasks)):
        r, m = real.tasks.get(name), memory.tasks.get(name)
        if r is None or m is None:
            differences.append(
                f"{name}: real={'present' if r else 'missing'} in-memory={'present' if m else 'missing'}"
            )
            continue
        if r.state != m.state:
            differences.append(f"{name}: state real={r.state} in-memory={m.state}")
        if r.return_value != m.return_value:
            differences.append(f"{name}: value real={r.return_value!r} in-memory={m.return_value!r}")
    if memory.unhandled:  # type: ignore[attr-defined]
        differences.append(f"in-memory API missing: {memory.unhandled}")  # type: ignore[attr-defined]
    return differences, len(real.tasks), real.seconds, memory.seconds


def main() -> int:
    same = different = gaps = instances = 0
    real_total = memory_total = 0.0
    for build in [*SCENARIOS, *MATRIX]:
        name = build.__name__
        started = time.monotonic()
        try:
            differences, count, real_seconds, memory_seconds = compare(build)
        except Exception as exc:
            differences, count, real_seconds, memory_seconds = (
                [f"harness {type(exc).__name__}: {str(exc).splitlines()[0][:200]}"],
                0,
                0.0,
                0.0,
            )
        instances += count
        real_total += real_seconds
        memory_total += memory_seconds
        known_gap = bool(differences) and name in KNOWN_GAPS
        gaps += known_gap
        same, different = same + (not differences), different + (bool(differences) and not known_gap)
        print(
            f"{'SAME' if not differences else 'GAP ' if known_gap else 'DIFF'}  {name:<48} {count:3d} task instances  "
            f"real {real_seconds:5.2f}s  in-memory {memory_seconds:5.3f}s  ({time.monotonic() - started:.1f}s)"
        )
        for line in differences:
            print(f"      x {line}")
    print(
        f"\n{same} identical, {different} different, {gaps} known gaps, {instances} task instances compared"
    )
    print(f"time inside the runs: real {real_total:.1f}s, in-memory {memory_total:.2f}s")
    print(f"database file used by the real side: {DB_FILE.exists()}")
    return 1 if different else 0


if __name__ == "__main__":
    sys.exit(main())
