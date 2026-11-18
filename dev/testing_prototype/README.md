<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Testing API prototype](#testing-api-prototype)
  - [What is in here](#what-is-in-here)
  - [Running it](#running-it)
  - [Results](#results)
  - [What it found in existing code](#what-it-found-in-existing-code)
  - [Known limits](#known-limits)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Testing API prototype

Throwaway research code behind a draft AIP for a public Dag-author testing API. It answers one
question per module: does this approach work on current `main`, and what breaks?

Nothing here is proposed for merging as is.

## What is in here

| Module | What it does |
|---|---|
| `fake_api.py` | An in-memory stand-in for the Execution API: 19 routes backed by dictionaries. Unimplemented routes fail loudly. |
| `task_runner.py` | `run_task` and `render_task`: one task through the real Task SDK runner, no metadata database. |
| `dag_runner.py` | `run_dag`: a whole Dag through `dag.test()` on a throwaway SQLite database, returning detached results. |
| `pytest_plugin.py` | The three functions as pytest fixtures. |
| `task_scenarios.py` | 26 task-level scenarios plus a repeated-run check for leaked process state. |
| `dag_scenarios.py` | 9 Dag-level scenarios plus repeated runs. |
| `fidelity.py` | 16 scenarios run against both the in-memory API and the real API server, compared field by field. |
| `dag_inmemory.py` | `run_dag_in_memory`: a whole Dag with no database and no scheduler. Task instances, trigger rules, mapping and the run state are decided from a dictionary. |
| `dag_fidelity_inmemory.py` | 41 Dag shapes run through `dag.test()` and through `run_dag_in_memory`, compared task instance by task instance. |
| `docs_style_cases.py` | 7 tests written the way the docs suggest, next to a real run and an in-memory run of the same task. |
| `dagtest_as_harness.py` | `dag.test()` used the way a test would use it, on a fresh home. Run it as a script. |

`../spike_dbfree_run_task.py` is the first, single-file version of the task-level idea.

## Running it

From the `dev` directory:

```bash
uv run --project ../task-sdk python -m testing_prototype.task_scenarios
uv run --project ../task-sdk python -m testing_prototype.dag_scenarios
uv run --project ../task-sdk python -m testing_prototype.fidelity
uv run --project ../task-sdk python -m testing_prototype.dag_fidelity_inmemory
uv run --project ../task-sdk python -m testing_prototype.docs_style_cases
uv run --project ../task-sdk python testing_prototype/dagtest_as_harness.py
```

Task logs go to stdout and stderr; pipe through `grep -E '^(PASS|FAIL|SAME|DIFF|GAP|RESULT|#####|  [a-z])'`
for the verdicts.

To try the fixtures on your own tests:

```bash
PYTHONPATH=/path/to/airflow/dev pytest -p testing_prototype.pytest_plugin your_tests/
```

## Results

Measured on `main` at `1cbcc306`, Python 3.13, SQLite, in one container. Indicative only.

| Suite | Result | Cost |
|---|---|---|
| Task level | 26 of 26 pass, no database file created | about 3 ms per task run |
| Fidelity | 16 of 16 identical between the in-memory API and the real server | |
| Dag level | 9 of 9 pass, with Dags defined inside the test module | database 0.5–0.8 s once, first run about 4.6 s, then 0.1–0.16 s per run |

### Whole Dag without a database

Measured on `main` at `542ab65b`. The first three suites above still pass there.

| What | Result |
|---|---|
| 41 Dag shapes, `dag.test()` against `run_dag_in_memory` | 40 identical in every task state, return value and run state; 307 task instances compared |
| Trigger rules | 12 rules against 10 pairs of upstream outcomes, all identical |
| Time inside the runs | 33.4 s through `dag.test()`, 2.2 s in memory, of which 1 s is a real trigger wait |
| Fresh process, in memory only | 1.3 s of imports, 74 ms for the first Dag run, 14 ms for the next, no database file |

The runner follows the scheduler loop: each round takes a snapshot of the states, decides from the
snapshot what changes and what may run, then runs it. Without the snapshot two shapes differed, a
task with `trigger_rule="always"` below a short-circuit and `all_success` over one skipped and one
upstream-failed task.

### Tests written the way the docs suggest

All seven pass. The same task fails, or returns something else, in a real run and in memory:

| Case | Docs-style test | Real run |
|---|---|---|
| `{{ ds }}` in a run without a logical date | passes | `'ds' is undefined` |
| `context["execution_date"]` | passes | `KeyError` |
| `xcom_pull` with a typo in the task id, mocked `ti` | passes | `'NoneType' object is not iterable` |
| TaskFlow task returns an object XCom cannot serialize | passes | fails |
| `multiple_outputs=True` returns a list | passes | fails |
| Branch returns a task id that does not exist | passes | fails |
| Mapped task expects the whole list | returns 2 | returns 5 for each element |

The docs-style test also changes the operator it tests. `render_template_fields` replaces the
template with its result, and `PythonOperator.execute` stores the hand-built context in `op_kwargs`.
A later run of the same operator in the same process uses those values.

### `dag.test()` used as a test harness

| Situation | What happens |
|---|---|
| No database prepared | `no such table: task_instance` |
| Dag defined inside the test | `Cannot create DagRun ... because the dag is not serialized` |
| First run in a process | about 6.5 s, then about 0.2 s per run |
| A task raises | a failed `DagRun` comes back; the exception is only in the log |
| `retries=1, retry_delay=8s` | waits 8 s |
| Rescheduling sensor, `poke_interval=6` | waits 6 s |
| The same test three times, task increments a Variable | returns 0, then 1, then 2 |
| Per-task states and values | ORM queries |

## What it found in existing code

- `Client(dry_run=True)` returns a run context that no longer validates against `TIRunContext`.
- `InProcessTestSupervisor` holds one file descriptor and one thread per run until garbage
  collection or a one-second poll releases them, and leaves `AIRFLOW_CTX_*` variables set.
- Run `conf` leaks into the shared Dag object across in-process runs. An upstream fix is open as
  PR #75918.
- `dag.test()` waits out the retry delay in real time and does not return the exception a task
  raised.
- The first Dag-level run in a process spends about 4.6 s building the versioned Execution API
  application.
- `import airflow` builds a database engine, so the throwaway home has to be configured before any
  test module imports Airflow. The package `__init__` does that.

- `tests_common.run_task_no_db` already wraps the dry-run client to remember XCom writes and to
  replace its stale run context. It cannot give a task its upstream values, and every other request
  gets the dry-run reply `{"text": "Hello, world!"}`.

## Known limits

- `run_dag_in_memory` does not expand mapped task groups, and it runs a deferred task's trigger in
  real time.
- The in-memory run loads 90 `airflow-core` modules through imports the Task SDK still has. It uses
  no database, no scheduler and no API server.

- Not exercised: human-in-the-loop tasks, virtualenv operators, `TriggerDagRunOperator`,
  asset-triggered runs across Dags, executor-driven runs.
- `render_task` reaches into private Task SDK internals. A real implementation needs a proper seam.
- A Dag has to be defined in a file. One built from standard input cannot be written to the
  database.
