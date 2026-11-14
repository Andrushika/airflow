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

`../spike_dbfree_run_task.py` is the first, single-file version of the task-level idea.

## Running it

From the `dev` directory:

```bash
uv run --project ../task-sdk python -m testing_prototype.task_scenarios
uv run --project ../task-sdk python -m testing_prototype.dag_scenarios
uv run --project ../task-sdk python -m testing_prototype.fidelity
```

Task logs go to stdout and stderr; pipe through `grep -E '^(PASS|FAIL|SAME|DIFF)'` for the verdicts.

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

## Known limits

- Not exercised: human-in-the-loop tasks, virtualenv operators, `TriggerDagRunOperator`,
  asset-triggered runs across Dags, executor-driven runs.
- `render_task` reaches into private Task SDK internals. A real implementation needs a proper seam.
- A Dag has to be defined in a file. One built from standard input cannot be written to the
  database.
