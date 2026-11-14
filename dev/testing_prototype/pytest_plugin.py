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
Expose the prototype runners as pytest fixtures.

Loading this module imports the package first, which points Airflow at a throwaway home before
any test module can import Airflow. Each pytest-xdist worker is a separate process and so gets
its own home and database.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

import testing_prototype  # noqa: F401  -- must run before Airflow is imported


@pytest.fixture
def run_task() -> Callable[..., Any]:
    """Run one task with no metadata database."""
    from testing_prototype.task_runner import run_task as _run_task

    return _run_task


@pytest.fixture
def render_task() -> Callable[..., Any]:
    """Render a task's template fields without executing it."""
    from testing_prototype.task_runner import render_task as _render_task

    return _render_task


@pytest.fixture
def run_dag() -> Callable[..., Any]:
    """Run a whole Dag on a throwaway SQLite database, created on first use."""
    from testing_prototype.dag_runner import run_dag as _run_dag

    return _run_dag
