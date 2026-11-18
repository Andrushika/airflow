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
Research prototype for a public Dag-author testing API.

Throwaway code that backs the feasibility claims of an AIP draft. Run the scenario
modules from the ``dev`` directory, for example::

    cd dev && uv run --project ../task-sdk python -m testing_prototype.task_scenarios
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

# ``import airflow`` reads its configuration and builds a SQLAlchemy engine immediately, so the
# environment has to be in place before any submodule of this package imports Airflow. SQLite only
# creates its file on first connection, which makes ``DB_FILE.exists()`` the test for database access.
AIRFLOW_HOME = Path(tempfile.mkdtemp(prefix="testing_prototype_home_"))
DB_FILE = AIRFLOW_HOME / "airflow.db"
os.environ["AIRFLOW_HOME"] = str(AIRFLOW_HOME)
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{DB_FILE}"
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
