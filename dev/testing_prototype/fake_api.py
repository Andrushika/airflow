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
"""An Execution API stand-in that answers task requests from plain dictionaries."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import Any

import httpx2

from airflow.sdk.api.client import Client

XComKey = tuple[str, str, str, str, int]
Handler = Callable[[re.Match[str], httpx2.Request], httpx2.Response]

_NOT_FOUND = {"detail": {"reason": "not_found"}}


class _NoRetryClient(Client):
    """Skip the client's retry loop so an unimplemented endpoint fails once instead of five times."""

    def request(self, *args: Any, **kwargs: Any) -> httpx2.Response:
        kwargs["retry"] = False
        return super().request(*args, **kwargs)


class InMemoryExecutionAPI:
    """
    Serve the Execution API endpoints a task talks to, backed by dictionaries.

    An endpoint that is not implemented returns 501 and is recorded in ``unhandled``: a canned
    success would let the fake drift from the real server without anyone noticing.
    """

    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str = "test_run",
        logical_date: str | None = "2026-01-01T00:00:00Z",
        conf: dict[str, Any] | None = None,
        max_tries: int = 0,
        should_retry: bool = False,
        next_method: str | None = None,
        next_kwargs: dict[str, Any] | None = None,
        task_reschedule_count: int = 0,
    ) -> None:
        self.dag_id = dag_id
        self.run_id = run_id
        self.logical_date = logical_date
        self.conf = conf
        self.max_tries = max_tries
        self.should_retry = should_retry
        self.next_method = next_method
        self.next_kwargs = next_kwargs
        self.task_reschedule_count = task_reschedule_count

        self.xcoms: dict[XComKey, Any] = {}
        self.connections: dict[str, dict[str, Any]] = {}
        self.variables: dict[str, str] = {}
        self.task_store: dict[str, Any] = {}
        self.state_updates: list[dict[str, Any]] = []
        self.skipped_downstream: list[Any] = []
        self.rendered_fields: dict[str, Any] = {}
        self.requests: list[str] = []
        self.unhandled: list[str] = []

        ti = r"task-instances/[^/]+"
        xcom = r"xcoms/([^/]+)/([^/]+)/([^/]+)/([^/]+)"
        self._routes: list[tuple[str, re.Pattern[str], Handler]] = [
            ("PATCH", re.compile(rf"{ti}/run"), self._start),
            ("PATCH", re.compile(rf"{ti}/state"), self._state),
            ("PUT", re.compile(rf"{ti}/heartbeat"), self._no_content),
            ("PUT", re.compile(rf"{ti}/rtif"), self._rtif),
            ("PATCH", re.compile(rf"{ti}/rendered-map-index"), self._no_content),
            ("PATCH", re.compile(rf"{ti}/skip-downstream"), self._skip_downstream),
            ("GET", re.compile(rf"{ti}/validate-inlets-and-outlets"), self._validate_assets),
            ("GET", re.compile(r"task-reschedules/[^/]+/start_date"), self._reschedule_start),
            ("GET", re.compile(rf"{xcom}/slice"), self._xcom_slice),
            ("GET", re.compile(xcom), self._xcom_get),
            ("HEAD", re.compile(xcom), self._xcom_head),
            ("POST", re.compile(xcom), self._xcom_set),
            ("DELETE", re.compile(xcom), self._xcom_delete),
            ("GET", re.compile(r"connections/(.+)"), self._connection),
            ("GET", re.compile(r"variables/(.+)"), self._variable_get),
            ("PUT", re.compile(r"variables/(.+)"), self._variable_put),
            ("GET", re.compile(r"store/ti/[^/]+/(.+)"), self._store_get),
            ("PUT", re.compile(r"store/ti/[^/]+/(.+)"), self._store_put),
            ("DELETE", re.compile(r"store/ti/[^/]+/(.+)"), self._store_delete),
        ]

    def client(self) -> Client:
        client = _NoRetryClient(base_url=None, dry_run=True, token="", transport=httpx2.MockTransport(self))
        client.base_url = "http://in-process.invalid./"
        return client

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        path = request.url.path.lstrip("/")
        self.requests.append(f"{request.method} {path}")
        for method, pattern, handler in self._routes:
            if request.method == method and (match := pattern.fullmatch(path)):
                return handler(match, request)
        self.unhandled.append(f"{request.method} {path}")
        return httpx2.Response(
            501, json={"detail": f"in-memory API does not implement {request.method} {path}"}
        )

    def _xcom_key(self, match: re.Match[str], request: httpx2.Request) -> XComKey:
        map_index = int(request.url.params.get("map_index", -1))
        return (match.group(1), match.group(2), match.group(3), match.group(4), map_index)

    def _start(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            200,
            json={
                "dag_run": {
                    "dag_id": self.dag_id,
                    "run_id": self.run_id,
                    "logical_date": self.logical_date,
                    "data_interval_start": self.logical_date,
                    "data_interval_end": self.logical_date,
                    "run_after": self.logical_date or "2026-01-01T00:00:00Z",
                    "start_date": self.logical_date,
                    "end_date": None,
                    "run_type": "manual",
                    "state": "running",
                    "conf": self.conf,
                    "consumed_asset_events": [],
                    "partition_key": None,
                },
                "max_tries": self.max_tries,
                "should_retry": self.should_retry,
                "task_reschedule_count": self.task_reschedule_count,
                "next_method": self.next_method,
                "next_kwargs": self.next_kwargs,
            },
        )

    def _state(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        self.state_updates.append(json.loads(request.content))
        return httpx2.Response(204)

    def _no_content(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(204)

    def _rtif(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        self.rendered_fields = json.loads(request.content)
        return httpx2.Response(201, json={"message": "ok"})

    def _skip_downstream(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        self.skipped_downstream.extend(json.loads(request.content).get("tasks", []))
        return httpx2.Response(204)

    def _validate_assets(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, json={"inactive_assets": []})

    def _reschedule_start(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, json=None)

    def _xcom_slice(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        prefix = (match.group(1), match.group(2), match.group(3), match.group(4))
        found = sorted((key[4], value) for key, value in self.xcoms.items() if key[:4] == prefix)
        return httpx2.Response(200, json=[value for _, value in found])

    def _xcom_get(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        key = self._xcom_key(match, request)
        if key in self.xcoms:
            return httpx2.Response(200, json={"key": key[3], "value": self.xcoms[key]})
        return httpx2.Response(404, json=_NOT_FOUND)

    def _xcom_head(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        prefix = (match.group(1), match.group(2), match.group(3), match.group(4))
        count = sum(1 for key in self.xcoms if key[:4] == prefix)
        return httpx2.Response(200, headers={"Content-Range": f"map_indexes {count}"})

    def _xcom_set(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        self.xcoms[self._xcom_key(match, request)] = json.loads(request.content)
        return httpx2.Response(201, json={"message": "ok"})

    def _xcom_delete(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        self.xcoms.pop(self._xcom_key(match, request), None)
        return httpx2.Response(200, json={"message": "ok"})

    def _connection(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        if fields := self.connections.get(match.group(1)):
            defaults = dict.fromkeys(("host", "schema", "login", "password", "port", "extra"))
            return httpx2.Response(200, json={**defaults, "conn_id": match.group(1), **fields})
        return httpx2.Response(404, json=_NOT_FOUND)

    def _variable_get(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        if match.group(1) in self.variables:
            return httpx2.Response(200, json={"key": match.group(1), "value": self.variables[match.group(1)]})
        return httpx2.Response(404, json=_NOT_FOUND)

    def _variable_put(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        self.variables[match.group(1)] = json.loads(request.content)["value"]
        return httpx2.Response(201, json={"message": "ok"})

    def _store_get(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        if match.group(1) in self.task_store:
            return httpx2.Response(200, json={"value": self.task_store[match.group(1)]})
        return httpx2.Response(404, json=_NOT_FOUND)

    def _store_put(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        self.task_store[match.group(1)] = json.loads(request.content)["value"]
        return httpx2.Response(204)

    def _store_delete(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        self.task_store.pop(match.group(1), None)
        return httpx2.Response(204)
