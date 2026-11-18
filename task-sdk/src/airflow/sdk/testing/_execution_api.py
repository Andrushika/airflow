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
"""An in-memory stand-in for the Execution API, serving one task instance from dictionaries."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import Any

import httpx2

from airflow.sdk.api.client import Client
from airflow.sdk.api.datamodels._generated import (
    ConnectionResponse,
    DagRun,
    TIRunContext,
)

XComKey = tuple[str, int]
Handler = Callable[[re.Match[str], httpx2.Request], httpx2.Response]

_NOT_FOUND = {"detail": {"reason": "not_found"}}
_TI = r"task-instances/[^/]+"
_XCOM = r"xcoms/[^/]+/[^/]+/([^/]+)/([^/]+)"


class _NoRetryClient(Client):
    def request(self, *args: Any, **kwargs: Any) -> httpx2.Response:
        kwargs["retry"] = False
        return super().request(*args, **kwargs)


class InMemoryExecutionAPI:
    """
    Answer the Execution API requests of one task instance from dictionaries.

    A request it does not implement gets a 501 and is recorded in ``unhandled``, so the caller can
    fail loudly instead of letting the task see an answer the real server would not give.
    """

    def __init__(self, dag_run: DagRun, *, max_tries: int, should_retry: bool) -> None:
        self.run_context = TIRunContext(dag_run=dag_run, max_tries=max_tries, should_retry=should_retry)
        self.xcoms: dict[str, dict[XComKey, Any]] = {}
        self.variables: dict[str, str] = {}
        self.connections: dict[str, ConnectionResponse] = {}
        self.task_store: dict[str, Any] = {}
        self.skipped_downstream: list[str | tuple[str, int]] = []
        self.unhandled: list[str] = []
        self._routes: list[tuple[str, re.Pattern[str], Handler]] = [
            ("PATCH", re.compile(rf"{_TI}/run"), self._start),
            ("PATCH", re.compile(rf"{_TI}/state"), self._no_content),
            ("PUT", re.compile(rf"{_TI}/heartbeat"), self._no_content),
            ("PUT", re.compile(rf"{_TI}/rtif"), self._created),
            ("PATCH", re.compile(rf"{_TI}/rendered-map-index"), self._no_content),
            ("PATCH", re.compile(rf"{_TI}/skip-downstream"), self._skip_downstream),
            ("GET", re.compile(rf"{_TI}/validate-inlets-and-outlets"), self._validate_assets),
            ("GET", re.compile(r"task-reschedules/[^/]+/start_date"), self._reschedule_start),
            ("GET", re.compile(rf"{_XCOM}/slice"), self._xcom_slice),
            ("GET", re.compile(rf"{_XCOM}/item/(-?\d+)"), self._xcom_item),
            ("GET", re.compile(_XCOM), self._xcom_get),
            ("HEAD", re.compile(_XCOM), self._xcom_head),
            ("POST", re.compile(_XCOM), self._xcom_set),
            ("DELETE", re.compile(_XCOM), self._xcom_delete),
            ("GET", re.compile(r"connections/(.+)"), self._connection),
            ("GET", re.compile(r"variables/(.+)"), self._variable_get),
            ("PUT", re.compile(r"variables/(.+)"), self._variable_put),
            ("GET", re.compile(r"store/ti/[^/]+/(.+)"), self._store_get),
            ("PUT", re.compile(r"store/ti/[^/]+/(.+)"), self._store_put),
            ("DELETE", re.compile(r"store/ti/[^/]+/(.+)"), self._store_delete),
        ]

    def client(self) -> Client:
        client = _NoRetryClient(base_url=None, dry_run=True, token="", transport=httpx2.MockTransport(self))
        client.base_url = "http://in-memory.invalid./"  # type: ignore[assignment]
        return client

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        path = request.url.path.lstrip("/")
        for method, pattern, handler in self._routes:
            if request.method == method and (match := pattern.fullmatch(path)):
                return handler(match, request)
        self.unhandled.append(f"{request.method} {path}")
        return httpx2.Response(501, json={"detail": f"not implemented in memory: {request.method} {path}"})

    def _start(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, content=self.run_context.model_dump_json())

    def _no_content(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(204)

    def _created(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(201, json={"message": "ok"})

    def _skip_downstream(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        tasks = json.loads(request.content)["tasks"]
        self.skipped_downstream.extend(t if isinstance(t, str) else (t[0], t[1]) for t in tasks)
        return httpx2.Response(204)

    def _validate_assets(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, json={"inactive_assets": []})

    def _reschedule_start(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, json=None)

    def _values(self, match: re.Match[str]) -> list[Any]:
        found = self.xcoms.get(match.group(2), {})
        return [value for (task_id, _), value in sorted(found.items()) if task_id == match.group(1)]

    def _xcom_slice(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, json=self._values(match))

    def _xcom_item(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        values = self._values(match)
        offset = int(match.group(3))
        if -len(values) <= offset < len(values):
            return httpx2.Response(200, json=values[offset])
        return httpx2.Response(404, json=_NOT_FOUND)

    def _xcom_key(self, match: re.Match[str], request: httpx2.Request) -> XComKey:
        return match.group(1), int(request.url.params.get("map_index", -1))

    def _xcom_get(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        found = self.xcoms.get(match.group(2), {})
        key = self._xcom_key(match, request)
        if key in found:
            return httpx2.Response(200, json={"key": match.group(2), "value": found[key]})
        return httpx2.Response(404, json=_NOT_FOUND)

    def _xcom_head(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        count = len(self._values(match))
        return httpx2.Response(200, headers={"Content-Range": f"map_indexes {count}"})

    def _xcom_set(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        self.xcoms.setdefault(match.group(2), {})[self._xcom_key(match, request)] = json.loads(
            request.content
        )
        return httpx2.Response(201, json={"message": "ok"})

    def _xcom_delete(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        self.xcoms.get(match.group(2), {}).pop(self._xcom_key(match, request), None)
        return httpx2.Response(200, json={"message": "ok"})

    def _connection(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        if conn := self.connections.get(match.group(1)):
            return httpx2.Response(200, content=conn.model_dump_json(by_alias=True))
        return httpx2.Response(404, json=_NOT_FOUND)

    def _variable_get(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        if match.group(1) in self.variables:
            return httpx2.Response(200, json={"key": match.group(1), "value": self.variables[match.group(1)]})
        return httpx2.Response(404, json=_NOT_FOUND)

    def _variable_put(self, match: re.Match[str], request: httpx2.Request) -> httpx2.Response:
        self.variables[match.group(1)] = json.loads(request.content)["val"]
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
