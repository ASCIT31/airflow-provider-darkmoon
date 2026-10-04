from __future__ import annotations

import json
from types import SimpleNamespace
from unittest import mock

import pytest

from darkmoon_provider.hooks.darkmoon import DarkmoonHook


class FakeResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body

    @property
    def text(self):
        return json.dumps(self._body)


class FakeDarkmoon:
    """Route (METHOD, path-prefix) -> response or list of responses (consumed in order)."""

    def __init__(self):
        self.routes = {}
        self.calls = []

    def add(self, method, path, *responses):
        self.routes[(method, path)] = list(responses)

    def __call__(self, method, url, headers=None, timeout=None, verify=None, **kw):
        path = url.split("://", 1)[1].split("/", 1)[1]
        path = "/" + path
        self.calls.append({"method": method, "path": path, "headers": headers, "json": kw.get("json"), "verify": verify})
        for (m, p), resps in self.routes.items():
            if m == method and path.split("?")[0] == p.split("?")[0] and (("?" not in p) or path == p):
                r = resps[0] if len(resps) == 1 else resps.pop(0)
                return r
        return FakeResponse(404, {"detail": f"no route {method} {path}"})


@pytest.fixture
def api():
    fake = FakeDarkmoon()
    fake.add("POST", "/api/v1/auth/login", FakeResponse(200, {"token": "tok-123"}))
    conn = SimpleNamespace(
        host="https://dm.example.com/", port=None, login="admin", password="pw",
        extra_dejson={"timeout": 5},
    )
    with mock.patch.object(DarkmoonHook, "get_connection", return_value=conn), \
         mock.patch("requests.Session.request", side_effect=fake):
        yield fake
