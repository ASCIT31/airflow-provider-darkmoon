"""Hook for the Darkmoon Dashboard API (Darkmoon Pro, self-hosted).

Endpoints used (same ones as ``langchain-darkmoon`` and ``@darkmoon_ai/client``)::

    POST   /api/v1/auth/login                  -> { token }
    POST   /api/v1/run/campaign                -> { run_id, ... }
    GET    /api/v1/run/logs/{run_id}           -> { data: events[], total }
    DELETE /api/v1/run/{run_id}/stop           -> { message, run_id }
    GET    /api/v1/campaigns                   -> { data: campaigns[], total }
    GET    /api/v1/vulnerabilities?campaign_id -> { data, total, stats }
    GET    /api/v1/campaigns/{id}/report       -> { content, format }

The Dashboard API belongs to Darkmoon Pro. The open-source engine and CLI do
not expose it. There is no public hosted endpoint: the base URL is always your
own deployment.
"""
from __future__ import annotations

import time
from typing import Any, Callable
from urllib.parse import quote

import requests

try:  # Airflow 3
    from airflow.sdk import BaseHook
except ImportError:  # pragma: no cover - Airflow 2
    from airflow.hooks.base import BaseHook

try:
    from airflow.exceptions import AirflowException
except ImportError:  # pragma: no cover
    AirflowException = Exception  # type: ignore[misc,assignment]

TERMINAL_EVENT_TYPES = frozenset({"run_completed", "run_error"})
SEVERITIES = ("info", "low", "medium", "high", "critical")


class DarkmoonError(AirflowException):  # type: ignore[misc,valid-type]
    """Darkmoon API error carrying the API's own ``detail`` and HTTP status."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class DarkmoonHook(BaseHook):
    """Talk to a Darkmoon Pro Dashboard API using an Airflow connection.

    Connection fields:

    * **Host**: base URL of the dashboard API, e.g. ``https://darkmoon.example.com``
      (a scheme is required; ``http://`` is only sensible on a trusted network).
    * **Login / Password**: a Darkmoon dashboard user.
    * **Extra** (JSON, optional): ``{"verify_ssl": true, "timeout": 60}``.

    :param darkmoon_conn_id: Airflow connection id.
    """

    conn_name_attr = "darkmoon_conn_id"
    default_conn_name = "darkmoon_default"
    conn_type = "darkmoon"
    hook_name = "Darkmoon"

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        return {
            "hidden_fields": ["port", "schema"],
            "relabeling": {"host": "Dashboard API URL", "login": "Username"},
            "placeholders": {
                "host": "https://darkmoon.example.com",
                "extra": '{"verify_ssl": true, "timeout": 60}',
            },
        }

    def __init__(self, darkmoon_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.darkmoon_conn_id = darkmoon_conn_id
        self._base_url: str | None = None
        self._token: str | None = None
        self._verify: bool | str = True
        self._timeout: float = 60.0
        self._session: requests.Session | None = None

    # -- plumbing -----------------------------------------------------------
    def _configure(self) -> requests.Session:
        if self._session is not None:
            return self._session
        conn = self.get_connection(self.darkmoon_conn_id)
        host = (conn.host or "").strip()
        if not host:
            raise DarkmoonError(f"Connection {self.darkmoon_conn_id!r} has no Host (dashboard API URL).")
        if "://" not in host:
            host = f"https://{host}"
        if conn.port:
            host = f"{host}:{conn.port}"
        self._base_url = host.rstrip("/")
        extra = conn.extra_dejson or {}
        self._verify = extra.get("verify_ssl", True)
        self._timeout = float(extra.get("timeout", 60))
        self._username = conn.login or ""
        self._password = conn.password or ""
        self._session = requests.Session()
        return self._session

    def _request(self, method: str, path: str, *, auth: bool = True, **kwargs: Any) -> Any:
        session = self._configure()
        if auth and self._token is None:
            self.login()
        headers = {"Content-Type": "application/json"}
        if auth and self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        try:
            resp = session.request(
                method,
                f"{self._base_url}{path}",
                headers=headers,
                timeout=self._timeout,
                verify=self._verify,
                **kwargs,
            )
        except requests.exceptions.RequestException as exc:
            raise DarkmoonError(f"Request to Darkmoon failed: {exc}") from exc
        if auth and resp.status_code == 401:
            # Token expired: re-authenticate once.
            self._token = None
            self.login()
            headers["Authorization"] = f"Bearer {self._token}"
            try:
                resp = session.request(
                    method, f"{self._base_url}{path}", headers=headers,
                    timeout=self._timeout, verify=self._verify, **kwargs,
                )
            except requests.exceptions.RequestException as exc:
                raise DarkmoonError(f"Request to Darkmoon failed: {exc}") from exc
        return resp

    @staticmethod
    def _json(resp: requests.Response) -> Any:
        try:
            return resp.json()
        except ValueError:
            return None

    @classmethod
    def _detail(cls, resp: requests.Response, fallback: str) -> str:
        body = cls._json(resp)
        if isinstance(body, dict):
            d = body.get("detail", body.get("message"))
            if isinstance(d, str) and d:
                return d
        return fallback

    def _expect(self, resp: requests.Response, fallback: str, ok: tuple[int, ...] = (200,)) -> Any:
        if resp.status_code not in ok:
            raise DarkmoonError(self._detail(resp, fallback), resp.status_code)
        return self._json(resp)

    # -- auth ---------------------------------------------------------------
    def login(self) -> str:
        """Authenticate with the connection's credentials and cache the token."""
        self._configure()
        resp = self._request(
            "POST",
            "/api/v1/auth/login",
            auth=False,
            json={"username": self._username, "password": self._password},
        )
        body = self._json(resp)
        token = body.get("token") if isinstance(body, dict) else None
        if resp.status_code != 200 or not token:
            raise DarkmoonError(self._detail(resp, "Darkmoon login failed"), resp.status_code)
        self._token = token
        return token

    def test_connection(self) -> tuple[bool, str]:
        """Used by the Airflow UI "Test" button: log in and list campaigns."""
        try:
            self.list_campaigns()
            return True, "Connected to the Darkmoon Dashboard API."
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    # -- campaigns / runs ---------------------------------------------------
    def list_campaigns(self) -> list[dict[str, Any]]:
        body = self._expect(self._request("GET", "/api/v1/campaigns"), "Failed to list campaigns")
        return body.get("data", []) if isinstance(body, dict) else []

    def run_campaign(self, params: dict[str, Any]) -> dict[str, Any]:
        """Start a campaign in the background; returns the run handle (``run_id``)."""
        resp = self._request("POST", "/api/v1/run/campaign", json=params)
        body = self._json(resp)
        if resp.status_code >= 400 or not (isinstance(body, dict) and body.get("run_id")):
            raise DarkmoonError(self._detail(resp, "Failed to start pentest run"), resp.status_code)
        return body

    def get_run_log(self, run_id: str) -> list[dict[str, Any]]:
        resp = self._request("GET", f"/api/v1/run/logs/{quote(run_id, safe='')}")
        if resp.status_code == 404:
            return []
        body = self._expect(resp, "Failed to read run log")
        return body.get("data", []) if isinstance(body, dict) else []

    def get_run_state(self, run_id: str) -> dict[str, Any]:
        """Return ``{"finished": bool, "terminal": event|None, "events": [...]}``.

        The run log is the API's single source of truth: a ``run_completed`` or
        ``run_error`` event marks the run as finished.
        """
        events = self.get_run_log(run_id)
        terminal = next((e for e in events if e.get("type") in TERMINAL_EVENT_TYPES), None)
        return {"finished": terminal is not None, "terminal": terminal, "events": events}

    def stop_run(self, run_id: str) -> None:
        self._request("DELETE", f"/api/v1/run/{quote(run_id, safe='')}/stop")

    def wait_for_run(
        self,
        run_id: str,
        poll_interval: float = 10.0,
        timeout: float = 3600.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> dict[str, Any]:
        """Poll the run log until a terminal event or ``timeout`` seconds."""
        start = time.monotonic()
        while True:
            state = self.get_run_state(run_id)
            if state["finished"]:
                return {**state, "timed_out": False}
            if time.monotonic() - start >= timeout:
                return {**state, "timed_out": True}
            sleep(poll_interval)

    def resolve_run_campaign(self, before_ids: set[str], target: str) -> dict[str, Any] | None:
        """Find the campaign a run produced (the trigger returns a run id only).

        Diffs the campaign set captured before the run against the set after,
        preferring a new campaign whose id contains the target host, else the
        newest campaign.
        """
        campaigns = self.list_campaigns()
        host = (target or "").strip().lower()

        def by_date(c: dict[str, Any]) -> str:
            return str(c.get("date") or "")

        fresh = [c for c in campaigns if c.get("id") not in before_ids]
        if len(fresh) == 1:
            return fresh[0]
        if fresh:
            fresh.sort(key=by_date, reverse=True)
            if host:
                for c in fresh:
                    if host in str(c.get("id", "")).lower():
                        return c
            return fresh[0]
        if not campaigns:
            return None
        return sorted(campaigns, key=by_date, reverse=True)[0]

    # -- findings / report --------------------------------------------------
    def get_findings(self, campaign_id: str) -> dict[str, Any]:
        resp = self._request("GET", f"/api/v1/vulnerabilities?campaign_id={quote(campaign_id, safe='')}")
        body = self._expect(resp, "Failed to fetch findings")
        body = body if isinstance(body, dict) else {}
        return {"data": body.get("data") or [], "total": body.get("total") or 0, "stats": body.get("stats") or {}}

    def get_report(self, campaign_id: str) -> dict[str, Any]:
        resp = self._request("GET", f"/api/v1/campaigns/{quote(campaign_id, safe='')}/report")
        body = self._expect(resp, "Failed to fetch report")
        body = body if isinstance(body, dict) else {}
        return {"content": body.get("content") or "", "format": body.get("format") or "markdown"}

    @staticmethod
    def count_at_or_above(findings: list[dict[str, Any]], threshold: str) -> int:
        """Count findings whose severity is >= ``threshold``."""
        t = threshold.lower()
        if t not in SEVERITIES:
            raise ValueError(f"Unknown severity {threshold!r}; expected one of {SEVERITIES}")
        floor = SEVERITIES.index(t)
        return sum(
            1 for f in findings
            if str(f.get("severity", "")).lower() in SEVERITIES
            and SEVERITIES.index(str(f["severity"]).lower()) >= floor
        )
