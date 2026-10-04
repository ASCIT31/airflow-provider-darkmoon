import pytest

from darkmoon_provider.hooks.darkmoon import DarkmoonError, DarkmoonHook
from tests.conftest import FakeResponse


def test_login_and_bearer_header(api):
    api.add("GET", "/api/v1/campaigns", FakeResponse(200, {"data": [{"id": "c1"}], "total": 1}))
    hook = DarkmoonHook()
    assert hook.list_campaigns() == [{"id": "c1"}]
    assert api.calls[0]["path"] == "/api/v1/auth/login"
    assert api.calls[0]["json"] == {"username": "admin", "password": "pw"}
    assert api.calls[1]["headers"]["Authorization"] == "Bearer tok-123"


def test_login_failure_surfaces_api_detail(api):
    api.add("POST", "/api/v1/auth/login", FakeResponse(401, {"detail": "bad credentials"}))
    with pytest.raises(DarkmoonError, match="bad credentials") as e:
        DarkmoonHook().login()
    assert e.value.status_code == 401


def test_token_refresh_on_401(api):
    api.add("POST", "/api/v1/auth/login", FakeResponse(200, {"token": "t1"}), FakeResponse(200, {"token": "t2"}))
    api.add("GET", "/api/v1/campaigns", FakeResponse(401, {"detail": "expired"}), FakeResponse(200, {"data": []}))
    assert DarkmoonHook().list_campaigns() == []
    assert api.calls[-1]["headers"]["Authorization"] == "Bearer t2"


def test_run_campaign_requires_run_id(api):
    api.add("POST", "/api/v1/run/campaign", FakeResponse(200, {"run_id": "r1", "pid": 4}))
    assert DarkmoonHook().run_campaign({"target": "x"})["run_id"] == "r1"
    api.add("POST", "/api/v1/run/campaign", FakeResponse(500, {"detail": "boom"}))
    with pytest.raises(DarkmoonError, match="boom"):
        DarkmoonHook().run_campaign({"target": "x"})


def test_run_state_terminal_detection(api):
    api.add("GET", "/api/v1/run/logs/r1", FakeResponse(200, {"data": [{"type": "log"}, {"type": "run_completed"}]}))
    st = DarkmoonHook().get_run_state("r1")
    assert st["finished"] and st["terminal"]["type"] == "run_completed"


def test_run_log_404_is_empty(api):
    api.add("GET", "/api/v1/run/logs/r1", FakeResponse(404, {"detail": "nope"}))
    assert DarkmoonHook().get_run_log("r1") == []


def test_wait_for_run_polls_then_finishes_and_times_out(api):
    api.add("GET", "/api/v1/run/logs/r1",
            FakeResponse(200, {"data": []}), FakeResponse(200, {"data": [{"type": "run_error"}]}))
    sleeps = []
    out = DarkmoonHook().wait_for_run("r1", poll_interval=1, timeout=100, sleep=sleeps.append)
    assert out["finished"] and not out["timed_out"] and sleeps == [1]
    api.add("GET", "/api/v1/run/logs/r2", FakeResponse(200, {"data": []}))
    out = DarkmoonHook().wait_for_run("r2", poll_interval=0, timeout=0, sleep=lambda s: None)
    assert out["timed_out"]


def test_resolve_run_campaign(api):
    api.add("GET", "/api/v1/campaigns", FakeResponse(200, {"data": [
        {"id": "old", "date": "2026-01-01"},
        {"id": "camp_a_other", "date": "2026-02-01"},
        {"id": "camp_b_staging.example.com", "date": "2026-02-02"},
    ]}))
    c = DarkmoonHook().resolve_run_campaign({"old"}, "staging.example.com")
    assert c["id"] == "camp_b_staging.example.com"


def test_get_findings_and_report(api):
    api.add("GET", "/api/v1/vulnerabilities?campaign_id=c1",
            FakeResponse(200, {"data": [{"severity": "high"}], "total": 1, "stats": {"high": 1}}))
    f = DarkmoonHook().get_findings("c1")
    assert f == {"data": [{"severity": "high"}], "total": 1, "stats": {"high": 1}}
    api.add("GET", "/api/v1/campaigns/c1/report", FakeResponse(200, {"content": "# R", "format": "markdown"}))
    assert DarkmoonHook().get_report("c1") == {"content": "# R", "format": "markdown"}


def test_count_at_or_above():
    fs = [{"severity": "low"}, {"severity": "High"}, {"severity": "critical"}, {"severity": "bogus"}]
    assert DarkmoonHook.count_at_or_above(fs, "high") == 2
    with pytest.raises(ValueError):
        DarkmoonHook.count_at_or_above(fs, "nope")


def test_connection_test(api):
    api.add("GET", "/api/v1/campaigns", FakeResponse(200, {"data": []}))
    ok, msg = DarkmoonHook().test_connection()
    assert ok
    api.add("POST", "/api/v1/auth/login", FakeResponse(401, {"detail": "bad"}))
    ok, msg = DarkmoonHook().test_connection()
    assert not ok and "bad" in msg
