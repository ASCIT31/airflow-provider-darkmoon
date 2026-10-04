import pytest
from airflow.exceptions import AirflowException

from darkmoon_provider.operators.darkmoon import DarkmoonGetFindingsOperator, DarkmoonRunPentestOperator
from darkmoon_provider.sensors.darkmoon import DarkmoonRunSensor
from tests.conftest import FakeResponse


def _wire(api, severities=("high",)):
    api.add("GET", "/api/v1/campaigns",
            FakeResponse(200, {"data": [{"id": "old", "date": "1"}]}),
            FakeResponse(200, {"data": [{"id": "old", "date": "1"}, {"id": "new_camp", "date": "2"}]}))
    api.add("POST", "/api/v1/run/campaign", FakeResponse(200, {"run_id": "r1"}))
    api.add("GET", "/api/v1/run/logs/r1", FakeResponse(200, {"data": [{"type": "run_completed"}]}))
    api.add("GET", "/api/v1/vulnerabilities?campaign_id=new_camp", FakeResponse(200, {
        "data": [{"severity": s} for s in severities], "total": len(severities),
        "stats": {s: 1 for s in severities}}))


def test_run_operator_happy_path(api):
    _wire(api, ("low",))
    op = DarkmoonRunPentestOperator(task_id="t", target="https://x.test", focus="auth, injection",
                                    severity="medium", poll_interval=0, fail_on_severity="high")
    out = op.execute({})
    assert out["run_id"] == "r1" and out["campaign_id"] == "new_camp" and out["total"] == 1
    body = next(c["json"] for c in api.calls if c["path"] == "/api/v1/run/campaign")
    assert body == {"target": "https://x.test", "focus": ["auth", "injection"], "severity": "medium"}


def test_run_operator_gate_fails(api):
    _wire(api, ("low", "critical"))
    op = DarkmoonRunPentestOperator(task_id="t", target="x", poll_interval=0, fail_on_severity="high")
    with pytest.raises(AirflowException, match="1 Darkmoon finding"):
        op.execute({})


def test_run_operator_no_wait(api):
    _wire(api)
    out = DarkmoonRunPentestOperator(task_id="t", target="x", wait_for_completion=False).execute({})
    assert out["run_id"] == "r1" and out["campaign_id"] is None


def test_run_operator_run_error(api):
    _wire(api)
    api.add("GET", "/api/v1/run/logs/r1", FakeResponse(200, {"data": [{"type": "run_error", "message": "m"}]}))
    with pytest.raises(AirflowException, match="ended with an error"):
        DarkmoonRunPentestOperator(task_id="t", target="x", poll_interval=0).execute({})


def test_run_operator_timeout_stops_run(api):
    _wire(api)
    api.add("GET", "/api/v1/run/logs/r1", FakeResponse(200, {"data": []}))
    api.add("DELETE", "/api/v1/run/r1/stop", FakeResponse(200, {"message": "stopped"}))
    op = DarkmoonRunPentestOperator(task_id="t", target="x", poll_interval=0, timeout=0)
    with pytest.raises(AirflowException, match="did not finish"):
        op.execute({})
    assert any(c["method"] == "DELETE" for c in api.calls)


def test_run_operator_validation():
    with pytest.raises(ValueError):
        DarkmoonRunPentestOperator(task_id="t", target="x", fail_on_severity="nope")


def test_run_operator_requires_target(api):
    with pytest.raises(AirflowException, match="target is required"):
        DarkmoonRunPentestOperator(task_id="t", target="  ").execute({})


def test_findings_operator(api):
    api.add("GET", "/api/v1/vulnerabilities?campaign_id=c9",
            FakeResponse(200, {"data": [{"severity": "medium"}], "total": 1, "stats": {"medium": 1}}))
    out = DarkmoonGetFindingsOperator(task_id="t", campaign_id="c9", fail_on_severity="high").execute({})
    assert out["total"] == 1
    with pytest.raises(AirflowException):
        DarkmoonGetFindingsOperator(task_id="t", campaign_id="c9", fail_on_severity="medium").execute({})


def test_sensor(api):
    s = DarkmoonRunSensor(task_id="s", run_id="r1")
    api.add("GET", "/api/v1/run/logs/r1", FakeResponse(200, {"data": []}))
    assert s.poke({}) is False
    api.add("GET", "/api/v1/run/logs/r1", FakeResponse(200, {"data": [{"type": "run_completed"}]}))
    assert s.poke({}) is True
    api.add("GET", "/api/v1/run/logs/r1", FakeResponse(200, {"data": [{"type": "run_error"}]}))
    with pytest.raises(AirflowException):
        s.poke({})
