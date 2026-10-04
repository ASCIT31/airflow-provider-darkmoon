"""Operators for Darkmoon (Darkmoon Pro Dashboard API)."""
from __future__ import annotations

from typing import Any, Sequence

try:  # Airflow 3
    from airflow.sdk import BaseOperator
except ImportError:  # pragma: no cover - Airflow 2
    from airflow.models import BaseOperator

from airflow.exceptions import AirflowException

from darkmoon_provider.hooks.darkmoon import SEVERITIES, DarkmoonHook


def _split_csv(value: str | Sequence[str] | None) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        value = value.split(",")
    return [str(p).strip() for p in value if str(p).strip()]


class DarkmoonRunPentestOperator(BaseOperator):
    """Run a Darkmoon pentest campaign and (optionally) wait for its findings.

    Only point this at systems you own or are explicitly authorised to test.
    Findings can include false positives and need human review.

    :param target: host, URL or scope to assess.
    :param program: optional program name / rules-of-engagement note.
    :param focus: optional focus areas (list or comma separated string).
    :param severity: optional minimum severity for the campaign to report.
    :param wait_for_completion: poll until the run finishes and return findings.
        If False, return right after the run starts (pair with
        :class:`~darkmoon_provider.sensors.darkmoon.DarkmoonRunSensor`).
    :param poll_interval: seconds between run-log polls.
    :param timeout: seconds to wait before stopping the run and failing the task.
    :param fail_on_severity: if set (``info|low|medium|high|critical``), fail the
        task when at least one finding is at or above that severity. Useful as a
        pipeline gate.
    :param return_findings: include the full findings list in the XCom result.
    :param darkmoon_conn_id: Airflow connection id (type ``darkmoon``).

    The returned XCom value is a dict: ``run_id``, ``campaign_id``, ``total``,
    ``stats`` and (if ``return_findings``) ``findings``.
    """

    template_fields: Sequence[str] = ("target", "program", "focus", "severity")
    ui_color = "#4f46e5"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        target: str,
        program: str | None = None,
        focus: str | Sequence[str] | None = None,
        severity: str | None = None,
        wait_for_completion: bool = True,
        poll_interval: float = 10.0,
        timeout: float = 3600.0,
        fail_on_severity: str | None = None,
        return_findings: bool = True,
        darkmoon_conn_id: str = DarkmoonHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if fail_on_severity is not None and fail_on_severity.lower() not in SEVERITIES:
            raise ValueError(f"fail_on_severity must be one of {SEVERITIES}")
        self.target = target
        self.program = program
        self.focus = focus
        self.severity = severity
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.fail_on_severity = fail_on_severity
        self.return_findings = return_findings
        self.darkmoon_conn_id = darkmoon_conn_id
        self._run_id: str | None = None

    def _hook(self) -> DarkmoonHook:
        return DarkmoonHook(darkmoon_conn_id=self.darkmoon_conn_id)

    def execute(self, context: Any) -> dict[str, Any]:
        target = str(self.target or "").strip()
        if not target:
            raise AirflowException("A target is required (a host, URL or scope you are authorised to test).")
        params: dict[str, Any] = {"target": target}
        if self.program:
            params["program"] = str(self.program).strip()
        areas = _split_csv(self.focus)
        if areas:
            params["focus"] = areas
        if self.severity:
            params["severity"] = str(self.severity).strip()

        hook = self._hook()
        before = {c.get("id") for c in hook.list_campaigns()}
        handle = hook.run_campaign(params)
        self._run_id = handle["run_id"]
        self.log.info("Started Darkmoon run %s against %s", self._run_id, target)
        if not self.wait_for_completion:
            return {"run_id": self._run_id, "campaign_id": None, "total": 0, "stats": {}, "findings": []}

        state = hook.wait_for_run(self._run_id, poll_interval=self.poll_interval, timeout=self.timeout)
        if state["timed_out"]:
            hook.stop_run(self._run_id)
            raise AirflowException(f"Darkmoon run {self._run_id} did not finish within {self.timeout}s; stop requested.")
        terminal = state["terminal"] or {}
        if terminal.get("type") == "run_error":
            raise AirflowException(f"Darkmoon run {self._run_id} ended with an error: {terminal}")

        campaign = hook.resolve_run_campaign(before, target)
        campaign_id = campaign.get("id") if campaign else None
        result: dict[str, Any] = {
            "run_id": self._run_id, "campaign_id": campaign_id, "total": 0, "stats": {}, "findings": [],
        }
        if campaign_id:
            findings = hook.get_findings(campaign_id)
            result.update(total=findings["total"], stats=findings["stats"])
            if self.return_findings:
                result["findings"] = findings["data"]
            if self.fail_on_severity:
                n = hook.count_at_or_above(findings["data"], self.fail_on_severity)
                if n:
                    raise AirflowException(
                        f"{n} Darkmoon finding(s) at or above {self.fail_on_severity!r} in campaign {campaign_id}."
                    )
        self.log.info("Darkmoon run %s finished: campaign=%s findings=%s", self._run_id, campaign_id, result["total"])
        return result

    def on_kill(self) -> None:
        if self._run_id:
            self.log.info("Stopping Darkmoon run %s", self._run_id)
            self._hook().stop_run(self._run_id)


class DarkmoonGetFindingsOperator(BaseOperator):
    """Fetch findings and severity stats for an existing campaign.

    :param campaign_id: Darkmoon campaign id (templated).
    :param fail_on_severity: fail if any finding is at or above this severity.
    :param darkmoon_conn_id: Airflow connection id.
    """

    template_fields: Sequence[str] = ("campaign_id",)
    ui_color = "#4f46e5"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        campaign_id: str,
        fail_on_severity: str | None = None,
        darkmoon_conn_id: str = DarkmoonHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if fail_on_severity is not None and fail_on_severity.lower() not in SEVERITIES:
            raise ValueError(f"fail_on_severity must be one of {SEVERITIES}")
        self.campaign_id = campaign_id
        self.fail_on_severity = fail_on_severity
        self.darkmoon_conn_id = darkmoon_conn_id

    def execute(self, context: Any) -> dict[str, Any]:
        campaign_id = str(self.campaign_id or "").strip()
        if not campaign_id:
            raise AirflowException("A campaign id is required.")
        hook = DarkmoonHook(darkmoon_conn_id=self.darkmoon_conn_id)
        findings = hook.get_findings(campaign_id)
        if self.fail_on_severity:
            n = hook.count_at_or_above(findings["data"], self.fail_on_severity)
            if n:
                raise AirflowException(
                    f"{n} Darkmoon finding(s) at or above {self.fail_on_severity!r} in campaign {campaign_id}."
                )
        return {
            "campaign_id": campaign_id,
            "total": findings["total"],
            "stats": findings["stats"],
            "findings": findings["data"],
        }
