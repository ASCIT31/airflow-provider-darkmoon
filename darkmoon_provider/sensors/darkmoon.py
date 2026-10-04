"""Sensor for Darkmoon runs."""
from __future__ import annotations

from typing import Any, Sequence

try:  # Airflow 3.1+
    from airflow.sdk import BaseSensorOperator
except ImportError:  # pragma: no cover
    from airflow.sensors.base import BaseSensorOperator

from airflow.exceptions import AirflowException

from darkmoon_provider.hooks.darkmoon import DarkmoonHook


class DarkmoonRunSensor(BaseSensorOperator):
    """Wait for a Darkmoon run to reach a terminal state.

    Succeeds on a ``run_completed`` event, fails on ``run_error``. Use with
    ``DarkmoonRunPentestOperator(wait_for_completion=False)`` and pass its
    XCom ``run_id``.

    :param run_id: the run id returned when the campaign was started (templated).
    :param darkmoon_conn_id: Airflow connection id.
    """

    template_fields: Sequence[str] = ("run_id",)

    def __init__(self, *, run_id: str, darkmoon_conn_id: str = DarkmoonHook.default_conn_name, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.run_id = run_id
        self.darkmoon_conn_id = darkmoon_conn_id

    def poke(self, context: Any) -> bool:
        state = DarkmoonHook(darkmoon_conn_id=self.darkmoon_conn_id).get_run_state(self.run_id)
        if not state["finished"]:
            self.log.info("Darkmoon run %s still running (%d events)", self.run_id, len(state["events"]))
            return False
        if state["terminal"].get("type") == "run_error":
            raise AirflowException(f"Darkmoon run {self.run_id} ended with an error: {state['terminal']}")
        return True
