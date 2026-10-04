# airflow-provider-darkmoon

Apache Airflow provider for [Darkmoon](https://github.com/ASCIT31/Dark-Moon), the autonomous AI penetration testing platform. Start a pentest campaign from a DAG, wait for it, and use the findings (or a severity gate) in the rest of your pipeline.

> **Open source vs Pro.** The Darkmoon engine and CLI are open source (GPL-3.0). The **Dashboard API** this provider talks to is part of **Darkmoon Pro** and is self-hosted by you: there is no public hosted endpoint. This provider needs a reachable Darkmoon Pro dashboard, a dashboard user, and an Airflow worker that can reach it. If you only run the open-source CLI, use the CLI JSON/SARIF output or the `ASCIT31/darkmoon-action` GitHub Action instead.

> Only run assessments against systems you own or are explicitly authorised to test. Findings can contain false positives and need review by a qualified human. This provider never triggers remediation or merges anything.

Not an Apache Software Foundation provider: it is a community package (`airflow-provider-*`, see the [Airflow ecosystem page](https://airflow.apache.org/ecosystem/)).

## Install

```bash
pip install airflow-provider-darkmoon
```

Requires Python 3.9+ and Apache Airflow 2.7+ (tested on Airflow 3). Airflow discovers the provider through the `apache_airflow_provider` entry point.

## Connection

Create a connection of type **Darkmoon** (id `darkmoon_default` by default):

| Field | Value |
| --- | --- |
| Dashboard API URL (Host) | `https://darkmoon.example.com` |
| Username / Password | a Darkmoon dashboard user |
| Extra (JSON, optional) | `{"verify_ssl": true, "timeout": 60}` |

```bash
airflow connections add darkmoon_default --conn-type darkmoon \
  --conn-host https://darkmoon.example.com --conn-login admin --conn-password '***'
```

## What is included

| Class | Purpose |
| --- | --- |
| `darkmoon_provider.hooks.darkmoon.DarkmoonHook` | Login (JWT, auto re-login on 401), run campaign, poll run log, list campaigns, findings, report |
| `darkmoon_provider.operators.darkmoon.DarkmoonRunPentestOperator` | Start a campaign, optionally wait, return findings via XCom, optional `fail_on_severity` gate, stops the run on timeout/kill |
| `darkmoon_provider.operators.darkmoon.DarkmoonGetFindingsOperator` | Fetch findings + severity stats of an existing campaign |
| `darkmoon_provider.sensors.darkmoon.DarkmoonRunSensor` | Wait for a run to finish (`run_completed` succeeds, `run_error` fails); supports `mode="reschedule"` |

## Example

```python
import pendulum
from airflow import DAG
from darkmoon_provider.operators.darkmoon import DarkmoonRunPentestOperator

with DAG("darkmoon_staging_pentest", start_date=pendulum.datetime(2026, 1, 1), schedule=None, catchup=False):
    DarkmoonRunPentestOperator(
        task_id="pentest_and_gate",
        target="https://staging.example.com",
        focus="auth, injection",
        fail_on_severity="high",   # fail the task if any finding is high or critical
        timeout=2 * 3600,
    )
```

The task returns a dict (`run_id`, `campaign_id`, `total`, `stats`, `findings`). A complete example, including the start / sensor split, is in `darkmoon_provider/example_dags/`.

## API endpoints used

Same Dashboard API surface as the `langchain-darkmoon` package: `POST /api/v1/auth/login`, `POST /api/v1/run/campaign`, `GET /api/v1/run/logs/{run_id}`, `DELETE /api/v1/run/{run_id}/stop`, `GET /api/v1/campaigns`, `GET /api/v1/vulnerabilities?campaign_id=`, `GET /api/v1/campaigns/{id}/report`.

## Development

```bash
pip install -e '.[test]'
pytest
```

Tests mock the HTTP layer; no Darkmoon instance is needed.

## License

Apache-2.0. Darkmoon itself is GPL-3.0 and is not bundled here.
