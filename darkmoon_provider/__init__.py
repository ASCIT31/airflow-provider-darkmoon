"""Apache Airflow provider for Darkmoon (https://github.com/ASCIT31/Dark-Moon)."""
from __future__ import annotations

__version__ = "0.1.0"


def get_provider_info() -> dict:
    """Metadata Airflow reads through the ``apache_airflow_provider`` entry point."""
    return {
        "package-name": "airflow-provider-darkmoon",
        "name": "Darkmoon",
        "description": (
            "Trigger autonomous AI penetration-test campaigns on a self-hosted "
            "Darkmoon Pro deployment and consume their findings from Airflow DAGs."
        ),
        "connection-types": [
            {
                "hook-class-name": "darkmoon_provider.hooks.darkmoon.DarkmoonHook",
                "connection-type": "darkmoon",
            }
        ],
        "versions": [__version__],
    }
