import json
from importlib.metadata import entry_points

import darkmoon_provider


def test_get_provider_info_required_fields():
    info = darkmoon_provider.get_provider_info()
    assert info["package-name"] == "airflow-provider-darkmoon"
    assert info["name"] == "Darkmoon"
    assert info["description"]
    assert info["versions"] == [darkmoon_provider.__version__]
    assert info["connection-types"] == [
        {"hook-class-name": "darkmoon_provider.hooks.darkmoon.DarkmoonHook", "connection-type": "darkmoon"}
    ]


def test_provider_info_matches_airflow_schema():
    jsonschema = __import__("pytest").importorskip("jsonschema")
    import airflow, pathlib
    root = pathlib.Path(airflow.__file__).parent
    schema_path = next(iter(root.glob("provider_info.schema.json")), None)
    if schema_path is None:
        __import__("pytest").skip("airflow provider_info.schema.json not shipped")
    jsonschema.validate(darkmoon_provider.get_provider_info(), json.loads(schema_path.read_text()))


def test_hook_class_importable_from_provider_info():
    import importlib

    def import_string(path):
        mod, _, name = path.rpartition(".")
        return getattr(importlib.import_module(mod), name)

    for ct in darkmoon_provider.get_provider_info()["connection-types"]:
        cls = import_string(ct["hook-class-name"])
        assert cls.conn_type == ct["connection-type"]
