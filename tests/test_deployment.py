"""Consistency of the deployment files with the code they run next to.

These checks need neither Docker nor Grafana: they catch the mistakes that
otherwise only show up as an empty panel (a renamed column, a state added to
the enum but not to the dashboard, a port that drifted from the documentation).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

from usine40.model import State
from usine40.store import _SCHEMA

ROOT = Path(__file__).resolve().parents[1]
GRAFANA = ROOT / "deploy" / "grafana"
PUBLISHED_PORTS = {"grafana": 3300, "mosquitto": 18830, "db": 15432, "plc": 14840}
NO_DATA = -1


def _compose() -> dict:
    return yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))


def _dashboard() -> dict:
    path = GRAFANA / "dashboards" / "usine40-cell.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _alert_rules() -> list[dict]:
    path = GRAFANA / "provisioning" / "alerting" / "usine40.yaml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    return [rule for group in document["groups"] for rule in group["rules"]]


def _datasource_uid() -> str:
    path = GRAFANA / "provisioning" / "datasources" / "usine40.yaml"
    (datasource,) = yaml.safe_load(path.read_text(encoding="utf-8"))["datasources"]
    return datasource["uid"]


def _queries() -> list[str]:
    panels = [panel for panel in _dashboard()["panels"] if "targets" in panel]
    dashboard_sql = [target["rawSql"] for panel in panels for target in panel["targets"]]
    alert_sql = [
        step["model"]["rawSql"]
        for rule in _alert_rules()
        for step in rule["data"]
        if "rawSql" in step["model"]
    ]
    return dashboard_sql + alert_sql


def _schema_columns() -> set[str]:
    columns: set[str] = set()
    for statement in _SCHEMA:
        columns.update(re.findall(r"^\s+([a-z_]+)\s+[A-Z]", statement, flags=re.MULTILINE))
    return columns


def test_compose_project_and_ports_match_the_documentation():
    compose = _compose()
    assert compose["name"] == "usine40cell"
    published = {}
    for name, service in compose["services"].items():
        for mapping in service.get("ports", []):
            host, port, _container = mapping.split(":")
            assert host == "127.0.0.1", f"{name} must only be published on the loopback"
            published[name] = int(port)
    assert published == PUBLISHED_PORTS


def test_every_service_has_a_healthcheck():
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "HEALTHCHECK" in dockerfile
    for name, service in _compose()["services"].items():
        built_from_dockerfile = "build" in service
        assert built_from_dockerfile or "healthcheck" in service, f"{name} has no healthcheck"


def test_dependencies_wait_for_health_and_collector_subscribes_before_the_gateway():
    services = _compose()["services"]
    for name, service in services.items():
        for dependency, rule in service.get("depends_on", {}).items():
            assert rule == {"condition": "service_healthy"}, f"{name} -> {dependency}"
    assert "collector" in services["gateway"]["depends_on"]


def test_environment_example_documents_every_compose_variable():
    compose_text = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    used = set(re.findall(r"\$\{(USINE40_[A-Z0-9_]+)", compose_text))
    documented = set(re.findall(r"^(USINE40_[A-Z0-9_]+)=", example, flags=re.MULTILINE))
    assert used == documented


def test_dashboard_and_alert_rules_use_the_provisioned_datasource():
    uid = _datasource_uid()
    for panel in _dashboard()["panels"]:
        for target in panel.get("targets", []):
            assert target["datasource"]["uid"] == uid, panel["title"]
    for rule in _alert_rules():
        sources = {step["datasourceUid"] for step in rule["data"]}
        assert sources == {uid, "__expr__"}, rule["title"]


def test_state_timeline_mapping_matches_the_state_enum():
    (timeline,) = [p for p in _dashboard()["panels"] if p["type"] == "state-timeline"]
    (mapping,) = timeline["fieldConfig"]["defaults"]["mappings"]
    labels = {int(value): entry["text"] for value, entry in mapping["options"].items()}
    expected = {int(state): state.name for state in State} | {NO_DATA: "NO DATA"}
    assert labels == expected
    colors = [entry["color"] for entry in mapping["options"].values()]
    assert len(set(colors)) == len(colors)


def test_fault_alert_tests_the_fault_state_value():
    (rule,) = [rule for rule in _alert_rules() if rule["uid"] == "usine40-station-fault"]
    sql = rule["data"][0]["model"]["rawSql"]
    assert f"value = {int(State.FAULT)}" in sql
    assert all(rule["for"] == "0s" and rule["noDataState"] == "OK" for rule in _alert_rules())


def test_queries_only_use_columns_that_exist_in_the_store_schema():
    columns = _schema_columns()
    assert {"source_us", "db_us", "running_us", "ideal_cycle_s", "complete"} <= columns
    timestamp_aliases = {"from_us", "to_us", "until_us", "last_us"}
    for sql in _queries():
        referenced = set(re.findall(r"\b[a-z]+_us\b", sql)) - timestamp_aliases
        assert referenced <= columns, referenced - columns
        for table in re.findall(r"\b(?:FROM|JOIN)\s+([a-z_]+)\b(?!\s*\()", sql):
            defined_in_query = re.search(rf"\b{table}\s+AS\s*\(", sql) is not None
            assert table in {"sample", "oee_window"} or defined_in_query, table


def test_broker_keeps_sessions_across_restarts():
    conf = (ROOT / "deploy" / "mosquitto" / "mosquitto.conf").read_text(encoding="utf-8")
    assert re.search(r"^persistence true$", conf, flags=re.MULTILINE)
    assert re.search(r"^autosave_interval \d+$", conf, flags=re.MULTILINE)
