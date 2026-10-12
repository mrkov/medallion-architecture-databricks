"""ODCS validation, compilation, and compatibility helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jsonschema
import yaml


@dataclass(frozen=True)
class CompiledObject:
    contract_id: str
    object_name: str
    columns: tuple[str, ...]
    required_expectations: dict[str, str]
    enum_expectations: dict[str, str]
    primary_key: tuple[str, ...]

    @property
    def expectations(self) -> dict[str, str]:
        return {**self.required_expectations, **self.enum_expectations}


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)
    if not isinstance(loaded, dict):
        raise ValueError(f"Contract must be a mapping: {path}")
    return loaded


def validate_contract(path: Path, schema_path: Path) -> dict[str, Any]:
    contract = load_yaml(path)
    with schema_path.open(encoding="utf-8") as handle:
        schema = json.load(handle)
    validator = jsonschema.Draft201909Validator(schema)
    errors = sorted(validator.iter_errors(contract), key=lambda error: list(error.path))
    if errors:
        formatted = "\n".join(
            f"{'.'.join(map(str, error.path)) or '<root>'}: {error.message}" for error in errors
        )
        raise ValueError(f"{path} does not conform to ODCS:\n{formatted}")
    return contract


def compile_object(contract: dict[str, Any], object_name: str) -> CompiledObject:
    try:
        element = next(item for item in contract.get("schema", []) if item["name"] == object_name)
    except StopIteration as exc:
        raise KeyError(f"Object {object_name!r} not found in contract {contract.get('id')}") from exc

    properties = element.get("properties", [])
    required = {
        f"{prop['name']}_not_null": f"{prop['name']} IS NOT NULL"
        for prop in properties
        if prop.get("required")
    }
    enums: dict[str, str] = {}
    for prop in properties:
        values = [entry["value"] for entry in prop.get("enum", [])]
        if values:
            encoded = ", ".join(_sql_literal(value) for value in values)
            enums[f"{prop['name']}_accepted_values"] = f"{prop['name']} IN ({encoded})"

    primary_key = tuple(
        prop["name"]
        for prop in sorted(properties, key=lambda item: item.get("primaryKeyPosition", 999))
        if prop.get("primaryKey")
    )
    return CompiledObject(
        contract_id=contract["id"],
        object_name=object_name,
        columns=tuple(prop["name"] for prop in properties),
        required_expectations=required,
        enum_expectations=enums,
        primary_key=primary_key,
    )


def breaking_changes(previous: dict[str, Any], current: dict[str, Any]) -> list[str]:
    """Return consumer-facing breaking schema changes between contract versions."""

    old_objects = {obj["name"]: obj for obj in previous.get("schema", [])}
    new_objects = {obj["name"]: obj for obj in current.get("schema", [])}
    changes: list[str] = []
    for object_name, old_object in old_objects.items():
        if object_name not in new_objects:
            changes.append(f"removed object {object_name}")
            continue
        old_properties = {prop["name"]: prop for prop in old_object.get("properties", [])}
        new_properties = {
            prop["name"]: prop for prop in new_objects[object_name].get("properties", [])
        }
        for property_name, old_property in old_properties.items():
            path = f"{object_name}.{property_name}"
            if property_name not in new_properties:
                changes.append(f"removed property {path}")
                continue
            new_property = new_properties[property_name]
            if old_property.get("logicalType") != new_property.get("logicalType"):
                changes.append(f"changed type of {path}")
            if not old_property.get("required", False) and new_property.get("required", False):
                changes.append(f"made optional property required: {path}")
        for property_name, new_property in new_properties.items():
            if property_name not in old_properties and new_property.get("required", False):
                changes.append(f"added required property {object_name}.{property_name}")
    return changes


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"

