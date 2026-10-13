"""ODCS validation, compilation, and compatibility helpers."""

from __future__ import annotations

import json
import re
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


@dataclass(frozen=True)
class CompiledSourceObject:
    object_name: str
    source_system: str
    columns: tuple[str, ...]
    spark_schema: str


def compile_source_objects(contract: dict[str, Any]) -> tuple[CompiledSourceObject, ...]:
    """Compile flat CSV ingestion interfaces, preserving the contract's column order."""
    systems = [
        item.get("value") for item in contract.get("customProperties", []) if item.get("property") == "source_system"
    ]
    if len(systems) != 1 or not isinstance(systems[0], str) or not _identifier(systems[0]):
        raise ValueError(f"Source contract {contract.get('id')} needs one valid source_system custom property")
    compiled = []
    objects = set()
    for element in contract.get("schema", []):
        name = element["name"]
        if not _identifier(name) or name.casefold() in objects:
            raise ValueError(f"Invalid or duplicate source object: {name!r}")
        objects.add(name.casefold())
        properties = element.get("properties", [])
        columns = tuple(prop["name"] for prop in properties)
        if not columns or any(not _identifier(column) for column in columns):
            raise ValueError(f"Source object {name} needs flat columns with valid identifiers")
        if len({column.casefold() for column in columns}) != len(columns):
            raise ValueError(f"Duplicate source columns in {name}")
        reserved = {
            "_metadata",
            "_rescued_data",
            "_source_file",
            "_ingested_at",
            "_source_system",
            "_batch_id",
            "_raw_payload",
            "_contract_violations",
        }
        if any(column.casefold() in reserved for column in columns):
            raise ValueError(f"Source object {name} uses columns reserved for ingestion metadata")
        if any(prop.get("logicalType") != "string" for prop in properties):
            raise ValueError(f"Source object {name} must declare every Bronze payload field as logicalType: string")
        compiled.append(
            CompiledSourceObject(
                object_name=name,
                source_system=systems[0],
                columns=columns,
                spark_schema=", ".join(f"{column} STRING" for column in columns),
            )
        )
    return tuple(compiled)


def _identifier(value: str) -> bool:
    return re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is not None


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
        formatted = "\n".join(f"{'.'.join(map(str, error.path)) or '<root>'}: {error.message}" for error in errors)
        raise ValueError(f"{path} does not conform to ODCS:\n{formatted}")
    return contract


def compile_object(contract: dict[str, Any], object_name: str) -> CompiledObject:
    try:
        element = next(item for item in contract.get("schema", []) if item["name"] == object_name)
    except StopIteration as exc:
        raise KeyError(f"Object {object_name!r} not found in contract {contract.get('id')}") from exc

    properties = element.get("properties", [])
    required = {
        f"{prop['name']}_not_null": f"{prop['name']} IS NOT NULL" for prop in properties if prop.get("required")
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
        new_properties = {prop["name"]: prop for prop in new_objects[object_name].get("properties", [])}
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
