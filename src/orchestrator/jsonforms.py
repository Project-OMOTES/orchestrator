"""Validation helpers for JSON Forms schema fragments."""

from typing import Any

from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError

JsonSchemaObject = dict[str, Any]
JsonFormsProperties = dict[str, JsonSchemaObject]


def validate_jsonforms_properties(properties: JsonFormsProperties) -> JsonFormsProperties:
    """Validate that an object is usable as JSON Schema `properties` for JSON Forms."""
    schema: JsonSchemaObject = {"type": "object", "properties": properties}
    try:
        Draft7Validator.check_schema(schema)
    except SchemaError as exc:
        raise ValueError(f"Invalid JSON Forms properties schema: {exc.message}") from exc
    return properties
