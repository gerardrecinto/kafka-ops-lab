"""Generic backward-compatibility checker between two pydantic schema
versions, standing in for a real schema registry (Confluent Schema
Registry, AWS Glue Schema Registry) in this portfolio's scope.

"Backward compatible" here means: a consumer running the NEW schema can
still correctly read messages produced under the OLD schema. That's the
direction that matters for a rolling consumer deployment — consumers
upgrade before producers, or vice versa, and the messages already in the
topic don't change.
"""

from dataclasses import dataclass, field
from typing import get_type_hints

from pydantic import BaseModel


@dataclass
class CompatibilityReport:
    compatible: bool
    removed_required_fields: list[str] = field(default_factory=list)
    newly_required_fields: list[str] = field(default_factory=list)
    type_changed_fields: list[str] = field(default_factory=list)


def check_backward_compatibility(old_schema: type[BaseModel], new_schema: type[BaseModel]) -> CompatibilityReport:
    old_fields = old_schema.model_fields
    new_fields = new_schema.model_fields
    old_types = get_type_hints(old_schema)
    new_types = get_type_hints(new_schema)

    removed_required = [
        name for name, info in old_fields.items() if info.is_required() and name not in new_fields
    ]
    newly_required = [
        name
        for name, info in new_fields.items()
        if info.is_required() and name not in old_fields
    ]
    type_changed = [
        name
        for name in old_fields
        if name in new_fields and name in old_types and name in new_types and old_types[name] != new_types[name]
    ]

    compatible = not removed_required and not newly_required and not type_changed
    return CompatibilityReport(
        compatible=compatible,
        removed_required_fields=removed_required,
        newly_required_fields=newly_required,
        type_changed_fields=type_changed,
    )
