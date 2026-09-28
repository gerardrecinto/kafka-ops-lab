from pydantic import BaseModel

from kafka_ops_lab.schema.compatibility import check_backward_compatibility
from kafka_ops_lab.schema.deploy_event import DeployEvent, DeployEventV1_0


def test_v1_1_is_backward_compatible_with_v1_0():
    report = check_backward_compatibility(DeployEventV1_0, DeployEvent)
    assert report.compatible
    assert report.removed_required_fields == []
    assert report.newly_required_fields == []


def test_removing_a_required_field_is_incompatible():
    class BrokenSchema(BaseModel):
        schema_version: str = "2.0"
        event_id: str
        environment: str
        status: str
        released_by: str
        occurred_at: str
        # "service" removed — a consumer that relies on it will break

    report = check_backward_compatibility(DeployEventV1_0, BrokenSchema)
    assert not report.compatible
    assert "service" in report.removed_required_fields


def test_adding_a_new_required_field_is_incompatible():
    class BrokenSchema(BaseModel):
        schema_version: str = "2.0"
        event_id: str
        service: str
        environment: str
        status: str
        released_by: str
        occurred_at: str
        region: str  # new AND required — old producers never send this

    report = check_backward_compatibility(DeployEventV1_0, BrokenSchema)
    assert not report.compatible
    assert "region" in report.newly_required_fields
