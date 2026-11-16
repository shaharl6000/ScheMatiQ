"""Public schema discovery orchestration package for ScheMatiQ."""

from schematiq.discovery.batch_execution import (
    generate_schema_with_split,
    is_token_limit_error,
)
from schematiq.discovery.batch_planner import PlannedBatch, plan_batches
from schematiq.discovery.main import (
    DiscoveryConfig,
    OnProgressCallback,
    OnSnapshotCallback,
    OnWarningCallback,
    ShouldStopCallback,
    discover_schema,
    load_initial_schema_from_config,
)

__all__ = [
    "DiscoveryConfig",
    "OnProgressCallback",
    "OnSnapshotCallback",
    "OnWarningCallback",
    "PlannedBatch",
    "ShouldStopCallback",
    "discover_schema",
    "generate_schema_with_split",
    "is_token_limit_error",
    "load_initial_schema_from_config",
    "plan_batches",
]
