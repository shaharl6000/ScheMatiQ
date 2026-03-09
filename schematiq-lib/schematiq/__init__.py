"""
ScheMatiQ

A modern approach to information extraction that takes a user query and a
collection of documents, then iteratively discovers a table schema that best
captures information needed to answer the query.
"""

__version__ = "0.1.0"

from schematiq.core.schema import (
    Column,
    Schema,
    SchemaSnapshot,
    SchemaEvolution,
    ObservationUnit,
)
from schematiq.core.llm_backends import LLMInterface, TogetherLLM, OpenAILLM, GeminiLLM
from schematiq.core.llm_call_tracker import (
    LLMCallTracker,
    GlobalLLMUsageTracker,
    QuotaExceededError,
)
from schematiq.core.retrievers import EmbeddingRetriever
from schematiq.core.prompts import SchemaMode
from schematiq.core.model_specs import (
    ModelSpec,
    get_model_spec,
    MODEL_SPECS,
    GLOBAL_FALLBACK,
)
from schematiq.core.document_preprocessor import (
    DocumentPreprocessor,
    PreprocessorConfig,
)
from schematiq.core.table_detector import TableDetector
from schematiq.core.schematiq import (
    _discover_observation_unit as discover_observation_unit,
    ObservationUnitError,
    ObservationUnitDiscoveryError,
)

__all__ = [
    "Column",
    "Schema",
    "SchemaSnapshot",
    "SchemaEvolution",
    "ObservationUnit",
    "ObservationUnitError",
    "ObservationUnitDiscoveryError",
    "SchemaMode",
    "LLMInterface",
    "TogetherLLM",
    "OpenAILLM",
    "GeminiLLM",
    "LLMCallTracker",
    "GlobalLLMUsageTracker",
    "QuotaExceededError",
    "EmbeddingRetriever",
    "DocumentPreprocessor",
    "PreprocessorConfig",
    "TableDetector",
    "discover_observation_unit",
    "ModelSpec",
    "get_model_spec",
    "MODEL_SPECS",
    "GLOBAL_FALLBACK",
]
