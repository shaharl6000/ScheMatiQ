"""
QBSD - Query-Based Schema Discovery

A modern approach to information extraction that takes a user query and a
collection of documents, then iteratively discovers a table schema that best
captures information needed to answer the query.
"""

__version__ = "0.1.0"

from qbsd.core.schema import Column, Schema, SchemaSnapshot, SchemaEvolution, ObservationUnit
from qbsd.core.llm_backends import LLMInterface, TogetherLLM, OpenAILLM, GeminiLLM
from qbsd.core.retrievers import EmbeddingRetriever
from qbsd.core.prompts import SchemaMode
from qbsd.core.qbsd import (
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
    "EmbeddingRetriever",
    "discover_observation_unit",
]
