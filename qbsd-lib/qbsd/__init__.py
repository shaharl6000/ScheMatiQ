"""
QBSD - Query-Based Schema Discovery

A modern approach to information extraction that takes a user query and a
collection of documents, then iteratively discovers a table schema that best
captures information needed to answer the query.
"""

__version__ = "0.1.0"

from qbsd.core.schema import Column, Schema, SchemaSnapshot, SchemaEvolution
from qbsd.core.llm_backends import LLMInterface, TogetherLLM, OpenAILLM, GeminiLLM
from qbsd.core.retrievers import EmbeddingRetriever

__all__ = [
    "Column",
    "Schema",
    "SchemaSnapshot",
    "SchemaEvolution",
    "LLMInterface",
    "TogetherLLM",
    "OpenAILLM",
    "GeminiLLM",
    "EmbeddingRetriever",
]
