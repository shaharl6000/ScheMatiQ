"""
Evaluation metrics and tools for ScheMatiQ schema and value extraction.

This module contains:
- ArxivDIGESTables metrics (SchemaRecallMetric, BaseMetric)
- ScheMatiQ evaluation framework (SchemaEvaluator, DataQualityEvaluator, etc.)
"""

# ArxivDIGESTables metrics
from .metrics import SchemaRecallMetric, BaseMetric
from .table import Table

# ScheMatiQ evaluation framework
from .schema_evaluator import SchemaEvaluator, SchemaEvaluationResult
from .few_shot_manager import FewShotManager
from .row_evaluator import RowQueryEvaluator, RowEvaluationResult
from .gt_comparator import GTComparator, ComparisonResult
from .data_quality_evaluation import DataQualityEvaluator, EvaluationResult

__all__ = [
    # ArxivDIGESTables
    'SchemaRecallMetric',
    'BaseMetric',
    'Table',
    'run_evaluation',
    # ScheMatiQ evaluation
    'SchemaEvaluator',
    'SchemaEvaluationResult',
    'FewShotManager',
    'RowQueryEvaluator',
    'RowEvaluationResult',
    'GTComparator',
    'ComparisonResult',
    'DataQualityEvaluator',
    'EvaluationResult',
]


def __getattr__(name):
    # run_eval pulls in nltk (optional extra: pip install schematiq[eval]) and
    # downloads NLTK data at import time, so load it only when asked for.
    if name == "run_evaluation":
        from .run_eval import main as run_evaluation
        return run_evaluation
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
