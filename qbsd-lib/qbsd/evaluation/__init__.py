"""
Evaluation metrics and tools for QBSD schema and value extraction.

This module contains:
- ArxivDIGESTables metrics (SchemaRecallMetric, BaseMetric)
- QBSD evaluation framework (SchemaEvaluator, DataQualityEvaluator, etc.)
"""

# ArxivDIGESTables metrics
from .metrics import SchemaRecallMetric, BaseMetric
from .table import Table
from .run_eval import main as run_evaluation

# QBSD evaluation framework
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
    # QBSD evaluation
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