"""
Evaluation metrics and tools for ScheMatiQ schema and value extraction.

This module contains:
- ArxivDIGESTables metrics (SchemaRecallMetric, BaseMetric)
- ScheMatiQ evaluation framework (SchemaEvaluator, DataQualityEvaluator, etc.)
"""

# ArxivDIGESTables metrics
from .metrics import SchemaRecallMetric, BaseMetric
from .table import Table
# run_eval is not imported here: it needs the optional 'eval' extra (nltk)
# and downloads NLTK data on import. Use schematiq.evaluation.run_eval.main.

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
