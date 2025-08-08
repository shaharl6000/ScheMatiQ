"""
ArxivDIGESTables - Table Synthesis Evaluation Framework

This module provides evaluation tools for comparing QBSD-generated table schemas 
against gold standard tables from the ArxivDIGESTables dataset.

Key components:
- metric/ - Evaluation metrics and scoring functions
- predictions/ - Evaluation results and outputs
"""

from .metric.metrics import SchemaRecallMetric, BaseMetric
from .metric.table import Table

__all__ = ['SchemaRecallMetric', 'BaseMetric', 'Table']