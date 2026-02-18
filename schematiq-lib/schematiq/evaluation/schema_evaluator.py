"""Main schema evaluation orchestrator."""

import json
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict

from ..core import utils
from .few_shot_manager import FewShotManager
from .row_evaluator import RowQueryEvaluator, RowEvaluationResult
from .gt_comparator import GTComparator, ComparisonResult


@dataclass
class SchemaEvaluationResult:
    """Complete schema evaluation result."""
    query: str
    total_rows: int
    evaluation_time: float
    overall_metrics: Dict[str, float]
    row_results: List[Dict[str, Any]]
    column_analysis: Dict[str, Any]
    recommendations: Dict[str, List[str]]
    config_used: Dict[str, Any]


class SchemaEvaluator:
    """Main orchestrator for schema evaluation."""

    def __init__(self, config_path: str):
        """
        Initialize SchemaEvaluator.

        Args:
            config_path: Path to evaluation configuration file
        """
        self.config = self._load_config(config_path)
        self.query = self._load_query_from_schematiq_config()
        self.data_path = self._load_data_path_from_schematiq_config()

        # Initialize components
        self.llm = utils.build_llm(self.config["evaluator_config"])
        self.few_shot_manager = FewShotManager(self.config["few_shot_config"])
        self.row_evaluator = RowQueryEvaluator(self.llm)
        self.gt_comparator = GTComparator(self.config.get("comparison_config", {}))

        self.gt_column = self.config["gt_column"]

        print(f"📊 Initialized SchemaEvaluator")
        print(f"   Query: {self.query[:100]}...")
        print(f"   Data: {self.data_path}")
        print(f"   GT Column: {self.gt_column}")

    def evaluate_schema(self) -> SchemaEvaluationResult:
        """
        Perform complete schema evaluation.

        Returns:
            SchemaEvaluationResult with comprehensive evaluation results
        """
        start_time = time.time()

        print(f"🔍 Starting schema evaluation...")

        # Load data
        data = self._load_evaluation_data()
        if not data:
            raise ValueError("No evaluation data found")

        # Extract few-shot examples
        few_shot_examples = self.few_shot_manager.extract_gt_examples(data, self.gt_column)
        formatted_examples = self.few_shot_manager.format_examples_for_prompt(
            few_shot_examples,
            self.query,
            self.gt_column
        )

        print(f"📋 Using {len(few_shot_examples)} few-shot examples")

        # Filter data for evaluation (exclude few-shot examples from test set)
        eval_data = self._filter_evaluation_data(data, few_shot_examples)
        print(f"📊 Evaluating {len(eval_data)} rows")

        # Evaluate each row
        row_results = self.row_evaluator.evaluate_batch(
            query=self.query,
            rows=eval_data,
            few_shot_examples=formatted_examples,
            gt_column=self.gt_column
        )

        # Compare with ground truth
        comparisons = self._compare_with_ground_truth(row_results, eval_data)

        # Calculate metrics
        overall_metrics = self._calculate_overall_metrics(row_results, comparisons)

        # Analyze column usage
        column_analysis = self._analyze_column_usage(row_results, eval_data)

        # Generate recommendations
        recommendations = self._generate_recommendations(overall_metrics, column_analysis, comparisons)

        # Combine row results with GT comparisons
        enhanced_row_results = self._enhance_row_results(row_results, eval_data, comparisons)

        evaluation_time = time.time() - start_time

        result = SchemaEvaluationResult(
            query=self.query,
            total_rows=len(eval_data),
            evaluation_time=evaluation_time,
            overall_metrics=overall_metrics,
            row_results=enhanced_row_results,
            column_analysis=column_analysis,
            recommendations=recommendations,
            config_used=self.config
        )

        print(f"✅ Evaluation completed in {evaluation_time:.2f}s")
        print(f"   Overall accuracy: {overall_metrics.get('accuracy', 0):.3f}")
        print(f"   Answer rate: {overall_metrics.get('answer_rate', 0):.3f}")

        return result

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load evaluation configuration."""
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _load_query_from_schematiq_config(self) -> str:
        """Load query from ScheMatiQ configuration."""
        schematiq_config_path = self.config["query_config_path"]
        with open(schematiq_config_path, 'r', encoding='utf-8') as f:
            schematiq_config = json.load(f)
        return schematiq_config["query"]

    def _load_data_path_from_schematiq_config(self) -> str:
        """Load data path from ScheMatiQ configuration."""
        schematiq_config_path = self.config["query_config_path"]
        with open(schematiq_config_path, 'r', encoding='utf-8') as f:
            schematiq_config = json.load(f)

        # Check if there's an override in eval config
        if "data_path_override" in self.config:
            return self.config["data_path_override"]

        # Get from value extraction config if present
        if "valueExtractionConfig_path" in self.config:
            with open(self.config["valueExtractionConfig_path"], 'r', encoding='utf-8') as f:
                ve_config = json.load(f)
            return ve_config.get("output_path", "output.jsonl")

        # Default fallback
        return "outputs/evaluation_data.jsonl"

    def _load_evaluation_data(self) -> List[Dict[str, Any]]:
        """Load evaluation data from file."""
        return self.few_shot_manager.load_data_from_jsonl(Path(self.data_path))

    def _filter_evaluation_data(self,
                               data: List[Dict[str, Any]],
                               few_shot_examples: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter out few-shot examples from evaluation data."""
        # Extract identifiers from few-shot examples
        few_shot_ids = set()
        for example in few_shot_examples:
            paper_id = (example.get('paper_name') or
                       example.get('title') or
                       str(example.get('id', '')))
            few_shot_ids.add(paper_id)

        # Filter evaluation data
        eval_data = []
        for row in data:
            row_id = (row.get('paper_name') or
                     row.get('title') or
                     str(row.get('id', '')))

            if row_id not in few_shot_ids and row.get(self.gt_column) is not None:
                eval_data.append(row)

        return eval_data

    def _compare_with_ground_truth(self,
                                 row_results: List[RowEvaluationResult],
                                 eval_data: List[Dict[str, Any]]) -> List[ComparisonResult]:
        """Compare evaluation results with ground truth."""
        comparisons = []

        for i, result in enumerate(row_results):
            if i < len(eval_data):
                gt_answer = eval_data[i].get(self.gt_column, "")
                comparison = self.gt_comparator.compare_answers(
                    result.predicted_answer,
                    str(gt_answer)
                )
                comparisons.append(comparison)

        return comparisons

    def _calculate_overall_metrics(self,
                                 row_results: List[RowEvaluationResult],
                                 comparisons: List[ComparisonResult]) -> Dict[str, float]:
        """Calculate overall evaluation metrics."""
        total_rows = len(row_results)

        if total_rows == 0:
            return {}

        # Basic metrics
        answered_rows = sum(1 for r in row_results if r.information_sufficiency != 'Insufficient')
        high_confidence = sum(1 for r in row_results if r.confidence == 'High')
        medium_confidence = sum(1 for r in row_results if r.confidence == 'Medium')
        low_confidence = sum(1 for r in row_results if r.confidence == 'Low')

        metrics = {
            'answer_rate': answered_rows / total_rows,
            'high_confidence_rate': high_confidence / total_rows,
            'medium_confidence_rate': medium_confidence / total_rows,
            'low_confidence_rate': low_confidence / total_rows,
            'avg_columns_used': sum(len(r.columns_used) for r in row_results) / total_rows
        }

        # Add GT comparison metrics
        if comparisons:
            gt_metrics = self.gt_comparator.calculate_aggregate_metrics(comparisons)
            metrics.update(gt_metrics)

        return metrics

    def _analyze_column_usage(self,
                            row_results: List[RowEvaluationResult],
                            eval_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze column usage patterns."""
        if not eval_data:
            return {}

        # Count available columns
        all_columns = set()
        for row in eval_data:
            all_columns.update(row.keys())

        # Exclude metadata columns
        exclude_columns = {self.gt_column, 'paper_name', 'title', 'id', '_metadata'}
        schema_columns = all_columns - exclude_columns

        # Count column usage
        column_usage = {col: 0 for col in schema_columns}
        for result in row_results:
            for col in result.columns_used:
                if col in column_usage:
                    column_usage[col] += 1

        # Calculate usage rates
        total_evaluations = len(row_results)
        usage_rates = {col: count / total_evaluations for col, count in column_usage.items()}

        # Sort by usage
        most_used = sorted(usage_rates.items(), key=lambda x: x[1], reverse=True)
        least_used = sorted(usage_rates.items(), key=lambda x: x[1])

        return {
            'total_columns': len(schema_columns),
            'column_usage_rates': usage_rates,
            'most_used_columns': [col for col, rate in most_used[:5]],
            'least_used_columns': [col for col, rate in least_used[:5] if rate < 0.1],
            'unused_columns': [col for col, rate in usage_rates.items() if rate == 0],
            'highly_used_columns': [col for col, rate in usage_rates.items() if rate > 0.7]
        }

    def _generate_recommendations(self,
                                overall_metrics: Dict[str, float],
                                column_analysis: Dict[str, Any],
                                comparisons: List[ComparisonResult]) -> Dict[str, List[str]]:
        """Generate actionable recommendations."""
        recommendations = {
            'schema_improvements': [],
            'data_quality': [],
            'evaluation_insights': []
        }

        # Schema recommendations
        accuracy = overall_metrics.get('accuracy', 0)
        answer_rate = overall_metrics.get('answer_rate', 0)

        if accuracy < 0.7:
            recommendations['schema_improvements'].append(
                "Low accuracy suggests schema may not capture the right information for the query"
            )

        if answer_rate < 0.8:
            recommendations['schema_improvements'].append(
                "Low answer rate indicates missing information - consider adding more columns"
            )

        # Column recommendations
        unused_cols = column_analysis.get('unused_columns', [])
        if unused_cols:
            recommendations['schema_improvements'].append(
                f"Consider removing unused columns: {', '.join(unused_cols[:3])}"
            )

        least_used = column_analysis.get('least_used_columns', [])
        if least_used:
            recommendations['data_quality'].append(
                f"Low-usage columns may need better extraction: {', '.join(least_used[:3])}"
            )

        # Evaluation insights
        high_conf_rate = overall_metrics.get('high_confidence_rate', 0)
        if high_conf_rate < 0.5:
            recommendations['evaluation_insights'].append(
                "Low confidence rates suggest extracted data may be unclear or incomplete"
            )

        return recommendations

    def _enhance_row_results(self,
                           row_results: List[RowEvaluationResult],
                           eval_data: List[Dict[str, Any]],
                           comparisons: List[ComparisonResult]) -> List[Dict[str, Any]]:
        """Enhance row results with GT comparison data."""
        enhanced = []

        for i, result in enumerate(row_results):
            row_dict = asdict(result)

            # Add GT information
            if i < len(eval_data):
                row_dict['gt_answer'] = eval_data[i].get(self.gt_column, "")

            # Add comparison results
            if i < len(comparisons):
                comparison = comparisons[i]
                row_dict.update({
                    'exact_match': comparison.exact_match,
                    'semantic_similarity': comparison.semantic_similarity,
                    'gt_comparison_confidence': comparison.confidence_score
                })

            enhanced.append(row_dict)

        return enhanced

    def save_results(self,
                    result: SchemaEvaluationResult,
                    output_path: str) -> None:
        """Save evaluation results to file."""
        output_data = asdict(result)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        print(f"💾 Saved evaluation results to {output_path}")

    def print_summary(self, result: SchemaEvaluationResult) -> None:
        """Print evaluation summary."""
        print(f"\n📊 SCHEMA EVALUATION SUMMARY")
        print(f"=" * 50)
        print(f"Query: {result.query}")
        print(f"Total Rows Evaluated: {result.total_rows}")
        print(f"Evaluation Time: {result.evaluation_time:.2f}s")

        print(f"\n📈 OVERALL METRICS:")
        for metric, value in result.overall_metrics.items():
            if isinstance(value, float):
                print(f"  {metric}: {value:.3f}")
            else:
                print(f"  {metric}: {value}")

        print(f"\n🔍 COLUMN ANALYSIS:")
        ca = result.column_analysis
        print(f"  Total Schema Columns: {ca.get('total_columns', 0)}")
        print(f"  Most Used: {', '.join(ca.get('most_used_columns', [])[:3])}")
        print(f"  Unused: {len(ca.get('unused_columns', []))} columns")

        print(f"\n💡 RECOMMENDATIONS:")
        for category, recs in result.recommendations.items():
            if recs:
                print(f"  {category.title()}:")
                for rec in recs[:2]:  # Show top 2 recommendations per category
                    print(f"    - {rec}")
        print()
