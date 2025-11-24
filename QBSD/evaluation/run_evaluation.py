#!/usr/bin/env python3
"""
Schema Evaluation Runner

This script runs the complete schema evaluation pipeline to assess how well
extracted schemas enable answering research queries.

Usage:
    python evaluation/run_evaluation.py [config_path] [output_path]
    
Examples:
    # Use default config
    python evaluation/run_evaluation.py
    
    # Use custom config
    python evaluation/run_evaluation.py evaluation/config/custom_eval.json
    
    # Specify output path
    python evaluation/run_evaluation.py evaluation/config/evaluation_config.json results/eval_results.json
"""

import sys
import argparse
from pathlib import Path

from QBSD.evaluation.core.schema_evaluator import SchemaEvaluator


def main():
    """Run schema evaluation."""
    parser = argparse.ArgumentParser(description="Run schema evaluation")
    parser.add_argument(
        "config_path", 
        nargs='?',
        default="evaluation/config/evaluation_config.json",
        help="Path to evaluation configuration file"
    )
    parser.add_argument(
        "output_path",
        nargs='?', 
        default="outputs/evaluation_results.json",
        help="Path to save evaluation results"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    
    # Ensure paths are absolute
    config_path = Path(args.config_path).resolve()
    output_path = Path(args.output_path).resolve()
    
    # Check if config file exists
    if not config_path.exists():
        print(f"❌ Configuration file not found: {config_path}")
        print(f"💡 Try creating a config file based on evaluation/config/evaluation_config.json")
        return 1
    
    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        print(f"🚀 Starting Schema Evaluation")
        print(f"   Config: {config_path}")
        print(f"   Output: {output_path}")
        print()
        
        # Initialize evaluator
        evaluator = SchemaEvaluator(str(config_path))
        
        # Run evaluation
        result = evaluator.evaluate_schema()
        
        # Save results
        evaluator.save_results(result, str(output_path))
        
        # Print summary
        evaluator.print_summary(result)
        
        # Generate additional reports if configured
        _generate_additional_reports(result, output_path, args.verbose)
        
        print(f"✅ Evaluation completed successfully!")
        print(f"📄 Results saved to: {output_path}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Evaluation failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


def _generate_additional_reports(result, output_path, verbose=False):
    """Generate additional report files."""
    base_path = output_path.parent
    base_name = output_path.stem
    
    # Generate CSV summary for easy analysis
    csv_path = base_path / f"{base_name}_summary.csv"
    _export_metrics_to_csv(result, csv_path)
    
    # Generate markdown report
    md_path = base_path / f"{base_name}_report.md"
    _generate_markdown_report(result, md_path)
    
    if verbose:
        print(f"📊 Additional reports generated:")
        print(f"   CSV Summary: {csv_path}")
        print(f"   Markdown Report: {md_path}")


def _export_metrics_to_csv(result, csv_path):
    """Export metrics to CSV for analysis."""
    import csv
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow(['metric', 'value'])
        
        # Write overall metrics
        for metric, value in result.overall_metrics.items():
            writer.writerow([metric, value])
        
        # Write column analysis
        ca = result.column_analysis
        writer.writerow(['total_columns', ca.get('total_columns', 0)])
        writer.writerow(['unused_columns_count', len(ca.get('unused_columns', []))])
        writer.writerow(['highly_used_columns_count', len(ca.get('highly_used_columns', []))])


def _generate_markdown_report(result, md_path):
    """Generate markdown evaluation report."""
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write("# Schema Evaluation Report\n\n")
        
        f.write(f"**Query:** {result.query}\n\n")
        f.write(f"**Total Rows:** {result.total_rows}\n")
        f.write(f"**Evaluation Time:** {result.evaluation_time:.2f}s\n\n")
        
        f.write("## Overall Metrics\n\n")
        f.write("| Metric | Value |\n")
        f.write("|--------|-------|\n")
        for metric, value in result.overall_metrics.items():
            if isinstance(value, float):
                f.write(f"| {metric} | {value:.3f} |\n")
            else:
                f.write(f"| {metric} | {value} |\n")
        
        f.write("\n## Column Analysis\n\n")
        ca = result.column_analysis
        f.write(f"- **Total Schema Columns:** {ca.get('total_columns', 0)}\n")
        f.write(f"- **Unused Columns:** {len(ca.get('unused_columns', []))}\n")
        f.write(f"- **Highly Used Columns:** {', '.join(ca.get('highly_used_columns', [])[:5])}\n")
        f.write(f"- **Least Used Columns:** {', '.join(ca.get('least_used_columns', [])[:5])}\n")
        
        f.write("\n## Recommendations\n\n")
        for category, recs in result.recommendations.items():
            if recs:
                f.write(f"### {category.replace('_', ' ').title()}\n\n")
                for rec in recs:
                    f.write(f"- {rec}\n")
                f.write("\n")


if __name__ == "__main__":
    sys.exit(main())