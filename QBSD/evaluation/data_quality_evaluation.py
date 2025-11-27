#!/usr/bin/env python3
"""
Data Quality Evaluation for QBSD Value Extraction

Compares extracted data CSV against ground truth CSV by:
1. Aligning rows based on protein name matching  
2. Evaluating schema similarity (column name alignment)
3. Evaluating value similarity for aligned fields

Usage:
    python data_quality_evaluation.py --gt_file data/NesDB_all_CRM1_with_peptides.csv --pred_file data/orig_nes_27.csv [--output results.json]
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional, Set
import argparse
from dataclasses import dataclass
from sentence_transformers import SentenceTransformer
import torch
import re


@dataclass
class RowAlignment:
    """Represents an aligned GT-prediction row pair."""
    gt_row: pd.Series
    pred_row: pd.Series
    gt_id: str
    pred_name: str
    match_confidence: float


@dataclass
class FieldAlignment:
    """Represents alignment between GT and prediction field names."""
    gt_field: str
    pred_field: str
    similarity_score: float


@dataclass
class EvaluationResult:
    """Complete evaluation results."""
    schema_similarity: Dict[str, Any]
    value_similarity: Dict[str, Any] 
    row_alignments: List[RowAlignment]
    field_alignments: List[FieldAlignment]
    summary_metrics: Dict[str, float]


class DataLoader:
    """Handles loading and preprocessing CSV data files."""
    
    @staticmethod
    def load_csv(file_path: str) -> pd.DataFrame:
        """Load CSV with encoding detection."""
        print(f"Loading data from: {file_path}")
        
        # Try multiple encodings
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                print(f"  ✓ Loaded {len(df)} rows using {encoding} encoding")
                return df
            except UnicodeDecodeError:
                continue
        
        raise ValueError(f"Could not read {file_path} with any encoding")


class RowMatcher:
    """Handles matching rows between GT and prediction data."""
    
    def __init__(self):
        self.fuzzy_threshold = 0.8
    
    def match_rows(self, gt_df: pd.DataFrame, pred_df: pd.DataFrame) -> List[RowAlignment]:
        """
        Match prediction rows to GT rows based on protein name containment.
        Logic: pred row_name should be contained in GT ID string.
        """
        alignments = []
        
        # Extract clean protein names from GT IDs
        gt_proteins = {}
        for idx, row in gt_df.iterrows():
            gt_id = str(row.get('ID', ''))
            # Extract main protein name before parentheses
            clean_name = re.sub(r'\s*\([^)]*\)', '', gt_id).strip().lower()
            gt_proteins[clean_name] = (gt_id, row)
        
        print(f"GT proteins extracted: {len(gt_proteins)}")
        print(f"Sample GT names: {list(gt_proteins.keys())[:3]}")
        
        # Match prediction rows
        matched_count = 0
        for idx, pred_row in pred_df.iterrows():
            pred_name = str(pred_row.get('row_name', '')).strip().lower()
            
            if not pred_name:
                continue
            
            best_match = None
            best_confidence = 0.0
            
            # Check if pred_name is contained in any GT protein name
            for gt_clean_name, (gt_id, gt_row) in gt_proteins.items():
                if pred_name in gt_clean_name or gt_clean_name in pred_name:
                    # Calculate simple similarity as confidence
                    confidence = min(len(pred_name), len(gt_clean_name)) / max(len(pred_name), len(gt_clean_name))
                    
                    if confidence > best_confidence:
                        best_confidence = confidence
                        best_match = (gt_id, gt_row)
            
            if best_match and best_confidence > 0.3:  # Minimum threshold
                gt_id, gt_row = best_match
                alignments.append(RowAlignment(
                    gt_row=gt_row,
                    pred_row=pred_row,
                    gt_id=gt_id,
                    pred_name=pred_name,
                    match_confidence=best_confidence
                ))
                matched_count += 1
        
        print(f"Successfully matched {matched_count} proteins")
        return alignments


class FieldAligner:
    """Handles alignment between GT and prediction field names."""
    
    def __init__(self):
        self.sentence_model = SentenceTransformer('all-MiniLM-L6-v2')
        self.similarity_threshold = 0.6
    
    def align_fields(self, gt_columns: List[str], pred_columns: List[str]) -> List[FieldAlignment]:
        """Align prediction fields to GT fields using semantic similarity."""
        alignments = []
        
        # Filter out metadata columns
        gt_fields = [col for col in gt_columns if not self._is_metadata_column(col)]
        pred_fields = [col for col in pred_columns if not self._is_metadata_column(col)]
        
        print(f"Aligning {len(pred_fields)} prediction fields to {len(gt_fields)} GT fields")
        
        used_gt_fields = set()
        
        for pred_field in pred_fields:
            best_match = None
            best_score = 0.0
            
            for gt_field in gt_fields:
                if gt_field in used_gt_fields:
                    continue
                
                similarity = self._calculate_field_similarity(pred_field, gt_field)
                
                if similarity > best_score and similarity > self.similarity_threshold:
                    best_score = similarity
                    best_match = gt_field
            
            if best_match:
                alignments.append(FieldAlignment(
                    gt_field=best_match,
                    pred_field=pred_field,
                    similarity_score=best_score
                ))
                used_gt_fields.add(best_match)
                print(f"  {pred_field} → {best_match} (score: {best_score:.3f})")
        
        return alignments
    
    def _is_metadata_column(self, column: str) -> bool:
        """Check if column is metadata (should be excluded from alignment)."""
        metadata_patterns = [
            'id', 'row_name', 'protein_name', 'fasta', 'sequence', 'hash', 
            'combined', 'gt_', '_id', 'index'
        ]
        column_lower = column.lower()
        return any(pattern in column_lower for pattern in metadata_patterns)
    
    def _calculate_field_similarity(self, field1: str, field2: str) -> float:
        """Calculate semantic similarity between two field names."""
        # String similarity
        field1_clean = field1.lower().strip()
        field2_clean = field2.lower().strip()
        
        # Exact match
        if field1_clean == field2_clean:
            return 1.0
        
        # Substring match
        if field1_clean in field2_clean or field2_clean in field1_clean:
            return 0.8
        
        # Semantic similarity
        try:
            embeddings = self.sentence_model.encode([field1_clean, field2_clean])
            semantic_sim = torch.cosine_similarity(
                torch.tensor(embeddings[0]).unsqueeze(0),
                torch.tensor(embeddings[1]).unsqueeze(0)
            ).item()
            return max(0.0, semantic_sim)
        except:
            return 0.0


class ValueEvaluator:
    """Evaluates similarity between GT and prediction values."""
    
    def __init__(self):
        self.sentence_model = SentenceTransformer('all-MiniLM-L6-v2')
    
    def evaluate_values(self, row_alignments: List[RowAlignment], 
                       field_alignments: List[FieldAlignment]) -> Dict[str, Any]:
        """Evaluate value similarity for aligned rows and fields."""
        
        if not row_alignments or not field_alignments:
            return {'error': 'No alignments to evaluate'}
        
        # Create field mapping
        field_map = {fa.pred_field: fa.gt_field for fa in field_alignments}
        
        all_scores = []
        field_scores = {}
        
        for row_alignment in row_alignments:
            gt_row = row_alignment.gt_row
            pred_row = row_alignment.pred_row
            
            for pred_field, gt_field in field_map.items():
                gt_value = self._extract_value(gt_row.get(gt_field, ''))
                pred_value = self._extract_value(pred_row.get(pred_field, ''))
                
                if not gt_value or not pred_value:
                    continue
                
                score = self._calculate_value_similarity(gt_value, pred_value)
                all_scores.append(score)
                
                if pred_field not in field_scores:
                    field_scores[pred_field] = []
                field_scores[pred_field].append(score)
        
        # Calculate aggregate metrics
        avg_field_scores = {field: np.mean(scores) for field, scores in field_scores.items()}
        
        return {
            'overall_similarity': np.mean(all_scores) if all_scores else 0.0,
            'median_similarity': np.median(all_scores) if all_scores else 0.0,
            'field_similarities': avg_field_scores,
            'total_comparisons': len(all_scores),
            'best_fields': sorted(avg_field_scores.items(), key=lambda x: x[1], reverse=True)[:5],
            'worst_fields': sorted(avg_field_scores.items(), key=lambda x: x[1])[:5]
        }
    
    def _extract_value(self, value: Any) -> str:
        """Extract clean string value from various data types."""
        if pd.isna(value):
            return ''
        
        value_str = str(value).strip()
        
        # Handle JSON-like structures from QBSD extraction
        if value_str.startswith('[{') and 'value' in value_str:
            try:
                import ast
                parsed = ast.literal_eval(value_str)
                if isinstance(parsed, list) and parsed:
                    if isinstance(parsed[0], dict) and 'value' in parsed[0]:
                        return str(parsed[0]['value']).strip()
            except:
                pass
        
        return value_str.lower()
    
    def _calculate_value_similarity(self, val1: str, val2: str) -> float:
        """Calculate similarity between two values."""
        val1 = val1.lower().strip()
        val2 = val2.lower().strip()
        
        if not val1 or not val2:
            return 0.0
        
        # Exact match
        if val1 == val2:
            return 1.0
        
        # Substring match
        if val1 in val2 or val2 in val1:
            return 0.7
        
        # Semantic similarity for longer texts
        if len(val1) > 10 and len(val2) > 10:
            try:
                embeddings = self.sentence_model.encode([val1, val2])
                semantic_sim = torch.cosine_similarity(
                    torch.tensor(embeddings[0]).unsqueeze(0),
                    torch.tensor(embeddings[1]).unsqueeze(0)
                ).item()
                return max(0.0, semantic_sim)
            except:
                pass
        
        return 0.0


class DataQualityEvaluator:
    """Main evaluator orchestrating the complete evaluation pipeline."""
    
    def __init__(self):
        self.data_loader = DataLoader()
        self.row_matcher = RowMatcher()
        self.field_aligner = FieldAligner()
        self.value_evaluator = ValueEvaluator()
    
    def evaluate(self, gt_csv_path: str, pred_csv_path: str) -> EvaluationResult:
        """Run complete data quality evaluation."""
        
        # Load data
        print("=== Loading Data ===")
        gt_df = self.data_loader.load_csv(gt_csv_path)
        pred_df = self.data_loader.load_csv(pred_csv_path)
        
        print(f"GT columns: {list(gt_df.columns)[:5]}...")
        print(f"Prediction columns: {list(pred_df.columns)[:5]}...")
        
        # Match rows
        print("\n=== Matching Rows ===")
        row_alignments = self.row_matcher.match_rows(gt_df, pred_df)
        
        if not row_alignments:
            raise ValueError("No rows could be aligned between GT and predictions")
        
        # Align fields
        print("\n=== Aligning Fields ===")
        field_alignments = self.field_aligner.align_fields(gt_df.columns.tolist(), pred_df.columns.tolist())
        
        # Evaluate schema similarity
        schema_similarity = {
            'total_gt_fields': len([col for col in gt_df.columns if not self.field_aligner._is_metadata_column(col)]),
            'total_pred_fields': len([col for col in pred_df.columns if not self.field_aligner._is_metadata_column(col)]),
            'aligned_fields': len(field_alignments),
            'schema_recall': len(field_alignments) / len([col for col in gt_df.columns if not self.field_aligner._is_metadata_column(col)]) if len([col for col in gt_df.columns if not self.field_aligner._is_metadata_column(col)]) > 0 else 0,
            'schema_precision': len(field_alignments) / len([col for col in pred_df.columns if not self.field_aligner._is_metadata_column(col)]) if len([col for col in pred_df.columns if not self.field_aligner._is_metadata_column(col)]) > 0 else 0,
            'avg_field_similarity': np.mean([fa.similarity_score for fa in field_alignments]) if field_alignments else 0.0
        }
        
        # Calculate schema F1
        if schema_similarity['schema_recall'] + schema_similarity['schema_precision'] > 0:
            schema_similarity['schema_f1'] = 2 * (schema_similarity['schema_recall'] * schema_similarity['schema_precision']) / (schema_similarity['schema_recall'] + schema_similarity['schema_precision'])
        else:
            schema_similarity['schema_f1'] = 0.0
        
        # Evaluate value similarity
        print("\n=== Evaluating Values ===")
        value_similarity = self.value_evaluator.evaluate_values(row_alignments, field_alignments)
        
        # Summary metrics
        summary_metrics = {
            'rows_matched': len(row_alignments),
            'total_gt_rows': len(gt_df),
            'total_pred_rows': len(pred_df),
            'row_match_rate': len(row_alignments) / min(len(gt_df), len(pred_df)),
            'schema_recall': schema_similarity['schema_recall'],
            'schema_precision': schema_similarity['schema_precision'], 
            'schema_f1': schema_similarity['schema_f1'],
            'avg_value_similarity': value_similarity.get('overall_similarity', 0.0),
            'median_value_similarity': value_similarity.get('median_similarity', 0.0)
        }
        
        return EvaluationResult(
            schema_similarity=schema_similarity,
            value_similarity=value_similarity,
            row_alignments=row_alignments,
            field_alignments=field_alignments,
            summary_metrics=summary_metrics
        )
    
    def print_summary(self, result: EvaluationResult):
        """Print evaluation summary to console."""
        print("\n" + "="*50)
        print("DATA QUALITY EVALUATION SUMMARY")
        print("="*50)
        
        metrics = result.summary_metrics
        
        print(f"\n📊 ROW ALIGNMENT:")
        print(f"  Matched rows: {metrics['rows_matched']} / {metrics['total_pred_rows']} predictions")
        print(f"  Row match rate: {metrics['row_match_rate']:.3f}")
        
        print(f"\n🏗️  SCHEMA SIMILARITY:")
        print(f"  Schema recall: {metrics['schema_recall']:.3f}")
        print(f"  Schema precision: {metrics['schema_precision']:.3f}")
        print(f"  Schema F1: {metrics['schema_f1']:.3f}")
        print(f"  Avg field similarity: {result.schema_similarity['avg_field_similarity']:.3f}")
        
        print(f"\n💎 VALUE SIMILARITY:")
        print(f"  Overall similarity: {metrics['avg_value_similarity']:.3f}")
        print(f"  Median similarity: {metrics['median_value_similarity']:.3f}")
        print(f"  Total comparisons: {result.value_similarity.get('total_comparisons', 0)}")
        
        # Show best/worst performing fields
        if 'best_fields' in result.value_similarity:
            print(f"\n🏆 BEST PERFORMING FIELDS:")
            for field, score in result.value_similarity['best_fields']:
                print(f"  {field}: {score:.3f}")
        
        if 'worst_fields' in result.value_similarity:
            print(f"\n📉 WORST PERFORMING FIELDS:")
            for field, score in result.value_similarity['worst_fields']:
                print(f"  {field}: {score:.3f}")
    
    def save_results(self, result: EvaluationResult, output_path: str):
        """Save detailed results to JSON file."""
        # Convert dataclass and pandas objects to serializable format
        output_data = {
            'schema_similarity': result.schema_similarity,
            'value_similarity': result.value_similarity,
            'summary_metrics': result.summary_metrics,
            'field_alignments': [
                {
                    'gt_field': fa.gt_field,
                    'pred_field': fa.pred_field,
                    'similarity_score': fa.similarity_score
                } for fa in result.field_alignments
            ],
            'row_alignments': [
                {
                    'gt_id': ra.gt_id,
                    'pred_name': ra.pred_name,
                    'match_confidence': ra.match_confidence
                } for ra in result.row_alignments
            ]
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Results saved to: {output_path}")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Evaluate data quality: GT CSV vs predictions CSV')
    parser.add_argument('--gt_file', required=True, help='Path to ground truth CSV file')
    parser.add_argument('--pred_file', required=True, help='Path to predictions CSV file')
    parser.add_argument('--output', help='Path to save results JSON (optional)')
    
    args = parser.parse_args()
    
    # Validate input files
    if not Path(args.gt_file).exists():
        print(f"❌ Ground truth file not found: {args.gt_file}")
        return 1
    
    if not Path(args.pred_file).exists():
        print(f"❌ Predictions file not found: {args.pred_file}")
        return 1
    
    # Generate output path if not provided
    if not args.output:
        pred_name = Path(args.pred_file).stem
        args.output = f"data_quality_evaluation_{pred_name}.json"
    
    try:
        # Run evaluation
        evaluator = DataQualityEvaluator()
        result = evaluator.evaluate(args.gt_file, args.pred_file)
        
        # Display results
        evaluator.print_summary(result)
        
        # Save results
        evaluator.save_results(result, args.output)
        
        return 0
        
    except Exception as e:
        print(f"❌ Evaluation failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())