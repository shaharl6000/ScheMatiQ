#!/usr/bin/env python3
"""
Evaluation script for QBSD valueExtractor outputs against ground truth data.
Dynamically aligns prediction and ground truth schemas using semantic similarity.
Handles schema mismatches and provides comprehensive evaluation metrics.

Usage:
    python evaluate_valueextractor.py --gt_file path/to/groundtruth.csv --pred_file path/to/predictions.json [--output_path results.json]
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
import re
import argparse
from difflib import SequenceMatcher
from sentence_transformers import SentenceTransformer
import torch

class ValueExtractorEvaluator:
    """Evaluates valueExtractor outputs against ground truth data."""
    
    def __init__(self, similarity_threshold: float = 0.7):
        self.similarity_threshold = similarity_threshold
        self.sentence_model = SentenceTransformer('all-MiniLM-L6-v2')
    
    def load_ground_truth(self, csv_path: str) -> pd.DataFrame:
        """Load ground truth data from CSV file."""
        print(f"Loading ground truth from: {csv_path}")
        
        # Try to load the CSV with different encodings
        try:
            gt_df = pd.read_csv(csv_path, encoding='utf-8')
        except UnicodeDecodeError:
            try:
                gt_df = pd.read_csv(csv_path, encoding='latin-1')
            except:
                gt_df = pd.read_csv(csv_path, encoding='cp1252')
        
        print(f"Loaded {len(gt_df)} ground truth entries")
        print(f"GT columns: {list(gt_df.columns)}")
        
        # Extract protein names for matching
        if 'ID' in gt_df.columns:
            gt_df['protein_name'] = gt_df['ID'].str.extract(r'([^(]+)')[0].str.strip()
        elif 'Full Name' in gt_df.columns:
            gt_df['protein_name'] = gt_df['Full Name']
        else:
            # Try to find any column that might contain protein names
            name_candidates = [col for col in gt_df.columns if 'name' in col.lower() or 'id' in col.lower()]
            if name_candidates:
                gt_df['protein_name'] = gt_df[name_candidates[0]]
            else:
                gt_df['protein_name'] = gt_df.index.astype(str)
        
        return gt_df
    
    def load_predictions(self, json_path: str) -> List[Dict]:
        """Load prediction data from JSON file."""
        print(f"Loading predictions from: {json_path}")
        
        predictions = []
        with open(json_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        pred = json.loads(line)
                        predictions.append(pred)
                    except json.JSONDecodeError as e:
                        print(f"Error parsing line: {line[:100]}... Error: {e}")
        
        print(f"Loaded {len(predictions)} predictions")
        
        # Print sample prediction structure
        if predictions:
            sample = predictions[0]
            print(f"Sample prediction keys: {list(sample.keys())}")
            if '_row_name' in sample:
                print(f"Sample _row_name: {sample['_row_name']}")
        
        return predictions
    
    def extract_protein_name(self, row_name: str) -> str:
        """Extract clean protein name from _row_name field."""
        # Remove common suffixes and clean up
        name = row_name.strip()
        name = re.sub(r'\s*\([^)]*\)', '', name)  # Remove parentheses
        name = re.sub(r'\s*\[[^\]]*\]', '', name)  # Remove brackets
        name = name.strip()
        return name
    
    def match_proteins(self, gt_df: pd.DataFrame, predictions: List[Dict]) -> Dict[str, Tuple[Dict, pd.Series]]:
        """Match predictions to ground truth entries by protein name."""
        matches = {}
        
        # Create mapping from GT
        gt_protein_map = {}
        for idx, row in gt_df.iterrows():
            protein_name = str(row.get('protein_name', '')).lower().strip()
            gt_protein_map[protein_name] = row
        
        print(f"GT proteins: {list(gt_protein_map.keys())[:5]}...")
        
        # Match predictions
        unmatched_predictions = []
        for pred in predictions:
            if '_row_name' not in pred:
                continue
                
            pred_name = self.extract_protein_name(pred['_row_name']).lower().strip()
            
            # Try exact match first
            if pred_name in gt_protein_map:
                matches[pred_name] = (pred, gt_protein_map[pred_name])
            else:
                # Try fuzzy matching
                best_match = None
                best_score = 0
                for gt_name in gt_protein_map.keys():
                    score = SequenceMatcher(None, pred_name, gt_name).ratio()
                    if score > best_score and score > 0.8:  # High threshold for protein matching
                        best_score = score
                        best_match = gt_name
                
                if best_match:
                    matches[pred_name] = (pred, gt_protein_map[best_match])
                    print(f"Fuzzy match: '{pred_name}' -> '{best_match}' (score: {best_score:.3f})")
                else:
                    unmatched_predictions.append(pred_name)
        
        print(f"Matched {len(matches)} proteins")
        if unmatched_predictions:
            print(f"Unmatched predictions: {unmatched_predictions[:5]}...")
            
        return matches
    
    def align_fields(self, pred_fields: List[str], gt_fields: List[str]) -> Dict[str, str]:
        """Align prediction fields to ground truth fields using dynamic similarity matching."""
        field_alignment = {}
        
        # Use similarity matching for all fields (no hardcoded mappings)
        for pred_field in pred_fields:
            pred_field_clean = pred_field.lower().strip()
            best_match = None
            best_score = 0
            
            # Find GT fields not already aligned
            available_gt_fields = [f for f in gt_fields if f not in field_alignment.values()]
            
            for gt_field in available_gt_fields:
                gt_field_clean = gt_field.lower().strip()
                
                # String similarity
                string_score = SequenceMatcher(None, pred_field_clean, gt_field_clean).ratio()
                
                # Semantic similarity using sentence transformers
                semantic_score = 0
                try:
                    embeddings = self.sentence_model.encode([pred_field_clean, gt_field_clean])
                    semantic_score = torch.cosine_similarity(
                        torch.tensor(embeddings[0]).unsqueeze(0),
                        torch.tensor(embeddings[1]).unsqueeze(0)
                    ).item()
                except:
                    pass
                
                # Combined score - give semantic similarity higher weight
                combined_score = 0.3 * string_score + 0.7 * semantic_score
                
                if combined_score > best_score and combined_score > self.similarity_threshold:
                    best_score = combined_score
                    best_match = gt_field
            
            if best_match:
                field_alignment[pred_field] = best_match
                print(f"Field alignment: '{pred_field}' -> '{best_match}' (score: {best_score:.3f})")
        
        return field_alignment
    
    def extract_value(self, pred_value: Any) -> str:
        """Extract actual value from prediction structure."""
        if pred_value is None:
            return ""
        
        if isinstance(pred_value, dict):
            # Handle {"answer": "...", "excerpts": ["..."]} structure
            if 'answer' in pred_value:
                return str(pred_value['answer']).strip()
            elif 'value' in pred_value:
                return str(pred_value['value']).strip()
            else:
                # Return the first non-empty value
                for key, value in pred_value.items():
                    if value and str(value).strip():
                        return str(value).strip()
        
        return str(pred_value).strip()
    
    def evaluate_field(self, gt_value: Any, pred_value: Any, field_name: str) -> Dict[str, float]:
        """Evaluate a single field comparison."""
        gt_str = str(gt_value).lower().strip() if gt_value and str(gt_value).strip() else ""
        pred_str = self.extract_value(pred_value).lower().strip()
        
        if not gt_str or not pred_str:
            return {
                'exact_match': 0.0,
                'substring_match': 0.0,
                'semantic_similarity': 0.0,
                'has_prediction': 1.0 if pred_str else 0.0
            }
        
        # Exact match
        exact_match = 1.0 if gt_str == pred_str else 0.0
        
        # Substring match (both directions)
        substring_match = 0.0
        if gt_str in pred_str or pred_str in gt_str:
            substring_match = 1.0
        elif len(gt_str) > 3 and len(pred_str) > 3:
            # For longer strings, check for significant overlap
            overlap = SequenceMatcher(None, gt_str, pred_str).ratio()
            if overlap > 0.5:
                substring_match = overlap
        
        # Semantic similarity
        semantic_similarity = 0.0
        try:
            if len(gt_str) > 2 and len(pred_str) > 2:
                embeddings = self.sentence_model.encode([gt_str, pred_str])
                semantic_similarity = torch.cosine_similarity(
                    torch.tensor(embeddings[0]).unsqueeze(0),
                    torch.tensor(embeddings[1]).unsqueeze(0)
                ).item()
                semantic_similarity = max(0.0, semantic_similarity)  # Ensure non-negative
        except:
            pass
        
        # Special handling for yes/no fields
        if field_name.lower() in ['nes_presence', 'leucine_rich', 'crm1_dependency', 'exportin1_dependency', 'nes_masking']:
            gt_bool = gt_str.lower() in ['yes', 'true', '1', 'positive', 'present']
            pred_bool = pred_str.lower() in ['yes', 'true', '1', 'positive', 'present']
            if gt_bool == pred_bool:
                exact_match = 1.0
                substring_match = 1.0
                semantic_similarity = 1.0
            else:
                exact_match = substring_match = semantic_similarity = 0.0
        
        return {
            'exact_match': exact_match,
            'substring_match': substring_match,
            'semantic_similarity': semantic_similarity,
            'has_prediction': 1.0
        }
    
    def evaluate_protein(self, gt_row: pd.Series, pred_dict: Dict) -> Dict[str, Any]:
        """Evaluate all fields for a single protein."""
        
        # Get all GT fields (exclude metadata columns)
        metadata_cols = ['protein_name', 'ID', 'Full Name', 'Alternative Names', 'Organism', 'Fasta Header', 'Sequence', 'CRM1_hash', 'Peptide_hash', 'Negative_hash', 'combined']
        gt_fields = [col for col in gt_row.index if col not in metadata_cols and pd.notna(gt_row[col]) and str(gt_row[col]).strip()]
        
        # Get prediction fields (exclude metadata and excerpt columns)
        pred_fields = [key for key in pred_dict.keys() 
                      if not key.startswith('_') 
                      and not key.endswith('_excerpts')  # Exclude excerpt columns
                      and key not in ['"name"', '"definition"', '"rationale"', 'name', 'definition', 'rationale']]
        
        # Align fields (pred_field -> gt_field)
        field_alignment = self.align_fields(pred_fields, gt_fields)
        
        field_results = {}
        total_pred_fields = len(pred_fields)
        matched_fields = 0
        total_scores = {'exact_match': 0, 'substring_match': 0, 'semantic_similarity': 0}
        
        # Evaluate each prediction field
        for pred_field in pred_fields:
            pred_value = pred_dict.get(pred_field, None)
            
            if pred_field in field_alignment:
                gt_field = field_alignment[pred_field]
                gt_value = gt_row[gt_field]
                scores = self.evaluate_field(gt_value, pred_value, pred_field)
                
                field_results[pred_field] = {
                    'gt_field': gt_field,
                    'gt_value': str(gt_value),
                    'pred_value': self.extract_value(pred_value),
                    'scores': scores
                }
                
                matched_fields += 1
                for metric in total_scores:
                    total_scores[metric] += scores[metric]
            else:
                field_results[pred_field] = {
                    'gt_field': None,
                    'gt_value': "",
                    'pred_value': self.extract_value(pred_value),
                    'scores': {'exact_match': 0, 'substring_match': 0, 'semantic_similarity': 0, 'has_prediction': 1}
                }
        
        # Calculate aggregate metrics 
        # Note: Using arxivDIGESTables-style recall = matched_gold_fields / total_gold_fields
        # But we're evaluating from prediction perspective, so we use pred fields as base
        field_recall = matched_fields / total_pred_fields if total_pred_fields > 0 else 0
        
        # Also calculate GT-style recall for comparison
        gt_fields_matched = len(set(field_alignment.values()))  # Unique GT fields matched
        gt_total_fields = len(gt_fields)  # Total GT fields available
        gt_recall = gt_fields_matched / gt_total_fields if gt_total_fields > 0 else 0
        
        avg_scores = {metric: score / matched_fields if matched_fields > 0 else 0 
                     for metric, score in total_scores.items()}
        
        return {
            'field_results': field_results,
            'field_recall': field_recall,  # Prediction-based recall
            'gt_recall': gt_recall,         # GT-based recall (arxivDIGESTables style)
            'matched_fields': matched_fields,
            'total_pred_fields': total_pred_fields,
            'total_gt_fields': gt_total_fields,
            'avg_scores': avg_scores,
            'alignment_matrix': field_alignment
        }
    
    def calculate_arxiv_style_recall(self, protein_results: Dict[str, Any]) -> Dict[str, float]:
        """Calculate recall metrics in the style of arxivDIGESTables SchemaRecallMetric."""
        
        all_gt_recalls = []
        all_field_recalls = []
        all_exact_matches = []
        all_semantic_similarities = []
        
        # Collect scores across all proteins
        for protein_data in protein_results.values():
            all_gt_recalls.append(protein_data['gt_recall'])
            all_field_recalls.append(protein_data['field_recall'])
            all_exact_matches.append(protein_data['avg_scores']['exact_match'])
            all_semantic_similarities.append(protein_data['avg_scores']['semantic_similarity'])
        
        return {
            'schema_recall': np.mean(all_gt_recalls),  # arxivDIGESTables style
            'field_recall': np.mean(all_field_recalls),  # Prediction-based
            'exact_match_score': np.mean(all_exact_matches),
            'semantic_similarity_score': np.mean(all_semantic_similarities),
            'median_schema_recall': np.median(all_gt_recalls),
            'std_schema_recall': np.std(all_gt_recalls)
        }
    
    def run_evaluation(self, gt_csv_path: str, pred_json_path: str, output_path: str = None) -> Dict[str, Any]:
        """Run complete evaluation and return results."""
        
        # Load data
        gt_df = self.load_ground_truth(gt_csv_path)
        predictions = self.load_predictions(pred_json_path)
        
        # Match proteins
        protein_matches = self.match_proteins(gt_df, predictions)
        
        if not protein_matches:
            print("ERROR: No protein matches found between GT and predictions!")
            return {}
        
        # Evaluate each protein
        results = {}
        print("\nEvaluating proteins...")
        for protein_name, (pred_dict, gt_row) in protein_matches.items():
            protein_result = self.evaluate_protein(gt_row, pred_dict)
            results[protein_name] = protein_result
        
        # Calculate overall statistics using both approaches
        arxiv_metrics = self.calculate_arxiv_style_recall(results)
        
        overall_stats = {
            'total_proteins_evaluated': len(results),
            # arxivDIGESTables-style metrics
            'schema_recall': arxiv_metrics['schema_recall'],  # GT-based recall
            'field_recall': arxiv_metrics['field_recall'],   # Prediction-based recall  
            'exact_match_score': arxiv_metrics['exact_match_score'],
            'semantic_similarity_score': arxiv_metrics['semantic_similarity_score'],
            'median_schema_recall': arxiv_metrics['median_schema_recall'],
            'std_schema_recall': arxiv_metrics['std_schema_recall'],
            # Legacy metrics for compatibility
            'avg_field_recall': arxiv_metrics['field_recall'],
            'avg_exact_match': arxiv_metrics['exact_match_score'], 
            'avg_semantic_similarity': arxiv_metrics['semantic_similarity_score']
        }
        
        # Print summary
        print(f"\n=== EVALUATION SUMMARY ===")
        print(f"Total proteins evaluated: {overall_stats['total_proteins_evaluated']}")
        print(f"\n--- arxivDIGESTables-Style Metrics ---")
        print(f"Schema recall (GT-based): {overall_stats['schema_recall']:.3f}")
        print(f"Field recall (Pred-based): {overall_stats['field_recall']:.3f}")
        print(f"Exact match score: {overall_stats['exact_match_score']:.3f}")
        print(f"Semantic similarity score: {overall_stats['semantic_similarity_score']:.3f}")
        print(f"Median schema recall: {overall_stats['median_schema_recall']:.3f}")
        print(f"Schema recall std dev: {overall_stats['std_schema_recall']:.3f}")
        
        # Save detailed results
        if output_path:
            output_data = {
                'overall_stats': overall_stats,
                'protein_results': results
            }
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            print(f"\nDetailed results saved to: {output_path}")
        
        return {
            'overall_stats': overall_stats,
            'protein_results': results
        }


def main():
    """Main execution function."""
    
    parser = argparse.ArgumentParser(description='Evaluate valueExtractor outputs against ground truth data')
    parser.add_argument('--gt_file', required=True, help='Path to ground truth CSV file')
    parser.add_argument('--pred_file', required=True, help='Path to predictions JSON file')
    parser.add_argument('--output_path', help='Path for output results (optional, auto-generated if not provided)')
    parser.add_argument('--similarity_threshold', type=float, default=0.3, help='Similarity threshold for field alignment (default: 0.7)')
    
    args = parser.parse_args()
    
    # Generate output path if not provided
    if not args.output_path:
        pred_path = Path(args.pred_file)
        output_path = f"evaluation_results_{pred_path.stem}.json"
    else:
        output_path = args.output_path
    
    # Check if files exist
    if not Path(args.gt_file).exists():
        print(f"ERROR: Ground truth file not found: {args.gt_file}")
        return
    
    if not Path(args.pred_file).exists():
        print(f"ERROR: Predictions file not found: {args.pred_file}")
        return
    
    # Run evaluation
    evaluator = ValueExtractorEvaluator(similarity_threshold=args.similarity_threshold)
    results = evaluator.run_evaluation(args.gt_file, args.pred_file, output_path)
    
    # Print additional analysis
    if results and 'protein_results' in results:
        print(f"\n=== DETAILED ANALYSIS ===")
        
        # Best and worst performing proteins
        protein_scores = [(name, data['field_recall']) 
                         for name, data in results['protein_results'].items()]
        protein_scores.sort(key=lambda x: x[1], reverse=True)
        
        print(f"Best performing proteins (schema recall):")
        for name, score in protein_scores[:3]:
            gt_recall = results['protein_results'][name]['gt_recall'] 
            matched = results['protein_results'][name]['matched_fields']
            total_pred = results['protein_results'][name]['total_pred_fields']
            total_gt = results['protein_results'][name]['total_gt_fields']
            print(f"  {name}: {gt_recall:.3f} GT recall | {score:.3f} field recall ({matched}/{total_pred} pred fields, {len(set(results['protein_results'][name]['alignment_matrix'].values()))}/{total_gt} GT fields)")
        
        print(f"Worst performing proteins (schema recall):")
        for name, score in protein_scores[-3:]:
            gt_recall = results['protein_results'][name]['gt_recall']
            matched = results['protein_results'][name]['matched_fields']
            total_pred = results['protein_results'][name]['total_pred_fields']
            total_gt = results['protein_results'][name]['total_gt_fields']
            print(f"  {name}: {gt_recall:.3f} GT recall | {score:.3f} field recall ({matched}/{total_pred} pred fields, {len(set(results['protein_results'][name]['alignment_matrix'].values()))}/{total_gt} GT fields)")
        
        # Field analysis - which prediction fields perform best
        field_performance = {}
        field_alignment_success = {}
        
        for protein_data in results['protein_results'].values():
            for field_name, field_data in protein_data['field_results'].items():
                if field_name not in field_performance:
                    field_performance[field_name] = []
                    field_alignment_success[field_name] = 0
                
                field_performance[field_name].append(field_data['scores']['semantic_similarity'])
                if field_data['gt_field']:
                    field_alignment_success[field_name] += 1
        
        print(f"\nPrediction field performance (avg semantic similarity):")
        for field_name, scores in sorted(field_performance.items(), 
                                       key=lambda x: np.mean(x[1]), reverse=True)[:10]:
            avg_score = np.mean(scores)
            success_rate = field_alignment_success[field_name] / len(scores)
            print(f"  {field_name}: {avg_score:.3f} (aligned {success_rate:.1%} of the time)")
        
        # Show some successful alignments
        print(f"\nSuccessful field alignments found:")
        alignments_found = set()
        for protein_data in results['protein_results'].values():
            for field_name, field_data in protein_data['field_results'].items():
                if field_data['gt_field'] and (field_name, field_data['gt_field']) not in alignments_found:
                    alignments_found.add((field_name, field_data['gt_field']))
                    print(f"  {field_name} -> {field_data['gt_field']}")
                    if len(alignments_found) >= 10:  # Limit output
                        break
            if len(alignments_found) >= 10:
                break


if __name__ == "__main__":
    main()