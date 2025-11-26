"""Ground truth comparison utilities for schema evaluation."""

import re
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from difflib import SequenceMatcher

@dataclass 
class ComparisonResult:
    """Result of comparing predicted answer to ground truth."""
    exact_match: bool
    semantic_similarity: float
    contains_gt: bool
    gt_in_prediction: bool
    confidence_score: float
    comparison_type: str
    details: Dict[str, Any]


class GTComparator:
    """Compares LLM predictions to ground truth values."""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize GTComparator.
        
        Args:
            config: Configuration for comparison strategies
        """
        self.config = config or {}
        
        # Get GT options configuration
        gt_config = self.config.get('gt_options', {})
        self.binary_options = gt_config.get('binary_options', ['yes', 'no', 'true', 'false', 'positive', 'negative'])
        
        self.comparison_strategies = {
            'exact': self._exact_match,
            'contains': self._contains_match, 
            'semantic': self._semantic_similarity,
            'binary': self._binary_match,
            'sequence': self._sequence_match,
            'numeric': self._numeric_match
        }
    
    def compare_answers(self, 
                       predicted_answer: str,
                       gt_answer: str,
                       comparison_type: str = 'auto') -> ComparisonResult:
        """
        Compare predicted answer to ground truth.
        
        Args:
            predicted_answer: LLM's predicted answer
            gt_answer: Ground truth answer
            comparison_type: Type of comparison to perform
            
        Returns:
            ComparisonResult with detailed comparison metrics
        """
        if not predicted_answer or not gt_answer:
            return ComparisonResult(
                exact_match=False,
                semantic_similarity=0.0,
                contains_gt=False,
                gt_in_prediction=False,
                confidence_score=0.0,
                comparison_type='empty',
                details={'error': 'Empty answer(s)'}
            )
        
        # Auto-detect comparison type if needed
        if comparison_type == 'auto':
            comparison_type = self._detect_comparison_type(gt_answer)
        
        # Perform comparison using appropriate strategy
        if comparison_type in self.comparison_strategies:
            return self.comparison_strategies[comparison_type](predicted_answer, gt_answer)
        else:
            # Fallback to semantic comparison
            return self._semantic_similarity(predicted_answer, gt_answer)
    
    def _detect_comparison_type(self, gt_answer: str) -> str:
        """Automatically detect the best comparison strategy."""
        gt_lower = gt_answer.lower().strip()
        
        # Binary answers (use configurable options)
        binary_options_lower = [opt.lower() for opt in self.binary_options]
        if gt_lower in binary_options_lower:
            return 'binary'
        
        # Numeric answers
        try:
            float(gt_answer)
            return 'numeric'
        except ValueError:
            pass
        
        # Sequence-like answers (might be protein sequences, IDs, etc.)
        if len(gt_answer) > 10 and re.match(r'^[A-Z]+$', gt_answer):
            return 'sequence'
        
        # Default to semantic similarity
        return 'semantic'
    
    def _exact_match(self, predicted: str, gt: str) -> ComparisonResult:
        """Perform exact string matching."""
        pred_clean = predicted.strip().lower()
        gt_clean = gt.strip().lower()
        
        exact_match = pred_clean == gt_clean
        
        return ComparisonResult(
            exact_match=exact_match,
            semantic_similarity=1.0 if exact_match else 0.0,
            contains_gt=gt_clean in pred_clean,
            gt_in_prediction=gt_clean in pred_clean,
            confidence_score=1.0 if exact_match else 0.0,
            comparison_type='exact',
            details={'predicted_clean': pred_clean, 'gt_clean': gt_clean}
        )
    
    def _contains_match(self, predicted: str, gt: str) -> ComparisonResult:
        """Check if prediction contains ground truth."""
        pred_clean = predicted.strip().lower()
        gt_clean = gt.strip().lower()
        
        contains_gt = gt_clean in pred_clean
        gt_in_prediction = gt_clean in pred_clean
        
        # Calculate similarity based on overlap
        if contains_gt:
            similarity = min(1.0, len(gt_clean) / len(pred_clean))
        else:
            similarity = 0.0
        
        return ComparisonResult(
            exact_match=pred_clean == gt_clean,
            semantic_similarity=similarity,
            contains_gt=contains_gt,
            gt_in_prediction=gt_in_prediction,
            confidence_score=similarity,
            comparison_type='contains',
            details={'overlap_ratio': similarity}
        )
    
    def _semantic_similarity(self, predicted: str, gt: str) -> ComparisonResult:
        """Calculate semantic similarity using string matching."""
        pred_clean = predicted.strip().lower()
        gt_clean = gt.strip().lower()
        
        # Use difflib for similarity scoring
        similarity = SequenceMatcher(None, pred_clean, gt_clean).ratio()
        
        # Check containment
        contains_gt = gt_clean in pred_clean
        gt_in_prediction = contains_gt
        
        # Consider high similarity as match
        exact_match = similarity >= 0.95
        
        return ComparisonResult(
            exact_match=exact_match,
            semantic_similarity=similarity,
            contains_gt=contains_gt,
            gt_in_prediction=gt_in_prediction,
            confidence_score=similarity,
            comparison_type='semantic',
            details={'sequence_similarity': similarity}
        )
    
    def _binary_match(self, predicted: str, gt: str) -> ComparisonResult:
        """Handle binary (Yes/No, True/False) comparisons."""
        # Normalize binary answers
        pred_binary = self._normalize_binary(predicted)
        gt_binary = self._normalize_binary(gt)
        
        exact_match = pred_binary == gt_binary
        
        return ComparisonResult(
            exact_match=exact_match,
            semantic_similarity=1.0 if exact_match else 0.0,
            contains_gt=gt.lower() in predicted.lower(),
            gt_in_prediction=gt.lower() in predicted.lower(),
            confidence_score=1.0 if exact_match else 0.0,
            comparison_type='binary',
            details={
                'predicted_binary': pred_binary,
                'gt_binary': gt_binary
            }
        )
    
    def _normalize_binary(self, answer: str) -> Optional[bool]:
        """Normalize answer to binary value."""
        answer_lower = answer.lower().strip()
        
        # Positive indicators
        if any(word in answer_lower for word in ['yes', 'true', 'positive', 'confirmed', 'present']):
            return True
        
        # Negative indicators  
        if any(word in answer_lower for word in ['no', 'false', 'negative', 'absent', 'not', 'none']):
            return False
        
        # Insufficient information
        if any(phrase in answer_lower for phrase in ['insufficient', 'unclear', 'unknown']):
            return None
        
        return None
    
    def _sequence_match(self, predicted: str, gt: str) -> ComparisonResult:
        """Handle sequence matching (e.g., protein sequences)."""
        # Extract sequences from both answers
        pred_seq = self._extract_sequence(predicted)
        gt_seq = self._extract_sequence(gt)
        
        if not pred_seq or not gt_seq:
            return self._semantic_similarity(predicted, gt)
        
        # Calculate sequence similarity
        similarity = SequenceMatcher(None, pred_seq, gt_seq).ratio()
        exact_match = pred_seq == gt_seq
        contains_gt = gt_seq in pred_seq
        
        return ComparisonResult(
            exact_match=exact_match,
            semantic_similarity=similarity,
            contains_gt=contains_gt,
            gt_in_prediction=contains_gt,
            confidence_score=similarity,
            comparison_type='sequence',
            details={
                'predicted_sequence': pred_seq,
                'gt_sequence': gt_seq,
                'sequence_similarity': similarity
            }
        )
    
    def _extract_sequence(self, text: str) -> str:
        """Extract sequence-like strings from text."""
        # Look for patterns like protein sequences (uppercase letters)
        sequences = re.findall(r'[A-Z]{3,}', text)
        return max(sequences, key=len) if sequences else ""
    
    def _numeric_match(self, predicted: str, gt: str) -> ComparisonResult:
        """Handle numeric comparisons."""
        try:
            pred_num = float(re.search(r'[\d.]+', predicted).group())
            gt_num = float(gt)
            
            # Calculate relative error
            if gt_num == 0:
                exact_match = pred_num == 0
                similarity = 1.0 if exact_match else 0.0
            else:
                relative_error = abs(pred_num - gt_num) / abs(gt_num)
                similarity = max(0.0, 1.0 - relative_error)
                exact_match = relative_error < 0.01  # 1% tolerance
            
            return ComparisonResult(
                exact_match=exact_match,
                semantic_similarity=similarity,
                contains_gt=str(gt_num) in predicted,
                gt_in_prediction=str(gt_num) in predicted,
                confidence_score=similarity,
                comparison_type='numeric',
                details={
                    'predicted_numeric': pred_num,
                    'gt_numeric': gt_num,
                    'relative_error': relative_error if gt_num != 0 else None
                }
            )
            
        except (ValueError, AttributeError):
            # Fallback to semantic if numeric extraction fails
            return self._semantic_similarity(predicted, gt)
    
    def calculate_aggregate_metrics(self, 
                                  comparisons: List[ComparisonResult]) -> Dict[str, float]:
        """Calculate aggregate metrics across multiple comparisons."""
        if not comparisons:
            return {}
        
        total = len(comparisons)
        
        metrics = {
            'accuracy': sum(1 for c in comparisons if c.exact_match) / total,
            'avg_similarity': sum(c.semantic_similarity for c in comparisons) / total,
            'containment_rate': sum(1 for c in comparisons if c.contains_gt) / total,
            'avg_confidence': sum(c.confidence_score for c in comparisons) / total,
        }
        
        # Breakdown by comparison type
        type_counts = {}
        for comp in comparisons:
            comp_type = comp.comparison_type
            if comp_type not in type_counts:
                type_counts[comp_type] = {'total': 0, 'correct': 0}
            type_counts[comp_type]['total'] += 1
            if comp.exact_match:
                type_counts[comp_type]['correct'] += 1
        
        # Add per-type accuracy
        for comp_type, counts in type_counts.items():
            metrics[f'accuracy_{comp_type}'] = counts['correct'] / counts['total']
        
        return metrics
    
    def generate_comparison_report(self, 
                                 comparisons: List[ComparisonResult],
                                 include_details: bool = False) -> Dict[str, Any]:
        """Generate detailed comparison report."""
        metrics = self.calculate_aggregate_metrics(comparisons)
        
        report = {
            'summary': metrics,
            'total_comparisons': len(comparisons),
            'comparison_types': list(set(c.comparison_type for c in comparisons))
        }
        
        if include_details:
            report['individual_results'] = [
                {
                    'exact_match': c.exact_match,
                    'similarity': c.semantic_similarity,
                    'confidence': c.confidence_score,
                    'type': c.comparison_type,
                    'details': c.details
                }
                for c in comparisons
            ]
        
        return report
    
    def get_prompt_instructions(self) -> str:
        """Generate prompt instructions for LLM to use configured GT options."""
        options_str = ", ".join([f"'{opt}'" for opt in self.binary_options])
        
        return f"""
        Your answer should be one of these options: {options_str}.
        If you cannot determine the answer with confidence, choose the option that best represents uncertainty (e.g., 'unknown', 'unclear').
        Provide only the option, not additional explanation.
        """.strip()