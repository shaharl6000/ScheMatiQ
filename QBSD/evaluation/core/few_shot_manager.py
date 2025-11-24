"""Few-shot example management for schema evaluation."""

import json
import random
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd


class FewShotManager:
    """Manages few-shot examples from ground truth data for evaluation."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize FewShotManager.
        
        Args:
            config: Configuration dict with:
                - n_shots_per_category: Number of examples per GT category
                - selection_strategy: How to select examples
                - specific_rows: Optional specific rows to use
        """
        self.n_shots_per_category = config.get("n_shots_per_category", 3)
        self.selection_strategy = config.get("selection_strategy", "stratified")
        self.specific_rows = config.get("specific_rows", [])
        
    def extract_gt_examples(self, 
                           data: List[Dict[str, Any]], 
                           gt_column: str) -> List[Dict[str, Any]]:
        """
        Extract few-shot examples from ground truth data.
        
        Args:
            data: List of data rows
            gt_column: Name of the ground truth column
            
        Returns:
            List of selected examples
        """
        if not data:
            return []
            
        # Filter rows that have GT values
        gt_data = [row for row in data if row.get(gt_column) is not None]
        
        if not gt_data:
            print(f"⚠️  No ground truth data found in column '{gt_column}'")
            return []
            
        # Use specific rows if provided
        if self.specific_rows:
            return self._select_specific_rows(gt_data, self.specific_rows)
            
        # Apply selection strategy
        if self.selection_strategy == "stratified":
            return self._stratified_selection(gt_data, gt_column)
        elif self.selection_strategy == "diverse":
            return self._diverse_selection(gt_data, gt_column)
        elif self.selection_strategy == "representative":
            return self._representative_selection(gt_data, gt_column)
        else:
            raise ValueError(f"Unknown selection strategy: {self.selection_strategy}")
    
    def _stratified_selection(self, 
                            data: List[Dict[str, Any]], 
                            gt_column: str) -> List[Dict[str, Any]]:
        """Select N examples per unique GT value."""
        # Group by GT values
        gt_groups = {}
        for row in data:
            gt_value = row[gt_column]
            if gt_value not in gt_groups:
                gt_groups[gt_value] = []
            gt_groups[gt_value].append(row)
        
        # Sample from each group
        examples = []
        for gt_value, group in gt_groups.items():
            n_samples = min(self.n_shots_per_category, len(group))
            selected = random.sample(group, n_samples)
            examples.extend(selected)
            
        print(f"📋 Selected {len(examples)} stratified examples from {len(gt_groups)} GT categories")
        return examples
    
    def _diverse_selection(self, 
                          data: List[Dict[str, Any]], 
                          gt_column: str) -> List[Dict[str, Any]]:
        """Select examples to maximize diversity across all columns."""
        # For now, use stratified as base then add diversity logic
        stratified = self._stratified_selection(data, gt_column)
        
        # Could add logic here to ensure diversity in other columns too
        # For simplicity, return stratified for now
        return stratified
    
    def _representative_selection(self, 
                                data: List[Dict[str, Any]], 
                                gt_column: str) -> List[Dict[str, Any]]:
        """Select most common/representative examples."""
        # Count GT value frequencies
        gt_counts = {}
        for row in data:
            gt_value = row[gt_column]
            gt_counts[gt_value] = gt_counts.get(gt_value, 0) + 1
        
        # Sort by frequency and take top examples
        sorted_gts = sorted(gt_counts.items(), key=lambda x: x[1], reverse=True)
        
        examples = []
        for gt_value, count in sorted_gts:
            if len(examples) >= self.n_shots_per_category * 3:  # Cap total examples
                break
                
            # Get examples for this GT value
            gt_examples = [row for row in data if row[gt_column] == gt_value]
            selected = random.sample(gt_examples, min(self.n_shots_per_category, len(gt_examples)))
            examples.extend(selected)
            
        print(f"📋 Selected {len(examples)} representative examples")
        return examples
    
    def _select_specific_rows(self, 
                            data: List[Dict[str, Any]], 
                            specific_rows: List[str]) -> List[Dict[str, Any]]:
        """Select specific rows by identifier (e.g., paper name)."""
        examples = []
        for row in data:
            # Try different possible identifier fields
            row_id = (row.get('paper_name') or 
                     row.get('title') or 
                     row.get('name') or 
                     str(row.get('id', '')))
                     
            if row_id in specific_rows:
                examples.append(row)
                
        print(f"📋 Selected {len(examples)} specific examples")
        return examples
    
    def format_examples_for_prompt(self, 
                                  examples: List[Dict[str, Any]], 
                                  query: str,
                                  gt_column: str,
                                  format_strategy: str = "structured") -> str:
        """
        Format examples for inclusion in LLM prompt.
        
        Args:
            examples: List of example rows
            query: The evaluation query
            gt_column: Ground truth column name
            format_strategy: How to format examples
            
        Returns:
            Formatted string for prompt inclusion
        """
        if not examples:
            return "No examples available."
            
        if format_strategy == "structured":
            return self._format_structured_examples(examples, gt_column)
        elif format_strategy == "narrative":
            return self._format_narrative_examples(examples, gt_column, query)
        else:
            return self._format_structured_examples(examples, gt_column)
    
    def _format_structured_examples(self, 
                                   examples: List[Dict[str, Any]], 
                                   gt_column: str) -> str:
        """Format examples in structured format."""
        formatted = []
        
        for i, example in enumerate(examples, 1):
            # Get paper identifier
            paper_name = (example.get('paper_name') or 
                         example.get('title') or 
                         f"Paper_{i}")
            
            # Format extracted data (exclude GT and metadata)
            exclude_keys = {gt_column, 'paper_name', 'title', 'id', '_metadata'}
            data_items = []
            for key, value in example.items():
                if key not in exclude_keys and value is not None:
                    # Handle nested values (like column dictionaries)
                    if isinstance(value, dict):
                        if 'answer' in value:
                            data_items.append(f"{key}: {value['answer']}")
                        else:
                            data_items.append(f"{key}: {value}")
                    else:
                        data_items.append(f"{key}: {value}")
            
            extracted_data = "; ".join(data_items)
            gt_answer = example.get(gt_column, "Unknown")
            
            formatted.append(f"""Example {i}:
Paper: {paper_name}
Extracted Data: {extracted_data}
Ground Truth Answer: {gt_answer}""")
        
        return "\n\n".join(formatted)
    
    def _format_narrative_examples(self, 
                                  examples: List[Dict[str, Any]], 
                                  gt_column: str,
                                  query: str) -> str:
        """Format examples in narrative format."""
        formatted = []
        
        for i, example in enumerate(examples, 1):
            paper_name = (example.get('paper_name') or 
                         example.get('title') or 
                         f"Paper_{i}")
            gt_answer = example.get(gt_column, "Unknown")
            
            formatted.append(f"For {paper_name}, the answer to \"{query}\" is: {gt_answer}")
        
        return "\n".join(formatted)
    
    def load_data_from_jsonl(self, file_path: Path) -> List[Dict[str, Any]]:
        """Load data from JSONL file."""
        data = []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        data.append(json.loads(line))
            print(f"📁 Loaded {len(data)} rows from {file_path}")
            return data
        except Exception as e:
            print(f"❌ Error loading data from {file_path}: {e}")
            return []