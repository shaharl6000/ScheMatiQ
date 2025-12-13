"""Row data management for value extraction."""

from pathlib import Path
from typing import Dict, Any, List, Set


class RowDataManager:
    """Manages row data operations: grouping, merging, validation."""
    
    def __init__(self):
        pass
    
    def extract_row_name_from_filename(self, filename: str) -> str:
        """
        Extract row name from filename by taking the part before the first underscore.
        
        Examples:
            abc-gamma_348734_full.txt -> abc-gamma
            4E-T_32520643_full.txt -> 4E-T
            simple_file.txt -> simple
        """
        # Remove file extension and get the base name
        base_name = Path(filename).stem
        # Split by underscore and take the first part
        row_name = base_name.split('_')[0]
        return row_name
    
    def group_papers_by_row(self, docs: List[Path]) -> Dict[str, List[Path]]:
        """Group papers by row name extracted from filename."""
        papers_by_row: Dict[str, List[Path]] = {}
        for doc_path in docs:
            row_name = self.extract_row_name_from_filename(doc_path.name)
            if row_name not in papers_by_row:
                papers_by_row[row_name] = []
            papers_by_row[row_name].append(doc_path)
        return papers_by_row
    
    def validate_row_completion(self, row_data: Dict[str, Any], expected_papers: Set[str]) -> bool:
        """
        Check if a row is complete by verifying all expected papers have been processed.
        
        Args:
            row_data: The row data from JSONL
            expected_papers: Set of paper titles that should be in this row
        
        Returns:
            True if the row is complete and doesn't need further processing
        """
        if not row_data or not isinstance(row_data, dict):
            return False
        
        # Get papers that contributed to this row
        row_papers = set(row_data.get("_papers", []))
        
        # Row is complete if it contains all expected papers
        return expected_papers.issubset(row_papers)
    
    def merge_row_data(self, existing_row: Dict[str, Any], new_row: Dict[str, Any], 
                      new_paper_title: str) -> Dict[str, Any]:
        """
        Intelligently merge data from two rows with the same row name.
        
        Args:
            existing_row: The existing row data from the JSONL file
            new_row: New extracted data for the same row name
            new_paper_title: Title of the new paper being processed
        
        Returns:
            Merged row data combining both sources
        """
        merged = existing_row.copy()
        
        # Track source papers - update the _papers list
        existing_papers = merged.get('_papers', [])
        if new_paper_title not in existing_papers:
            existing_papers.append(new_paper_title)
        merged['_papers'] = existing_papers
        
        # For each column in the new data
        for col_name, new_col_data in new_row.items():
            if col_name.startswith('_'):  # Skip metadata fields (they're preserved from existing_row)
                continue
                
            if not isinstance(new_col_data, dict):
                continue
                
            new_answer = new_col_data.get('answer', '').strip()
            new_excerpts = new_col_data.get('excerpts', [])
            
            # If no existing data for this column, use new data
            if col_name not in merged:
                if new_answer:  # Only add if there's actually an answer
                    merged[col_name] = new_col_data.copy()
                continue
                
            existing_col_data = merged[col_name]
            if not isinstance(existing_col_data, dict):
                continue
                
            existing_answer = existing_col_data.get('answer', '').strip()
            existing_excerpts = existing_col_data.get('excerpts', [])
            
            # Merge logic: prefer more complete/detailed answer
            merged_answer = existing_answer
            if new_answer and (not existing_answer or len(new_answer) > len(existing_answer)):
                merged_answer = new_answer
            elif new_answer and existing_answer and new_answer != existing_answer:
                # If both have answers but different, combine them
                merged_answer = f"{existing_answer}; {new_answer}"
            
            # Merge excerpts, removing duplicates while preserving order
            # Handle both old format (plain strings) and new format (objects with text/source)
            all_excerpts = existing_excerpts + new_excerpts
            unique_excerpts = []
            seen = set()
            for excerpt in all_excerpts:
                # Extract text content for comparison - handle both formats
                if isinstance(excerpt, dict):
                    excerpt_text = excerpt.get('text', '').strip()
                else:
                    excerpt_text = str(excerpt).strip()

                excerpt_clean = excerpt_text.lower()
                if excerpt_clean and excerpt_clean not in seen:
                    unique_excerpts.append(excerpt)
                    seen.add(excerpt_clean)
            
            merged[col_name] = {
                'answer': merged_answer,
                'excerpts': unique_excerpts
            }
        
        return merged