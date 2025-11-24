"""Row-level query evaluation for schema assessment."""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

from QBSD.llm_backends import LLMInterface


@dataclass
class RowEvaluationResult:
    """Result of evaluating a single row against a query."""
    paper_name: str
    predicted_answer: str
    confidence: str
    reasoning: str
    columns_used: List[str]
    information_sufficiency: str
    raw_response: str


class RowQueryEvaluator:
    """Evaluates individual rows by asking LLM to answer query from extracted data."""
    
    def __init__(self, llm: LLMInterface, config=None):
        """
        Initialize RowQueryEvaluator.
        
        Args:
            llm: Language model interface for evaluation
            config: Configuration dict containing gt_options
        """
        self.llm = llm
        self.config = config or {}
        
        # Get GT options configuration
        gt_config = self.config.get('gt_options', {})
        self.binary_options = gt_config.get('binary_options', [])
        
    def evaluate_row(self, 
                    query: str,
                    row_data: Dict[str, Any],
                    few_shot_examples: str,
                    gt_column: str = "GT_NES") -> RowEvaluationResult:
        """
        Evaluate a single row by prompting LLM to answer query.
        
        Args:
            query: The research question to answer
            row_data: Dictionary of extracted data for this row
            few_shot_examples: Formatted few-shot examples
            gt_column: Ground truth column name (to exclude from evaluation)
            
        Returns:
            RowEvaluationResult with LLM response and metadata
        """
        # Format the row data for evaluation
        formatted_data = self._format_row_data(row_data, exclude_columns={gt_column})
        paper_name = self._extract_paper_name(row_data)
        
        # Build evaluation prompt
        prompt = self._build_evaluation_prompt(
            query=query,
            few_shot_examples=few_shot_examples,
            row_data=formatted_data
        )
        
        # Get LLM response
        try:
            raw_response = self.llm.generate(prompt)
            
            # Parse the response
            parsed = self._parse_llm_response(raw_response)
            
            # Analyze which columns were used
            columns_used = self._analyze_column_usage(raw_response, row_data, exclude_columns={gt_column})
            
            return RowEvaluationResult(
                paper_name=paper_name,
                predicted_answer=parsed['answer'],
                confidence=parsed['confidence'],
                reasoning=parsed['reasoning'],
                columns_used=columns_used,
                information_sufficiency=parsed['sufficiency'],
                raw_response=raw_response
            )
            
        except Exception as e:
            print(f"⚠️  Error evaluating row for {paper_name}: {e}")
            return RowEvaluationResult(
                paper_name=paper_name,
                predicted_answer="Error in evaluation",
                confidence="Low",
                reasoning=f"Evaluation failed: {e}",
                columns_used=[],
                information_sufficiency="Error",
                raw_response=""
            )
    
    def _format_row_data(self, 
                        row_data: Dict[str, Any], 
                        exclude_columns: set = None) -> str:
        """Format row data for LLM consumption."""
        exclude_columns = exclude_columns or set()
        exclude_columns.update({'paper_name', 'title', 'id', '_metadata', '_row_name', '_papers'})
        
        formatted_items = []
        
        for key, value in row_data.items():
            if key in exclude_columns or value is None:
                continue
                
            # Handle nested column dictionaries
            if isinstance(value, dict):
                if 'answer' in value:
                    # This is likely a value extraction result
                    answer = value['answer']
                    if answer and str(answer).strip():
                        formatted_items.append(f"{key}: {answer}")
                else:
                    # Other dictionary format
                    formatted_items.append(f"{key}: {value}")
            else:
                # Simple value
                if str(value).strip():
                    formatted_items.append(f"{key}: {value}")
        
        return "; ".join(formatted_items) if formatted_items else "No data available"
    
    def _extract_paper_name(self, row_data: Dict[str, Any]) -> str:
        """Extract paper identifier from row data."""
        return (row_data.get('paper_name') or 
                row_data.get('title') or 
                str(row_data.get('id', 'Unknown')))
    
    def _build_evaluation_prompt(self, 
                               query: str, 
                               few_shot_examples: str, 
                               row_data: str) -> str:
        """Build the evaluation prompt for the LLM."""
        
        # Build GT options instruction if available
        gt_instruction = ""
        if self.binary_options:
            options_str = ", ".join([f"'{opt}'" for opt in self.binary_options])
            gt_instruction = f"""
- Your answer must be one of these options: {options_str}
- If you cannot determine the answer with confidence, choose the option that best represents uncertainty"""
        
        prompt = f"""SYSTEM: You are evaluating extracted research data quality by answering research queries.

TASK: Given extracted data about a paper, answer the research question. Your answer should be comparable to the ground truth examples provided.

RESEARCH QUERY: {query}

EXAMPLES (Ground Truth):
{few_shot_examples}

EXTRACTED DATA FOR EVALUATION:
{row_data}

QUESTION: Based on the extracted data above, {query}

INSTRUCTIONS:
- Answer based ONLY on the provided extracted data{gt_instruction}
- If information is missing, unclear, or insufficient, state "Insufficient information"
- Provide confidence level: High/Medium/Low
- Be specific and precise in your answer
- Format your response as: Answer | Confidence | Reasoning

ANSWER:"""

        return prompt
    
    def _parse_llm_response(self, response: str) -> Dict[str, str]:
        """Parse LLM response into components."""
        try:
            # Look for the structured format: Answer | Confidence | Reasoning
            parts = response.split('|')
            
            if len(parts) >= 3:
                answer = parts[0].strip()
                confidence = parts[1].strip()
                reasoning = '|'.join(parts[2:]).strip()
            else:
                # Fallback: try to extract from unstructured response
                answer = response.strip()
                confidence = self._extract_confidence_fallback(response)
                reasoning = response.strip()
            
            # Determine information sufficiency
            sufficiency = self._assess_information_sufficiency(answer, reasoning)
            
            return {
                'answer': answer,
                'confidence': confidence,
                'reasoning': reasoning,
                'sufficiency': sufficiency
            }
            
        except Exception as e:
            return {
                'answer': response.strip()[:200],  # Truncate if too long
                'confidence': 'Low',
                'reasoning': f"Failed to parse response: {e}",
                'sufficiency': 'Unknown'
            }
    
    def _extract_confidence_fallback(self, response: str) -> str:
        """Extract confidence from unstructured response."""
        response_lower = response.lower()
        
        if any(word in response_lower for word in ['high confident', 'very confident', 'certain', 'definitely']):
            return 'High'
        elif any(word in response_lower for word in ['medium', 'somewhat', 'likely', 'probably']):
            return 'Medium'
        elif any(word in response_lower for word in ['low', 'uncertain', 'unclear', 'insufficient', 'not sure']):
            return 'Low'
        else:
            return 'Medium'  # Default
    
    def _assess_information_sufficiency(self, answer: str, reasoning: str) -> str:
        """Assess whether information was sufficient for answering."""
        combined = (answer + " " + reasoning).lower()
        
        if any(phrase in combined for phrase in ['insufficient information', 'not enough', 'missing', 'unclear']):
            return 'Insufficient'
        elif any(phrase in combined for phrase in ['partially', 'somewhat', 'limited']):
            return 'Partial'
        else:
            return 'Complete'
    
    def _analyze_column_usage(self, 
                            response: str, 
                            row_data: Dict[str, Any],
                            exclude_columns: set = None) -> List[str]:
        """Analyze which columns the LLM likely used in its response."""
        exclude_columns = exclude_columns or set()
        exclude_columns.update({'paper_name', 'title', 'id', '_metadata', '_row_name', '_papers'})
        
        used_columns = []
        response_lower = response.lower()
        
        for column_name, value in row_data.items():
            if column_name in exclude_columns:
                continue
                
            # Extract the actual answer text for matching
            if isinstance(value, dict) and 'answer' in value:
                answer_text = str(value['answer'])
            else:
                answer_text = str(value)
            
            if not answer_text or answer_text.strip() == '':
                continue
                
            # Check if column name or value appears in response
            column_name_lower = column_name.lower()
            answer_text_lower = answer_text.lower()
            
            # Look for column name references
            if column_name_lower in response_lower:
                used_columns.append(column_name)
                continue
                
            # Look for value references (for longer values, check partial matches)
            if len(answer_text_lower) > 10:
                # For longer text, check if significant portion is mentioned
                words = answer_text_lower.split()
                if len(words) >= 3:
                    # Check if multiple words from the answer appear in response
                    matches = sum(1 for word in words if len(word) > 3 and word in response_lower)
                    if matches >= min(3, len(words) // 2):
                        used_columns.append(column_name)
            else:
                # For shorter text, direct substring match
                if answer_text_lower in response_lower:
                    used_columns.append(column_name)
        
        return used_columns
    
    def evaluate_batch(self, 
                      query: str,
                      rows: List[Dict[str, Any]], 
                      few_shot_examples: str,
                      gt_column: str = "GT_NES") -> List[RowEvaluationResult]:
        """Evaluate multiple rows in batch."""
        results = []
        
        print(f"🔍 Evaluating {len(rows)} rows...")
        
        for i, row in enumerate(rows, 1):
            print(f"  Processing row {i}/{len(rows)}")
            result = self.evaluate_row(query, row, few_shot_examples, gt_column)
            results.append(result)
        
        print(f"✅ Completed evaluation of {len(results)} rows")
        return results