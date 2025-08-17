"""Prompt building utilities for LLM interactions."""

from typing import List, Dict
from schema import Column

from ..config.prompts import SYSTEM_PROMPT_VAL, SYSTEM_PROMPT_VAL_STRICT


class PromptBuilder:
    """Builds prompts for value extraction LLM calls."""
    
    def __init__(self):
        pass
    
    def build_val_messages(self, query: str,
                          paper_title: str,
                          paper_text: str,
                          columns: List[Column],
                          mode: str = "all",
                          *,
                          strict: bool = False) -> List[Dict[str, str]]:
        """
        Build messages for value extraction LLM calls.
        
        mode:
          - "all"         – ask for all columns at once
          - "one"         – (deprecated) alias of "one_by_one"
          - "one_by_one"  – single-column prompt, called per column by the caller
        """
        if mode in {"one", "one_by_one"}:
            col = columns[0]
            col_block = f"""
            <REQUESTED_COLUMN>
            name: {col['column']}
            definition: {col['definition']}
            </REQUESTED_COLUMN>
            """.strip()
        else:
            col_specs = "\n".join(
                f"- **{c['column']}**: {c['definition']}" for c in columns
            )
            col_block = f"""
            <REQUESTED_COLUMNS>
            {col_specs}
            </REQUESTED_COLUMNS>
            """.strip()

        user_prompt = f"""
            <QUESTION>
            {query}
            </QUESTION>

            {col_block}

            <PAPER_TITLE>
            {paper_title}
            </PAPER_TITLE>

            <PAPER_TEXT>
            {paper_text}
            </PAPER_TEXT>
            """.strip()

        system = SYSTEM_PROMPT_VAL_STRICT if strict else SYSTEM_PROMPT_VAL
        return [
            {"role": "system", "content": system},
            {"role": "user",   "content": user_prompt},
        ]