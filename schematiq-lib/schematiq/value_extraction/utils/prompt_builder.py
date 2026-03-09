"""Prompt building utilities for LLM interactions."""

from typing import List, Dict
from schematiq.core.schema import Column

from ..config.prompts import SYSTEM_PROMPT_VAL, SYSTEM_PROMPT_VAL_STRICT


class PromptBuilder:
    """Builds prompts for value extraction LLM calls."""

    def __init__(self):
        pass

    def build_val_messages(
        self,
        query: str,
        paper_title: str,
        paper_text: str,
        columns: List[Column],
        mode: str = "all",
        *,
        strict: bool = False,
    ) -> List[Dict[str, str]]:
        """
        Build messages for value extraction LLM calls.

        mode:
          - "all"         – ask for all columns at once
          - "one"         – (deprecated) alias of "one_by_one"
          - "one_by_one"  – single-column prompt, called per column by the caller
        """
        if mode in {"one", "one_by_one"}:
            col = columns[0]
            # Build allowed_values line if present
            allowed_values_line = ""
            if col.get("allowed_values"):
                allowed_values_line = f"\nallowed_values: {col['allowed_values']}"
            col_block = f"""
            <REQUESTED_COLUMN>
            name: {col['column']}
            definition: {col['definition']}{allowed_values_line}
            </REQUESTED_COLUMN>
            """.strip()
        else:
            col_specs = []
            for c in columns:
                spec = f"- **{c['column']}**: {c['definition']}"
                if c.get("allowed_values"):
                    spec += f" (allowed values: {', '.join(c['allowed_values'])})"
                col_specs.append(spec)
            col_block = f"""
            <REQUESTED_COLUMNS>
            {chr(10).join(col_specs)}
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

        # Table-aware prompting: if the document contains Markdown tables,
        # add a hint so the LLM parses them correctly.
        if "|---" in paper_text or "| ---" in paper_text:
            system += (
                "\n\nNote: Tables in the document are formatted as Markdown "
                "tables with | delimiters. Pay close attention to table "
                "headers and cell values when extracting data."
            )

        return [
            {"role": "system", "content": system},
            {"role": "user", "content": user_prompt},
        ]
