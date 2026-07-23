"""Prompt building utilities for LLM interactions."""
from __future__ import annotations

from typing import List, Dict
from schematiq.core.schema import Column

from ..config.prompts import SYSTEM_PROMPT_VAL, SYSTEM_PROMPT_VAL_STRICT


class PromptBuilder:
    """Builds prompts for value extraction LLM calls."""

    def __init__(self, reference_context: str | None = None):
        # Optional external reference text injected into every value-extraction
        # prompt as supplementary lookup material (never a source document).
        self.reference_context = reference_context

    def build_val_messages(
        self,
        query: str,
        paper_title: str,
        paper_text: str,
        columns: List[Column],
        mode: str = "all",
        *,
        strict: bool = False,
        already_extracted: Dict[str, str] | None = None,
    ) -> List[Dict[str, str]]:
        """
        Build messages for value extraction LLM calls.

        mode:
          - "all"         – ask for all columns at once
          - "one"         – (deprecated) alias of "one_by_one"
          - "one_by_one"  – single-column prompt, called per column by the caller

        already_extracted:
          Optional dict mapping column names to their already-extracted answers.
          When provided, injected as context so the LLM knows what was already
          found (e.g., Justice1-3 filled → don't hallucinate Justice4-9).
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

        extracted_block = ""
        if already_extracted:
            lines = [f"- {name}: {value}" for name, value in already_extracted.items()]
            extracted_block = f"""
            <ALREADY_EXTRACTED_VALUES>
            The following columns have already been extracted for this document.
            Use them as context — for example, if numbered columns (Judge1, Judge2, …)
            are already filled and a count column indicates the total, do NOT fill
            higher-numbered slots beyond that count.
            {chr(10).join(lines)}
            </ALREADY_EXTRACTED_VALUES>
            """.strip()

        reference_block = ""
        if self.reference_context:
            reference_block = (
                "<EXTERNAL_REFERENCE>\n"
                "The text below is an EXTERNAL REFERENCE supplied separately by the "
                "user. It is NOT the source document and does NOT define observation "
                "units - never create a row from it. Use it only as supplementary "
                "lookup information (for example, to map an entity named in the source "
                "document to an attribute recorded here) when it is relevant to a "
                "requested column; otherwise ignore it.\n"
                f"{self.reference_context}\n"
                "</EXTERNAL_REFERENCE>"
            )

        user_prompt = f"""
            <QUESTION>
            {query}
            </QUESTION>

            {col_block}

            {extracted_block}

            <PAPER_TITLE>
            {paper_title}
            </PAPER_TITLE>

            <PAPER_TEXT>
            {paper_text}
            </PAPER_TEXT>

            {reference_block}
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
