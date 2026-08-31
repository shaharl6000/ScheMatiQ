"""Prompt building utilities for LLM interactions."""
from __future__ import annotations

from typing import List, Dict
from schematiq.core.schema import Column

from ..config.prompts import SYSTEM_PROMPT_VAL, SYSTEM_PROMPT_VAL_STRICT
from .reference_retrieval import ReferenceRetriever, build_reference_query


class PromptBuilder:
    """Builds prompts for value extraction LLM calls."""

    def __init__(
        self,
        reference_context: str | None = None,
        reference_retriever: "ReferenceRetriever | None" = None,
        reference_k: int = 5,
    ):
        # Optional external reference injected into every value-extraction prompt
        # as supplementary lookup material (never a source document). When the
        # reference is small it is injected whole (reference_context). When it is
        # large a retriever is supplied instead, and only the passages relevant to
        # the current observation unit + columns are injected.
        self.reference_context = reference_context
        self.reference_retriever = reference_retriever
        self.reference_k = reference_k

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
        reference_query: str | None = None,
        feedback: str | None = None,
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

        feedback:
          Optional short factual note on what was wrong with a prior answer
          for this exact call (e.g. the previous value), supplied by the
          workspace "Wrong, try again" menu item. Wrapped with standard
          re-examination instructions (mirroring SYSTEM_PROMPT_VAL_REEXTRACT's
          search-strategy guidance) and prepended ahead of the rest of the
          prompt. ``None`` (the default, used by every other caller) leaves
          the prompt unchanged.
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
        ref_text = self.reference_context
        if self.reference_retriever is not None:
            # Prefer the explicit observation-unit descriptor (e.g. the unit name)
            # as the retrieval key; fall back to the document title otherwise.
            rq = build_reference_query(reference_query or paper_title, columns)
            passages = self.reference_retriever.retrieve(rq, k=self.reference_k)
            ref_text = "\n\n--- REFERENCE PASSAGE ---\n\n".join(passages) if passages else None
        if ref_text:
            reference_block = (
                "<EXTERNAL_REFERENCE>\n"
                "The text below is an EXTERNAL REFERENCE supplied separately by the "
                "user. It is NOT the source document and does NOT define observation "
                "units - never create a row from it. Use it only as supplementary "
                "lookup information (for example, to map an entity named in the source "
                "document to an attribute recorded here) when it is relevant to a "
                "requested column; otherwise ignore it.\n"
                f"{ref_text}\n"
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

        if feedback:
            # Mirrors SYSTEM_PROMPT_VAL_REEXTRACT's search-strategy guidance
            # (tables/figures/footnotes, synonyms, differently-phrased
            # mentions) for the "wrong value" case instead of "missing value".
            # Explicitly allows keeping the same answer or returning null
            # rather than demanding a different one -- without that, a model
            # that can't find better support might invent a new value just to
            # comply, which is worse than repeating the original mistake.
            feedback_block = f"""
            <PRIOR_ATTEMPT_FEEDBACK>
            A user reviewed this value and marked it WRONG. {feedback}
            Re-examine the passages carefully -- including any tables, figures,
            captions, and footnotes -- and consider synonyms or
            differently-phrased mentions of this value.
            Provide a different answer only if you find genuine support for it
            in the text. If you cannot find stronger support, it is fine to
            return the same answer, or null if the value truly is not present
            -- do not invent a new value just to be different.
            </PRIOR_ATTEMPT_FEEDBACK>
            """.strip()
            user_prompt = f"{feedback_block}\n\n{user_prompt}"

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
