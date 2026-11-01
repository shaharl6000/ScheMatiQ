"""Document-first prompt layout and system-prompt folding (issue #547).

With document_first=True every value-extraction prompt for one document must
start with the same <PAPER_TEXT> block, so Gemini implicit caching can reuse
it. The default layout must stay unchanged.
"""

from schematiq.value_extraction.utils.prompt_builder import (
    PAPER_TEXT_CLOSE,
    PAPER_TEXT_OPEN,
    PromptBuilder,
    fold_system_after_document,
)

DOC = "Body of the document. " * 50


def _cols(*names):
    return [{"column": n, "definition": f"def {n}"} for n in names]


def test_default_layout_unchanged():
    content = PromptBuilder().build_val_messages("q", "Title", DOC, _cols("A"))[1]["content"]
    assert content.startswith("<QUESTION>")
    assert content.index("<QUESTION>") < content.index(PAPER_TEXT_OPEN)


def test_document_first_shares_prefix_across_calls():
    builder = PromptBuilder(document_first=True)
    a = builder.build_val_messages("q", "Title - Unit 1", DOC, _cols("A"))
    b = builder.build_val_messages(
        "q", "Title - Unit 2", DOC, _cols("B", "C"), mode="all",
        already_extracted={"A": "x"}, feedback="was wrong",
    )
    c = builder.build_val_messages("q", "Title", DOC, _cols("D"), mode="one_by_one", strict=True)
    prefix = f"{PAPER_TEXT_OPEN}\n{DOC}\n{PAPER_TEXT_CLOSE}"
    for msgs in (a, b, c):
        assert msgs[1]["content"].startswith(prefix)
    content_b = b[1]["content"]
    for tag in ("<PAPER_TITLE>", "<QUESTION>", "<REQUESTED_COLUMNS>",
                "<ALREADY_EXTRACTED_VALUES>", "<PRIOR_ATTEMPT_FEEDBACK>"):
        assert content_b.index(tag) > content_b.index(PAPER_TEXT_CLOSE)


def test_fold_moves_system_after_document():
    msgs = PromptBuilder(document_first=True).build_val_messages("q", "T", DOC, _cols("A"))
    msgs[0]["content"] = "UNIT SYSTEM PROMPT"
    folded = fold_system_after_document(msgs)
    assert [m["role"] for m in folded] == ["user"]
    content = folded[0]["content"]
    assert content.startswith(f"{PAPER_TEXT_OPEN}\n{DOC}\n{PAPER_TEXT_CLOSE}")
    assert content.index(PAPER_TEXT_CLOSE) < content.index("UNIT SYSTEM PROMPT") < content.index("<QUESTION>")


def test_fold_ignores_prompts_that_are_not_document_first():
    msgs = PromptBuilder().build_val_messages("q", "T", DOC, _cols("A"))
    assert fold_system_after_document(msgs) is None
    assert fold_system_after_document([{"role": "user", "content": "x"}]) is None


def _read_flags(env_extra):
    import json, os, subprocess, sys
    env = {**os.environ, **env_extra}
    code = (
        "import json; from schematiq.value_extraction.config import constants as c; "
        "print(json.dumps([c.PROMPT_LAYOUT, c.USE_EXPLICIT_CONTEXT_CACHE, c.DISABLE_LLM_RESPONSE_CACHE]))"
    )
    out = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout.strip().splitlines()[-1])


def test_layout_flag_defaults_to_document_first():
    import os
    env = {k: v for k, v in os.environ.items() if not k.startswith("SCHEMATIQ_")}
    os_environ_backup = dict(os.environ)
    try:
        os.environ.clear()
        os.environ.update(env)
        assert _read_flags({}) == ["document_first", False, False]
    finally:
        os.environ.clear()
        os.environ.update(os_environ_backup)


def test_legacy_layout_restores_explicit_cache_and_cache_flag():
    assert _read_flags({"SCHEMATIQ_PROMPT_LAYOUT": "legacy", "SCHEMATIQ_DISABLE_LLM_CACHE": "1"}) == [
        "legacy", True, True,
    ]
