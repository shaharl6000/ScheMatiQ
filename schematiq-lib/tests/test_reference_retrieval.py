"""Tests for domain-agnostic reference retrieval (chunking + BM25 + query)."""

from schematiq.value_extraction.utils.reference_retrieval import (
    ReferenceRetriever,
    build_reference_query,
    chunk_reference,
    _looks_tabular,
)


def _big_tabular():
    header = "nid,jid,Last Name,First Name,Birth Year,appointing_president"
    rows = [f"{1300000+i},{i},Surname{i},First{i},19{i%100:02d},President{i%5}" for i in range(2000)]
    rows.insert(1000, "13761857,13761857,Acker,William,1927,Ronald Reagan")
    return header + "\n" + "\n".join(rows)


def test_tabular_detection_and_header_replication():
    text = _big_tabular()
    assert _looks_tabular(text.splitlines())
    chunks = chunk_reference(text, max_words=60)
    assert len(chunks) > 1
    assert all(c.text.startswith("nid,jid,Last Name") for c in chunks)


def test_keyed_lookup_retrieves_target_row():
    retr = ReferenceRetriever(_big_tabular(), max_words=60)
    query = build_reference_query("William Acker", ["appointing_president"])
    res = retr.retrieve(query, k=3)
    assert res and "Acker" in res[0] and "Ronald Reagan" in res[0]


def test_absent_token_returns_no_chunks():
    retr = ReferenceRetriever(_big_tabular(), max_words=60)
    assert retr.retrieve("zzzqqxx", k=3) == []


def test_prose_retrieval():
    prose = (
        "The handbook describes courtroom procedures in detail.\n\n"
        "Judge William Marsh Acker Jr. was appointed by President Ronald Reagan in 1982 "
        "and served in Birmingham.\n\n"
        "Filing deadlines for civil motions are in the local rules."
    )
    assert not _looks_tabular(prose.splitlines())
    retr = ReferenceRetriever(prose, max_words=40)
    res = retr.retrieve(build_reference_query("William Marsh Acker", ["appointing president"]), k=1)
    assert res and "Reagan" in res[0]


def test_build_reference_query_includes_unit_and_columns():
    class Col:
        name = "President"
        definition = "Appointing president"

    q = build_reference_query("Unit Y", [Col()])
    assert "Unit Y" in q and "President" in q and "Appointing president" in q
    # string columns are supported too
    q2 = build_reference_query("Unit X", ["colA"])
    assert "Unit X" in q2 and "colA" in q2


def test_chunking_edge_cases():
    assert chunk_reference("") == []
    assert chunk_reference("   \n  ") == []
    assert ReferenceRetriever("").retrieve("anything") == []


def test_relative_cutoff_drops_weak_chunks():
    """When only one region truly matches, weak chunks matching only ubiquitous
    column terms are not dragged in just to fill k."""
    header = "judge,appointing_president"
    rows = [f"Judge{i},President{i % 5}" for i in range(5000)]
    rows.insert(2500, "William Acker,Ronald Reagan")
    big = header + "\n" + "\n".join(rows)
    retr = ReferenceRetriever(big)
    res = retr.retrieve(build_reference_query("William Acker", ["appointing_president"]), k=5)
    joined = "\n".join(res)
    assert "Ronald Reagan" in joined
    assert "Judge0,President0" not in joined


def test_prompt_builder_small_reference_injected_whole():
    from schematiq.value_extraction.utils.prompt_builder import PromptBuilder

    cols = [{"column": "appointing_president", "definition": "President who appointed"}]
    pb = PromptBuilder(reference_context="judge,appointing_president\nAcker,Reagan")
    content = pb.build_val_messages("q", "Acker ruling", "body", cols, mode="all")[1]["content"]
    assert "<EXTERNAL_REFERENCE>" in content and "Acker,Reagan" in content


def test_prompt_builder_large_reference_injects_only_retrieved():
    from schematiq.value_extraction.utils.prompt_builder import PromptBuilder

    cols = [{"column": "appointing_president", "definition": "President who appointed"}]
    header = "judge,appointing_president"
    rows = [f"Judge{i},President{i % 5}" for i in range(5000)]
    rows.insert(2500, "William Acker,Ronald Reagan")
    retr = ReferenceRetriever(header + "\n" + "\n".join(rows))
    pb = PromptBuilder(reference_retriever=retr, reference_k=3)
    content = pb.build_val_messages("q", "William Acker", "body", cols, mode="all")[1]["content"]
    assert "Ronald Reagan" in content
    assert "Judge0,President0" not in content
    assert len(content) < 5000  # retrieval actually bounded the prompt


def test_prompt_builder_explicit_reference_query_is_precise():
    """The explicit reference_query (the identified unit) pins the right passage
    regardless of the document title."""
    from schematiq.value_extraction.utils.prompt_builder import PromptBuilder

    cols = [{"column": "appointing_president", "definition": "President who appointed"}]
    header = "judge,appointing_president"
    rows = [f"Judge{i},President{i % 5}" for i in range(5000)]
    rows.insert(2500, "William Acker,Ronald Reagan")
    retr = ReferenceRetriever(header + "\n" + "\n".join(rows))
    pb = PromptBuilder(reference_retriever=retr, reference_k=3)
    content = pb.build_val_messages(
        "q", "unrelated_filename", "body", cols, mode="all", reference_query="William Acker"
    )[1]["content"]
    assert "Ronald Reagan" in content
    assert "Judge0,President0" not in content


def test_paper_processor_gates_on_reference_size():
    from schematiq.value_extraction.core.paper_processor import (
        PaperProcessor,
        REFERENCE_FULL_INJECT_MAX_CHARS,
    )

    small = "judge,pres\nAcker,Reagan"
    big = "judge,pres\n" + "\n".join(f"Judge{i},P{i}" for i in range(5000))
    assert len(big) > REFERENCE_FULL_INJECT_MAX_CHARS

    pp_big = PaperProcessor(llm=None, reference_context=big)
    assert pp_big.prompt_builder.reference_retriever is not None
    assert pp_big.prompt_builder.reference_context is None

    pp_small = PaperProcessor(llm=None, reference_context=small)
    assert pp_small.prompt_builder.reference_retriever is None
    assert pp_small.prompt_builder.reference_context == small

    pp_none = PaperProcessor(llm=None)
    assert pp_none.prompt_builder.reference_retriever is None
    assert pp_none.prompt_builder.reference_context is None


def test_oversized_single_record_is_split_not_one_giant_chunk():
    """A genuinely prose reference with no blank lines collapses to one record.
    It must still be split into bounded chunks rather than one chunk = the whole
    file. Regression: without this the whole reference was sent to the model,
    exceeding the token limit."""
    from schematiq.value_extraction.utils.reference_retrieval import (
        chunk_reference, _looks_tabular,
    )

    text = " ".join(f"word{i}" for i in range(3000))  # one long line, no blank lines
    assert not _looks_tabular(text.splitlines())  # genuinely prose, exercises _split_record
    chunks = chunk_reference(text, max_words=120)
    assert len(chunks) > 1
    assert max(len(c.text.split()) for c in chunks) <= 120


def test_quoted_delimited_file_detected_tabular_and_header_replicated():
    """A CSV whose fields contain quoted delimiters (e.g. "Doe, Jane") must be
    detected as tabular via csv parsing, not misread as prose by naive delimiter
    counting. Detection restores header replication so every retrieved chunk --
    including deep rows -- carries the column names the model needs. Regression
    for reference cells coming back N/A."""
    from schematiq.value_extraction.utils.reference_retrieval import (
        chunk_reference, _looks_tabular, ReferenceRetriever, build_reference_query,
    )

    header = "judge_name,appointing_president,court,notes"
    rows = []
    for i in range(1000):
        # Quoted embedded commas: naive counting sees an inconsistent column
        # count and (before the fix) falls back to the prose path.
        if i % 2 == 0:
            rows.append(f'"Judge, Number {i}",Ronald Reagan,"District, West",n{i}')
        else:
            rows.append(f"Judge {i},Jimmy Carter,Ninth Circuit,n{i}")
    text = header + "\n" + "\n".join(rows)

    assert _looks_tabular(text.splitlines())  # quoted commas no longer fool detection
    chunks = chunk_reference(text, max_words=120)
    assert len(chunks) > 1
    assert all(c.text.startswith(header) for c in chunks)  # header on every chunk

    retriever = ReferenceRetriever(text)
    # A deep row (not near the top of the file) still retrieves a small slice
    # that carries the header and the row's appointing value.
    query = build_reference_query(
        "Judge, Number 900",
        [{"column": "appointing_president", "definition": "who appointed"}],
    )
    joined = "\n\n".join(retriever.retrieve(query, k=5))
    assert "appointing_president" in joined  # header travelled with the row
    assert "Judge, Number 900" in joined and "Reagan" in joined
    assert len(joined) < len(text) / 10  # a small slice, not the whole file
