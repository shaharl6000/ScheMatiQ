# schematiq-lib

Core algorithms for query-driven schema discovery and grounded structured data extraction.

## Installation

```bash
cd schematiq-lib && pip install -e .
```

Set an API key in your environment (e.g. `GEMINI_API_KEY`).

## Quickstart: Discovery and Value Extraction

```python
import json
from pathlib import Path

from schematiq import DiscoveryConfig, GeminiLLM, discover_schema
from schematiq.value_extraction.main import build_table_jsonl

docs_dir = Path("path/to/documents")
doc_files = sorted(docs_dir.glob("*.txt"))
documents = [p.read_text(encoding="utf-8") for p in doc_files]
filenames = [p.name for p in doc_files]

query = "What factors drive appeal outcomes across cases?"

# 1. Discover a schema from documents
discovery_llm = GeminiLLM(model="gemini-3.8-flash")
schema, evolution = discover_schema(
    documents=documents,
    filenames=filenames,
    query=query,
    config=DiscoveryConfig(batch_strategy="smart", convergence_threshold=5),
    llm=discovery_llm,
)

# Persist schema for extraction
schema_path = Path("schema.json")
schema_path.write_text(
    json.dumps(schema.to_full_dict(), indent=2, ensure_ascii=False),
    encoding="utf-8",
)

# 2. Extract a grounded table using the discovered schema
extraction_llm = GeminiLLM(model="gemini-3.5-flash-lite")
build_table_jsonl(
    schema_path=schema_path,
    docs_directories=[docs_dir],
    output_path=Path("table.jsonl"),
    llm=extraction_llm,
)
```

`discover_schema` also accepts optional callbacks (`on_progress`, `should_stop`, `on_snapshot`, `on_warning`) and an optional `retriever` (e.g. `EmbeddingRetriever`). Pass `documents=[]` to run query-only schema planning.

## Testing

```bash
pytest schematiq-lib/tests
```
