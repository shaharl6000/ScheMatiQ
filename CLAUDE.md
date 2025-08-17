# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

QueryDiscovery is a research project implementing Query-Based Schema Discovery (QBSD), a modern approach to information extraction. The system takes a user query and a collection of documents, then iteratively discovers a table schema (column headers + rationales) that best captures information needed to answer the query.

## Architecture

The codebase is organized into two main directories:

### QBSD/ - Core Framework
- `QBSD.py` - Main pipeline with core functions: select_relevant_content, generate_schema, merge_schemas, evaluate_schema_convergence  
- `schema.py` - Schema data structures (Column, Schema classes) with embedding-based similarity and merging logic
- `llm_backends.py` - LLM interface implementations (Together AI, OpenAI) with abstract base class
- `retrievers.py` - Content retrieval implementations (embedding-based retrieval)
- `utils.py` - Utility functions for text processing and file operations
- `valueExtractor.py` - Clean interface to the refactored value extraction system
- `value_extraction/` - **Modular value extraction framework**:
  - `core/` - Business logic classes (TableBuilder, PaperProcessor, JSONResponseParser, LLMCache, RowDataManager)
  - `utils/` - Utility functions (TextProcessor, PromptBuilder)
  - `config/` - Configuration constants and system prompts
- `configurations/` - JSON config files for different experimental setups

### src/ - Data Processing & Experiments  
- `run.py` - Alternative experimental runner (legacy, contains hardcoded API token)
- `create_data.py` - Data preprocessing scripts
- `generate_tables.py` - Table generation from schemas
- `download_nes_papers.py` - Data collection scripts for NES (Nuclear Export Signal) research
- `abstracts/` and `full_text/` - Research paper data directories
- `manually/` - Manual annotation data

### arxivDIGESTables/ - Evaluation Framework
- `metric/run_eval.py` - **Main evaluation script** for comparing QBSD output against gold standard tables
- `metric/metrics.py` - Core evaluation metrics (SchemaRecallMetric, BaseMetric)
- `metric/metrics_utils.py` - Featurizers and scorers for different evaluation approaches
- `metric/table.py` - Table data structure for evaluation
- `predictions/` - Evaluation results and scoring outputs
- Based on ArxivDIGESTables dataset for table synthesis evaluation

## Development Commands

### Running QBSD
```bash
cd QBSD
python QBSD.py --config configurations/config_qbsd.json
```

### Python Environment
The project uses Python 3.11+ with dependencies listed in `requirements.txt`:
```bash
pip install -r requirements.txt
```

Key dependencies: transformers, torch, sentence-transformers, datasets, pandas, requests

### Running Value Extraction
```bash
cd QBSD
python valueExtractor.py  # Uses default config
# OR
python -c "import valueExtractor; valueExtractor.main(Path('configurations/valueExtractionConfig.json'))"
```

### Configuration
QBSD experiments are configured via JSON files in `QBSD/configurations/`. Key config parameters:
- `query` - The research question to explore
- `docs_path` - Path to document collection  
- `backend` - LLM provider and model settings
- `retriever` - Embedding model configuration with dynamic_k, batch_size, device settings
- `max_keys_schema` - Schema size limit

**Value Extraction Config** (`valueExtractionConfig.json`):
- `schema_path` - Path to discovered schema JSON file
- `docs_directory` - Directory containing papers to extract from
- `output_path` - Output JSONL file for extracted table data
- `mode` - Extraction mode: "all" (batch) or "one_by_one" (per-column)
- `resume` - Boolean to resume from existing output file
- `retrieval_k` - Number of passages to retrieve per query
- `max_workers` - Parallel processing workers (0 = sequential)

### Evaluation with arxivDIGESTables
The `arxivDIGESTables/metric/run_eval.py` script evaluates QBSD-generated table schemas against gold standard tables:

```bash
cd arxivDIGESTables/metric
python run_eval.py
```

**Key Evaluation Components:**
- **Featurizers** - Different ways to represent column schemas:
  - `name`: Uses raw column names
  - `values`: Concatenates column names with their values  
  - `decontext`: Uses LLM (Mixtral) to decontextualize column names
- **Scorers** - Similarity measurement methods:
  - `exact_match`: Exact string matching
  - `jaccard`: Jaccard similarity with stopword removal
  - `sentence_transformers`: Semantic similarity using embeddings
  - `llama3`: LLM-based alignment scoring
- **Metrics**: SchemaRecallMetric computes recall scores with alignment thresholds (0.5, 0.7, 0.9)

**Input/Output:**
- Input: QBSD-generated schemas (from `data/schema_6_filtered/results_gpt4o_clean_QBSD.jsonl`)
- Gold Standard: ArxivDIGESTables dataset tables
- Output: Detailed evaluation results with recall scores and column alignments in `predictions/SCORE_*.jsonl`

## Key Concepts

**Schema Discovery Pipeline:**
1. Content retrieval using embedding-based similarity
2. LLM-based schema generation with iterative refinement
3. Schema merging using semantic similarity
4. Convergence evaluation to determine completion

**Value Extraction Pipeline:**
1. **Paper Grouping**: Group papers by row name (extracted from filename prefix)
2. **Resume Logic**: Load existing data and identify completed/incomplete rows
3. **Paper Processing**: Extract values from papers using schema columns
   - Retrieval-based text selection for targeted extraction
   - Multi-strategy fallbacks (expanded retrieval → heuristic snippets)
   - LLM caching to avoid redundant calls
4. **Row Assembly**: Merge data from multiple papers into single table rows
5. **Incremental Writing**: Write completed rows immediately to prevent data loss

**Core Classes:**

*Schema Discovery:*
- `Column` - Individual schema column with name, rationale, definition
- `Schema` - Collection of columns with merging and similarity operations
- `LLMInterface` - Abstract base for different LLM providers
- `EmbeddingRetriever` - Document passage retrieval

*Value Extraction:*
- `TableBuilder` - Orchestrates the complete value extraction pipeline
- `PaperProcessor` - Handles value extraction from individual papers
- `JSONResponseParser` - Parses and validates LLM JSON responses
- `LLMCache` - Thread-safe LRU cache for LLM responses
- `RowDataManager` - Manages row grouping, merging, and validation
- `TextProcessor` - Text processing utilities (snippets, keywords, retrieval queries)
- `PromptBuilder` - Constructs prompts for value extraction LLM calls

## Research Domain
The project focuses on Nuclear Export Signal (NES) research, extracting structured information about protein sequences and their export properties from scientific literature.

## Important Notes
- QBSD must be run from the QBSD/ directory due to relative imports
- All LLM calls have been standardized through the llm_backends.py interface for security and consistency
- Configuration files define experimental parameters and should be customized per experiment
- The system supports both Together AI and OpenAI backends via environment variables (TOGETHER_API_KEY, OPENAI_API_KEY)
- All API keys must be set as environment variables - no hardcoded tokens remain in the codebase
- **Retrievers are now fully configurable** - dynamic_k, thresholds, batch sizes, and devices can be set via config files or CLI arguments
- **Evaluation capabilities** are provided through arxivDIGESTables framework for objective schema quality assessment
- **Resume functionality** preserves existing rows exactly as written - no data modification occurs when resuming
- **Value extraction is modular** - import classes from `value_extraction.core` for programmatic use (e.g., `from value_extraction import TableBuilder`)
- **NEVER run LLM flows (QBSD.py or any scripts that call OpenAI/Together AI APIs) without explicit user permission due to API costs**