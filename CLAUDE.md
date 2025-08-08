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
- `valueExtractor.py` - Value extraction from discovered schemas
- `configurations/` - JSON config files for different experimental setups

### src/ - Data Processing & Experiments  
- `run.py` - Alternative experimental runner (legacy, contains hardcoded API token)
- `create_data.py` - Data preprocessing scripts
- `generate_tables.py` - Table generation from schemas
- `download_nes_papers.py` - Data collection scripts for NES (Nuclear Export Signal) research
- `abstracts/` and `full_text/` - Research paper data directories
- `manually/` - Manual annotation data

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

### Configuration
QBSD experiments are configured via JSON files in `QBSD/configurations/`. Key config parameters:
- `query` - The research question to explore
- `docs_path` - Path to document collection  
- `backend` - LLM provider and model settings
- `retriever` - Embedding model configuration
- `max_keys_schema` - Schema size limit

## Key Concepts

**Schema Discovery Pipeline:**
1. Content retrieval using embedding-based similarity
2. LLM-based schema generation with iterative refinement
3. Schema merging using semantic similarity
4. Convergence evaluation to determine completion

**Core Classes:**
- `Column` - Individual schema column with name, rationale, definition
- `Schema` - Collection of columns with merging and similarity operations
- `LLMInterface` - Abstract base for different LLM providers
- `EmbeddingRetriever` - Document passage retrieval

## Research Domain
The project focuses on Nuclear Export Signal (NES) research, extracting structured information about protein sequences and their export properties from scientific literature.

## Important Notes
- QBSD must be run from the QBSD/ directory due to relative imports
- All LLM calls have been standardized through the llm_backends.py interface for security and consistency
- Configuration files define experimental parameters and should be customized per experiment
- The system supports both Together AI and OpenAI backends via environment variables (TOGETHER_API_KEY, OPENAI_API_KEY)
- All API keys must be set as environment variables - no hardcoded tokens remain in the codebase
- **NEVER run LLM flows (QBSD.py or any scripts that call OpenAI/Together AI APIs) without explicit user permission due to API costs**