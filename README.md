# QueryDiscovery: Query-Based Schema Discovery

🔬 **Automatically discover table schemas from scientific documents to answer research queries**

Instead of pre-defining what to extract, simply ask a question and let QBSD discover the optimal table structure to answer it.

## 🚀 Quick Start

```bash
pip install -r requirements.txt
export GEMINI_API_KEY="your_key_here"  # Support for multiple keys: GEMINI_API_KEYS="key1,key2,key3"

cd QBSD
# 1. Discover schema from documents
python QBSD.py --config example_config.json

# 2. Extract values using discovered schema  
python valueExtractor.py example_extraction_config.json

# 3. Evaluate schema quality
python evaluation/run_evaluation.py example_eval_config.json
```

## 🏗️ Architecture

```
Research Query → Schema Discovery → Value Extraction → Evaluation
   Documents  →  Table Schema   →  Populated Table →  Quality Metrics
```

## 📋 Components

### 1. Schema Discovery (`QBSD.py`)
**Discovers optimal table columns to answer your research query**

- **Input**: Research question + document collection
- **Output**: JSON schema with column definitions
- **Example**: "What factors affect protein folding?" → discovers columns like `protein_sequence`, `folding_temperature`, `stability_score`

### 2. Value Extraction (`valueExtractor.py`) 
**Populates the discovered schema with data from documents**

- **Input**: Schema JSON + document directories
- **Output**: JSONL file with extracted structured data
- **Features**: Multi-directory support, resume capability, parallel processing

### 3. Schema Evaluation (`evaluation/run_evaluation.py`)
**Evaluates how well the schema answers the original query**

- **Input**: Extracted data + ground truth labels
- **Output**: Accuracy, completeness, and utility metrics
- **Reports**: JSON results, CSV summary, Markdown report

### 4. Data Creation (`src/download_nes_papers.py`)
**Downloads NES research papers for testing (domain-specific)**

- **Input**: NES protein database URLs
- **Output**: Organized research papers (abstracts + full text)
- **Features**: Anti-detection web scraping, PDF/HTML support

## ⚙️ Configuration Examples

**Schema Discovery** (`example_config.json`):
```json
{
  "query": "What factors influence protein folding stability?",
  "docs_path": "data/papers",
  "max_keys_schema": 20,
  "backend": {
    "provider": "gemini",
    "model": "gemini-2.5-flash",
    "max_context_tokens": 1000000
  },
  "output_path": "outputs/protein_schema.json"
}
```

**Value Extraction** (`example_extraction_config.json`):
```json
{
  "schema_path": "outputs/protein_schema.json",
  "docs_directories": ["data/papers", "data/reviews"],
  "mode": "all",
  "resume": true,
  "backend_cfg": {
    "provider": "gemini", 
    "model": "gemini-2.5-flash"
  },
  "output_path": "outputs/extracted_data.jsonl"
}
```

**Evaluation** (`example_eval_config.json`):
```json
{
  "data_path_override": "outputs/extracted_data.jsonl",
  "gt_column": "ground_truth_label",
  "evaluator_config": {
    "provider": "openai",
    "model": "gpt-4o"
  },
  "evaluation_metrics": ["accuracy", "completeness", "column_utilization"]
}
```
