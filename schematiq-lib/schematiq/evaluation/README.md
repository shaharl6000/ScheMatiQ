# Schema Evaluation Framework

This framework evaluates the quality of ScheMatiQ-generated schemas by testing how well they enable answering research queries using extracted data.

## Overview

The evaluation framework uses a **query-centric approach**: it gives an LLM the extracted data from each row and asks it to answer the original research query. The answers are then compared to ground truth values to assess schema quality.

### Key Features

- **Few-shot Learning**: Uses ground truth examples to improve evaluation consistency
- **Multiple Comparison Strategies**: Automatic detection of answer types (binary, numeric, sequences, etc.)
- **Column Usage Analysis**: Identifies which columns are most/least valuable
- **Configurable Pipeline**: Supports different LLM backends and evaluation strategies
- **Comprehensive Reporting**: Generates detailed metrics and recommendations

## Quick Start

### 1. Configure Evaluation

Create or modify the configuration file:

```bash
cp evaluation/config/evaluation_config.json my_evaluation.json
```

Key configuration options:

```json
{
  "query_config_path": "path/to/schematiq_config.json",
  "data_path_override": "path/to/extracted_data.jsonl",
  "gt_column": "ground_truth",

  "few_shot_config": {
    "n_shots_per_category": 3,
    "selection_strategy": "stratified"
  },

  "evaluator_config": {
    "provider": "openai",
    "model": "gpt-4o",
    "temperature": 0.1
  }
}
```

### 2. Run Evaluation

```bash
# Using default config
python -m schematiq.evaluation.query_answering_evaluation

# Using custom config
python -m schematiq.evaluation.query_answering_evaluation my_evaluation.json

# Specify output path
python -m schematiq.evaluation.query_answering_evaluation my_evaluation.json results/my_results.json
```

### 3. View Results

The evaluation generates:
- **JSON Results**: Complete evaluation data (`evaluation_results.json`)
- **CSV Summary**: Key metrics for analysis (`evaluation_results_summary.csv`)
- **Markdown Report**: Human-readable summary (`evaluation_results_report.md`)

## Architecture

### Core Components

1. **SchemaEvaluator**: Main orchestrator
2. **FewShotManager**: Extracts GT examples for few-shot prompting
3. **RowQueryEvaluator**: Evaluates individual rows using LLM
4. **GTComparator**: Compares LLM answers to ground truth

### Evaluation Pipeline

```
Data Loading -> Few-Shot Selection -> Row Evaluation -> GT Comparison -> Metrics & Reports
```

## Metrics Calculated

**Overall Metrics:**
- `accuracy`: Exact match rate with ground truth
- `answer_rate`: Percentage of answered (non-"insufficient") queries
- `confidence_distribution`: High/Medium/Low confidence rates
- `avg_similarity`: Average semantic similarity to GT

**Column Analysis:**
- `column_usage_rates`: How often each column is referenced
- `most_used_columns`: Top contributors to answers
- `unused_columns`: Candidates for removal
- `highly_used_columns`: Essential schema elements

## Configuration Reference

### Few-Shot Configuration

```json
"few_shot_config": {
  "n_shots_per_category": 3,
  "selection_strategy": "stratified",
  "specific_rows": ["paper1", "paper2"]
}
```

**Selection Strategies:**
- `stratified`: Balanced samples per GT value (best for diverse GTs)
- `diverse`: Maximum variety across all columns
- `representative`: Most frequent GT patterns
- `specific`: User-defined example rows

### Evaluator Configuration

```json
"evaluator_config": {
  "provider": "openai",
  "model": "gpt-4o",
  "max_tokens": 1024,
  "temperature": 0.1
}
```

### Comparison Configuration

```json
"comparison_config": {
  "default_strategy": "auto",
  "binary_threshold": 0.8,
  "sequence_similarity_threshold": 0.9
}
```
