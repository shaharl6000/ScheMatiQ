# Schema Evaluation Framework

This framework evaluates the quality of QBSD-generated schemas by testing how well they enable answering research queries using extracted data.

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
  "query_config_path": "path/to/qbsd_config.json",           # QBSD config with query
  "data_path_override": "path/to/extracted_data.jsonl",     # Override data path
  "gt_column": "GT_NES",                                     # Ground truth column name
  
  "few_shot_config": {
    "n_shots_per_category": 3,                              # Examples per GT category
    "selection_strategy": "stratified"                      # or "diverse", "representative"
  },
  
  "evaluator_config": {
    "provider": "openai",                                    # LLM for evaluation
    "model": "gpt-4o",
    "temperature": 0.1
  }
}
```

### 2. Run Evaluation

```bash
# Using default config
cd QBSD
python evaluation/run_evaluation.py

# Using custom config
python evaluation/run_evaluation.py my_evaluation.json

# Specify output path
python evaluation/run_evaluation.py my_evaluation.json results/my_results.json
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
Data Loading → Few-Shot Selection → Row Evaluation → GT Comparison → Metrics & Reports
```

#### 1. Few-Shot Example Selection

Extracts examples from ground truth data using configurable strategies:

- **Stratified**: N examples per GT category (recommended)
- **Diverse**: Maximize variety across all columns  
- **Representative**: Most common patterns
- **Specific**: User-defined rows

#### 2. Row Evaluation

For each data row:
1. Format extracted data (exclude GT column)
2. Build prompt with few-shot examples + query + row data
3. Get LLM answer with confidence and reasoning
4. Analyze which columns were used in the response

#### 3. Ground Truth Comparison

Automatically detects answer type and applies appropriate comparison:

- **Binary**: Yes/No, True/False answers
- **Numeric**: Numbers with tolerance-based matching
- **Sequence**: Protein sequences, IDs with edit distance
- **Semantic**: General text similarity using string matching

### Metrics Calculated

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
  "n_shots_per_category": 3,           // Examples per GT category
  "selection_strategy": "stratified",  // Selection method
  "specific_rows": ["paper1", "paper2"] // Optional: specific examples
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
  "provider": "openai",     // "openai", "together", "gemini"
  "model": "gpt-4o",        // Model name
  "max_tokens": 1024,       // Response length
  "temperature": 0.1        // Deterministic evaluation
}
```

### Comparison Configuration

```json
"comparison_config": {
  "default_strategy": "auto",              // Auto-detect comparison type
  "binary_threshold": 0.8,                // Confidence for binary matches  
  "sequence_similarity_threshold": 0.9    // Threshold for sequence matches
}
```

## Use Cases

### 1. Schema Quality Assessment

Evaluate how well your discovered schema answers research queries:

```bash
python evaluation/run_evaluation.py schema_eval.json
```

### 2. Schema Comparison

Compare different schema discovery methods:

```json
// config_method_a.json
{"data_path_override": "results/method_a_data.jsonl"}

// config_method_b.json  
{"data_path_override": "results/method_b_data.jsonl"}
```

### 3. Iterative Schema Improvement

Use recommendations to improve schema:

1. Run evaluation
2. Review `unused_columns` and `least_used_columns`
3. Modify schema discovery parameters
4. Re-evaluate and compare results

### 4. Domain Adaptation

Test schema performance across different research domains by changing the query and ground truth column.

## Output Analysis

### Key Metrics to Watch

- **Accuracy > 0.7**: Good schema quality
- **Answer Rate > 0.8**: Sufficient information coverage
- **High Confidence Rate > 0.5**: Clear, usable extracted data

### Common Issues & Solutions

**Low Accuracy (< 0.5):**
- Schema may not capture query-relevant information
- Consider adding more targeted columns
- Review few-shot examples for consistency

**Low Answer Rate (< 0.6):**
- Missing information in extractions
- Consider expanding retrieval or using longer contexts
- Add columns for missing information types

**Many Unused Columns:**
- Schema may be over-engineered
- Remove columns that don't contribute to query answering
- Focus on query-specific information

**Low Confidence:**
- Extracted data may be unclear or incomplete
- Improve extraction prompts or retrieval
- Consider post-processing extracted values

## Extending the Framework

### Custom Comparison Strategies

Add domain-specific comparison logic:

```python
from evaluation.core.gt_comparator import GTComparator

class CustomComparator(GTComparator):
    def _custom_comparison(self, predicted, gt):
        # Your domain-specific logic
        pass
```

### Additional Metrics

Extend evaluation with custom metrics:

```python  
from evaluation.core.schema_evaluator import SchemaEvaluator

class ExtendedEvaluator(SchemaEvaluator):
    def _calculate_custom_metrics(self, results):
        # Your custom metric calculations
        pass
```

### Alternative LLM Backends

The framework supports any LLM backend compatible with the QBSD `LLMInterface`:

```json
"evaluator_config": {
  "provider": "custom_provider",
  "model": "custom_model",
  // ... other parameters
}
```

## Troubleshooting

### Common Errors

**"Configuration file not found"**
- Check the config file path
- Ensure JSON syntax is valid

**"No evaluation data found"** 
- Verify `data_path_override` points to valid JSONL file
- Check that ground truth column exists in data

**"No ground truth data found"**
- Ensure GT column name matches data
- Verify GT column has non-null values

**LLM API Errors**
- Check API keys are set correctly
- Verify model names and provider settings
- Consider rate limiting for large evaluations

### Performance Tips

**Large Datasets:**
- Use `specific_rows` to evaluate subset first
- Reduce `n_shots_per_category` for faster iteration
- Consider parallel evaluation for production use

**Cost Optimization:**
- Use smaller/cheaper models for initial testing  
- Reduce `max_tokens` if answers are short
- Cache evaluation results between runs

## Examples

See the `examples/` directory for complete evaluation workflows with different research domains and schema types.