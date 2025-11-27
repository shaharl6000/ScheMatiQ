# ValueExtractor Evaluation

This directory contains evaluation tools for QBSD valueExtractor outputs.

## Files

- `data_quality_evaluation.py` - Main evaluation script (moved to ../evaluation/)
- `evaluation_results.json` - Detailed evaluation results (generated)

## Usage

```bash
cd QBSD
python evaluation/data_quality_evaluation.py --gt_file ../data/NesDB_all_CRM1_with_peptides.csv --pred_file outputs/values_nes_with_retriever_one_by_one3.json
```

## Metrics

The evaluation script provides metrics compatible with arxivDIGESTables:

### Schema Recall Metrics
- **Schema Recall (GT-based)**: Percentage of ground truth fields successfully matched and extracted
- **Field Recall (Pred-based)**: Percentage of prediction fields successfully matched to GT
- **Exact Match Score**: Average exact string matching score
- **Semantic Similarity Score**: Average semantic similarity using sentence transformers

### Evaluation Methods
1. **Exact Match**: String-level exact comparison
2. **Substring Match**: Partial matching and overlap detection  
3. **Semantic Similarity**: Meaning-based comparison using sentence transformers

### Field Alignment
- Uses explicit field mappings for common prediction→GT relationships
- Falls back to semantic similarity matching for unmapped fields
- Configurable similarity threshold (default: 0.7)

## Data Sources

- **Ground Truth**: `../../data/NesDB_all_CRM1_with_peptides.csv`
- **Predictions**: `../outputs/values_nes_with_retriever_one_by_one3.json`

## Output

- Console summary with key metrics
- Detailed JSON report with per-field comparisons (`evaluation_results.json`)
- Analysis of field performance and alignment success

## Alignment Strategy

The script handles schema mismatches through:

1. **Explicit Mappings**: Predefined mappings for common field variations
2. **Semantic Similarity**: Sentence transformer-based matching for unmapped fields
3. **Multiple Similarity Scores**: Exact, substring, and semantic similarity evaluation

## Example Results

```
=== EVALUATION SUMMARY ===
Total proteins evaluated: 7

--- arxivDIGESTables-Style Metrics ---
Schema recall (GT-based): 0.538
Field recall (Pred-based): 0.895
Exact match score: 0.034
Semantic similarity score: 0.119
```

The evaluation shows strong field alignment (89.5% of prediction fields matched) but moderate semantic accuracy (11.9%), indicating successful schema discovery but room for improvement in value extraction quality.