#!/usr/bin/env python3
"""
Convert QBSD JSON output to CSV format, excluding metadata fields.

This script processes JSON files containing QBSD value extraction results
and converts them to CSV format suitable for analysis, removing internal
metadata fields like _metadata, _papers, etc.
"""

import json
import csv
import argparse
import sys
from pathlib import Path
from typing import Dict, Any, List


def extract_data_fields(record: Dict[str, Any], include_excerpts: bool = False) -> Dict[str, str]:
    """
    Extract data fields from a JSON record, excluding metadata fields.
    
    Args:
        record: JSON record containing QBSD extraction results
        include_excerpts: Whether to include excerpts in the output
        
    Returns:
        Dictionary with cleaned data fields
    """
    # Fields to exclude (metadata and internal fields)
    exclude_fields = {
        '_row_name', '_papers', '_metadata', 
        '"name"', '"definition"', '"rationale"'
    }
    
    cleaned_record = {}
    
    # Add row name as first column if it exists
    if '_row_name' in record:
        cleaned_record['row_name'] = record['_row_name']
    
    # Process all other fields
    for key, value in record.items():
        if key not in exclude_fields:
            # Handle nested dictionaries (like answer/excerpts structure)
            if isinstance(value, dict):
                if 'answer' in value:
                    cleaned_record[key] = value['answer']
                    # Add excerpts if requested and available
                    if include_excerpts and 'excerpts' in value and value['excerpts']:
                        excerpts_key = f"{key}_excerpts"
                        # Join excerpts with semicolon separator for CSV compatibility
                        cleaned_record[excerpts_key] = "; ".join(value['excerpts'])
                else:
                    # Convert dict to string representation
                    cleaned_record[key] = str(value)
            else:
                cleaned_record[key] = str(value)
    
    return cleaned_record


def convert_json_to_csv(input_file: Path, output_file: Path = None, include_excerpts: bool = False) -> None:
    """
    Convert JSON file to CSV format.
    
    Args:
        input_file: Path to input JSON file
        output_file: Path to output CSV file (optional)
        include_excerpts: Whether to include excerpts in the output
    """
    if output_file is None:
        output_file = input_file.with_suffix('.csv')
    
    records = []
    
    # Read JSON file (assuming JSONL format - one JSON object per line)
    with open(input_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
                
            try:
                record = json.loads(line)
                cleaned_record = extract_data_fields(record, include_excerpts)
                records.append(cleaned_record)
            except json.JSONDecodeError as e:
                print(f"Warning: Skipping invalid JSON on line {line_num}: {e}", file=sys.stderr)
                continue
    
    if not records:
        print("No valid records found in the input file.", file=sys.stderr)
        return
    
    # Get all unique fieldnames from all records
    all_fieldnames = set()
    for record in records:
        all_fieldnames.update(record.keys())
    
    # Sort fieldnames, but keep 'row_name' first if it exists
    fieldnames = sorted(all_fieldnames)
    if 'row_name' in fieldnames:
        fieldnames.remove('row_name')
        fieldnames.insert(0, 'row_name')
    
    # Write CSV file
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    
    print(f"Converted {len(records)} records from {input_file} to {output_file}")


def main():
    """Main function to handle command line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert QBSD JSON output to CSV format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python json_to_csv_converter.py input.json
  python json_to_csv_converter.py input.json -o output.csv
  python json_to_csv_converter.py outputs/values_nes_with_retriever_one_by_one_exp12.json
        """
    )
    
    parser.add_argument(
        'input_file',
        type=Path,
        help='Input JSON file path'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=Path,
        help='Output CSV file path (default: input_file.csv)'
    )
    
    parser.add_argument(
        '--include-excerpts',
        action='store_true',
        help='Include excerpts from the JSON in the CSV output'
    )
    
    args = parser.parse_args()
    
    if not args.input_file.exists():
        print(f"Error: Input file '{args.input_file}' not found.", file=sys.stderr)
        sys.exit(1)
    
    try:
        convert_json_to_csv(args.input_file, args.output, args.include_excerpts)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()