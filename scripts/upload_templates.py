#!/usr/bin/env python3
"""Upload example tables to Supabase storage as templates.

This script uploads pre-made ScheMatiQ result tables (CSV, JSON, JSONL) to the
Supabase 'templates' bucket for use in the production frontend's "Load from Examples" feature.

Usage:
    python scripts/upload_templates.py --source templates --bucket templates
    python scripts/upload_templates.py --file my_table.csv --name "My Example Table"
    python scripts/upload_templates.py  # Uses defaults

Environment variables required:
    SUPABASE_URL: Your Supabase project URL
    SUPABASE_KEY: Your Supabase service role key (anon key may have write restrictions)
"""

import os
import sys
import argparse
import json
import csv
from pathlib import Path
from typing import List, Dict, Optional

try:
    from supabase import create_client, Client
except ImportError:
    print("Error: supabase package not installed. Run: pip install supabase")
    sys.exit(1)


def get_supabase_client() -> Client:
    """Create and return a Supabase client."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        print("Error: SUPABASE_URL and SUPABASE_KEY environment variables are required")
        print("Set them with:")
        print("  export SUPABASE_URL='https://your-project.supabase.co'")
        print("  export SUPABASE_KEY='your-service-role-key'")
        sys.exit(1)

    return create_client(url, key)


def get_file_type(filename: str) -> str:
    """Get the file type from filename."""
    ext = filename.lower().split('.')[-1]
    return ext


def get_content_type(filename: str) -> str:
    """Determine content type based on file extension."""
    ext = filename.lower().split('.')[-1]
    content_types = {
        'csv': 'text/csv',
        'json': 'application/json',
        'jsonl': 'application/jsonl',
    }
    return content_types.get(ext, 'application/octet-stream')


def count_rows_and_columns(file_path: Path) -> tuple[int, int]:
    """Count rows and columns in a data file.

    Returns (row_count, column_count).
    """
    ext = file_path.suffix.lower()

    try:
        if ext == '.csv':
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader, [])
                column_count = len(headers)
                row_count = sum(1 for _ in reader)
            return row_count, column_count

        elif ext == '.json':
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list) and len(data) > 0:
                    row_count = len(data)
                    column_count = len(data[0].keys()) if isinstance(data[0], dict) else 0
                    return row_count, column_count
            return 0, 0

        elif ext == '.jsonl':
            row_count = 0
            column_count = 0
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        row_count += 1
                        if row_count == 1:
                            try:
                                obj = json.loads(line)
                                column_count = len(obj.keys()) if isinstance(obj, dict) else 0
                            except:
                                pass
            return row_count, column_count

    except Exception as e:
        print(f"Warning: Could not count rows/columns in {file_path}: {e}")

    return 0, 0


def find_templates(source_dir: Path) -> List[Dict]:
    """Find all template files in the source directory.

    Returns list of template info dicts.
    """
    templates = []

    if not source_dir.exists():
        print(f"Warning: Source directory does not exist: {source_dir}")
        return templates

    # Supported extensions
    extensions = {'.csv', '.json', '.jsonl'}

    for file_path in source_dir.iterdir():
        if not file_path.is_file():
            continue

        if file_path.suffix.lower() not in extensions:
            continue

        if file_path.name.startswith('.'):
            continue

        row_count, column_count = count_rows_and_columns(file_path)

        template = {
            'name': file_path.stem,  # filename without extension
            'path': file_path,
            'file_type': get_file_type(file_path.name),
            'row_count': row_count,
            'column_count': column_count,
        }

        templates.append(template)
        print(f"Found template: {template['name']} ({template['file_type']}, {row_count} rows, {column_count} cols)")

    return templates


def upload_template(
    client: Client,
    bucket: str,
    template: Dict,
    custom_name: Optional[str] = None,
    dry_run: bool = False
) -> bool:
    """Upload a template file to Supabase.

    Returns True if successful.
    """
    file_path = template['path']
    file_type = template['file_type']

    # Use custom name if provided, otherwise use filename
    name = custom_name or template['name']
    storage_path = f"{name}.{file_type}"
    content_type = get_content_type(file_path.name)

    if dry_run:
        print(f"  [DRY RUN] Would upload: {file_path.name} -> {storage_path}")
        return True

    try:
        with open(file_path, 'rb') as f:
            file_content = f.read()

        # Upload to Supabase storage
        result = client.storage.from_(bucket).upload(
            storage_path,
            file_content,
            file_options={"content-type": content_type}
        )

        print(f"  Uploaded: {file_path.name} -> {storage_path}")
        return True

    except Exception as e:
        error_msg = str(e)
        if "Duplicate" in error_msg or "already exists" in error_msg.lower():
            print(f"  Skipped (exists): {file_path.name}")
            return True
        else:
            print(f"  Error uploading {file_path.name}: {e}")
            return False


def ensure_bucket_exists(client: Client, bucket: str) -> bool:
    """Ensure the bucket exists, create if it doesn't."""
    try:
        buckets = client.storage.list_buckets()
        bucket_names = [b.name for b in buckets]

        if bucket not in bucket_names:
            print(f"Creating bucket: {bucket}")
            client.storage.create_bucket(bucket, options={"public": True})
            return True

        return True
    except Exception as e:
        print(f"Error checking/creating bucket: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Upload templates to Supabase storage")
    parser.add_argument(
        "--source",
        type=str,
        default="templates",
        help="Source directory containing template files (default: templates)"
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default="templates",
        help="Supabase storage bucket name (default: templates)"
    )
    parser.add_argument(
        "--file",
        type=str,
        help="Upload a single file as a template"
    )
    parser.add_argument(
        "--name",
        type=str,
        help="Custom name for the template (used with --file)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be uploaded without actually uploading"
    )

    args = parser.parse_args()

    print(f"Target bucket: {args.bucket}")
    print()

    # Handle single file upload
    if args.file:
        file_path = Path(args.file)
        if not file_path.exists():
            print(f"Error: File not found: {file_path}")
            sys.exit(1)

        row_count, column_count = count_rows_and_columns(file_path)
        template = {
            'name': args.name or file_path.stem,
            'path': file_path,
            'file_type': get_file_type(file_path.name),
            'row_count': row_count,
            'column_count': column_count,
        }

        print(f"Uploading single template: {template['name']}")
        print(f"  File: {file_path}")
        print(f"  Type: {template['file_type']}")
        print(f"  Size: {row_count} rows, {column_count} columns")
        print()

        if not args.dry_run:
            client = get_supabase_client()
            if not ensure_bucket_exists(client, args.bucket):
                sys.exit(1)
            upload_template(client, args.bucket, template, args.name, args.dry_run)
        else:
            print(f"[DRY RUN] Would upload {file_path} as {template['name']}")

        sys.exit(0)

    # Handle directory upload
    project_root = Path(__file__).parent.parent
    source_dir = project_root / args.source

    print(f"Source directory: {source_dir}")

    # Find templates
    templates = find_templates(source_dir)

    if not templates:
        print("\nNo templates found in source directory")
        print(f"Create a '{args.source}' directory with CSV, JSON, or JSONL files")
        print("Or use --file to upload a single file")
        sys.exit(0)

    print(f"\nFound {len(templates)} templates to upload")
    print()

    if args.dry_run:
        print("=== DRY RUN MODE ===\n")

    # Connect to Supabase
    client = get_supabase_client()

    # Ensure bucket exists
    if not args.dry_run:
        if not ensure_bucket_exists(client, args.bucket):
            sys.exit(1)

    # Upload each template
    uploaded = 0
    for template in templates:
        print(f"Uploading template: {template['name']}")
        if upload_template(client, args.bucket, template, dry_run=args.dry_run):
            uploaded += 1
        print()

    print(f"Total templates {'would be ' if args.dry_run else ''}uploaded: {uploaded}")


if __name__ == "__main__":
    main()
