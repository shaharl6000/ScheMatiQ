#!/usr/bin/env python3
"""Upload research datasets to Supabase storage.

This script uploads the contents of research/data directories to the
Supabase 'datasets' bucket for use in the production frontend.

Usage:
    python scripts/upload_datasets.py --source research/data --bucket datasets
    python scripts/upload_datasets.py  # Uses defaults

Environment variables required:
    SUPABASE_URL: Your Supabase project URL
    SUPABASE_KEY: Your Supabase service role key (anon key may have write restrictions)
"""

import os
import sys
import argparse
from pathlib import Path
from typing import List, Tuple

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


def get_content_type(filename: str) -> str:
    """Determine content type based on file extension."""
    ext = filename.lower().split('.')[-1]
    content_types = {
        'txt': 'text/plain',
        'md': 'text/markdown',
        'pdf': 'application/pdf',
        'json': 'application/json',
        'jsonl': 'application/jsonl',
        'csv': 'text/csv',
        'doc': 'application/msword',
        'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    }
    return content_types.get(ext, 'application/octet-stream')


def find_datasets(source_dir: Path) -> List[Tuple[str, Path]]:
    """Find all dataset directories in the source directory.

    Returns list of (dataset_name, dataset_path) tuples.
    """
    datasets = []

    if not source_dir.exists():
        print(f"Error: Source directory does not exist: {source_dir}")
        sys.exit(1)

    for item in source_dir.iterdir():
        if item.is_dir():
            # Check if directory contains files (not just subdirectories)
            files = list(item.glob('*'))
            file_count = sum(1 for f in files if f.is_file())
            if file_count > 0:
                datasets.append((item.name, item))
                print(f"Found dataset: {item.name} ({file_count} files)")

    return datasets


def upload_dataset(client: Client, bucket: str, dataset_name: str, dataset_path: Path, dry_run: bool = False) -> int:
    """Upload all files in a dataset directory to Supabase.

    Returns the number of files uploaded.
    """
    uploaded = 0

    for file_path in dataset_path.iterdir():
        if not file_path.is_file():
            continue

        # Skip hidden files
        if file_path.name.startswith('.'):
            continue

        storage_path = f"{dataset_name}/{file_path.name}"
        content_type = get_content_type(file_path.name)

        if dry_run:
            print(f"  [DRY RUN] Would upload: {file_path.name} -> {storage_path}")
            uploaded += 1
            continue

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
            uploaded += 1

        except Exception as e:
            error_msg = str(e)
            if "Duplicate" in error_msg or "already exists" in error_msg.lower():
                print(f"  Skipped (exists): {file_path.name}")
            else:
                print(f"  Error uploading {file_path.name}: {e}")

    return uploaded


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
    parser = argparse.ArgumentParser(description="Upload datasets to Supabase storage")
    parser.add_argument(
        "--source",
        type=str,
        default="research/data",
        help="Source directory containing dataset folders (default: research/data)"
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default="datasets",
        help="Supabase storage bucket name (default: datasets)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be uploaded without actually uploading"
    )
    parser.add_argument(
        "--datasets",
        type=str,
        nargs="+",
        help="Specific dataset names to upload (default: all)"
    )

    args = parser.parse_args()

    # Resolve source path relative to project root
    project_root = Path(__file__).parent.parent
    source_dir = project_root / args.source

    print(f"Source directory: {source_dir}")
    print(f"Target bucket: {args.bucket}")
    print()

    # Find datasets
    datasets = find_datasets(source_dir)

    if not datasets:
        print("No datasets found in source directory")
        sys.exit(0)

    # Filter to specific datasets if requested
    if args.datasets:
        datasets = [(name, path) for name, path in datasets if name in args.datasets]
        if not datasets:
            print(f"No matching datasets found for: {args.datasets}")
            sys.exit(1)

    print(f"\nFound {len(datasets)} datasets to upload")
    print()

    if args.dry_run:
        print("=== DRY RUN MODE ===\n")

    # Connect to Supabase
    client = get_supabase_client()

    # Ensure bucket exists
    if not args.dry_run:
        if not ensure_bucket_exists(client, args.bucket):
            sys.exit(1)

    # Upload each dataset
    total_uploaded = 0
    for dataset_name, dataset_path in datasets:
        print(f"Uploading dataset: {dataset_name}")
        uploaded = upload_dataset(
            client,
            args.bucket,
            dataset_name,
            dataset_path,
            dry_run=args.dry_run
        )
        total_uploaded += uploaded
        print()

    print(f"Total files {'would be ' if args.dry_run else ''}uploaded: {total_uploaded}")


if __name__ == "__main__":
    main()
