"""Download a dataset folder from the Supabase 'datasets' bucket to a local folder.

Usage (from the repo root):
    .venv\\Scripts\\python.exe backend\\scripts\\download_dataset.py            # auto-picks the NES dataset
    .venv\\Scripts\\python.exe backend\\scripts\\download_dataset.py --list     # show available datasets
    .venv\\Scripts\\python.exe backend\\scripts\\download_dataset.py <name> --out <dir>

Reads SUPABASE_URL / SUPABASE_KEY from backend/.env. Files that already exist
locally are skipped, so the script can be re-run to resume.
"""

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

BUCKET = "datasets"
PAGE = 1000


def list_items(storage, path: str):
    items, offset = [], 0
    while True:
        page = storage.list(path, {"limit": PAGE, "offset": offset})
        if not page:
            break
        items.extend(page)
        if len(page) < PAGE:
            break
        offset += PAGE
    return items


def walk_files(storage, path: str):
    """Yield every file path under `path`, recursing into subfolders (items with no id)."""
    for item in list_items(storage, path):
        child = f"{path}/{item['name']}" if path else item["name"]
        if item.get("id"):
            yield child
        else:
            yield from walk_files(storage, child)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("dataset", nargs="?", help="Dataset folder name (default: the one whose name contains 'nes')")
    parser.add_argument("--out", default=str(Path.home() / "Downloads" / "NES"), help="Local output folder (default: %(default)s)")
    parser.add_argument("--list", action="store_true", help="List datasets and exit")
    args = parser.parse_args()

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    url, key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if not url or not key:
        sys.exit("SUPABASE_URL / SUPABASE_KEY not found in backend/.env")

    storage = create_client(url, key).storage.from_(BUCKET)
    datasets = sorted(i["name"] for i in list_items(storage, "") if not i.get("id"))

    if args.list:
        print("\n".join(datasets) or "(no datasets)")
        return

    name = args.dataset
    if not name:
        matches = [d for d in datasets if "nes" in d.lower()]
        if len(matches) != 1:
            sys.exit(f"Could not pick a single NES dataset (matches: {matches}). "
                     f"Pass the name explicitly. Available: {datasets}")
        name = matches[0]
    elif name not in datasets:
        sys.exit(f"Dataset '{name}' not found. Available: {datasets}")

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    files = list(walk_files(storage, name))
    print(f"Downloading {len(files)} files from '{BUCKET}/{name}' to {out_dir}")

    downloaded = skipped = failed = 0
    for i, remote in enumerate(files, 1):
        local = out_dir / remote[len(name) + 1:]
        if local.exists():
            skipped += 1
            continue
        try:
            data = storage.download(remote)
            local.parent.mkdir(parents=True, exist_ok=True)
            local.write_bytes(data)
            downloaded += 1
        except Exception as e:
            failed += 1
            print(f"  FAILED {remote}: {e}")
        if i % 25 == 0 or i == len(files):
            print(f"  {i}/{len(files)}")

    print(f"Done: {downloaded} downloaded, {skipped} already existed, {failed} failed")


if __name__ == "__main__":
    main()
