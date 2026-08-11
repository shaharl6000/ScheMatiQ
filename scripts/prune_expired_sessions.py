#!/usr/bin/env python3
"""Delete operational session storage past the retention window.

This enforces the operational retention promise in privacy_policy.md. It removes
a session's working copies from Supabase, so a project older than the window
stops occupying storage:

    sessions/{session_id}.json
    data/{session_id}/...
    documents/{session_id}/...

It does NOT touch the research archive. Those ZIPs live in Google Drive and the
policy retains them indefinitely, removable on request only.

Dry run is the default. Nothing is deleted unless --apply is passed, because
these deletions are irreversible and the objects are user research data.

Usage:
    python scripts/prune_expired_sessions.py                    # report only
    python scripts/prune_expired_sessions.py --days 180         # report only
    python scripts/prune_expired_sessions.py --days 180 --apply # actually delete

Environment variables required:
    SUPABASE_URL: Your Supabase project URL
    SUPABASE_KEY: Key with delete permission on the buckets below
"""

import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# sessions/ is listed last on purpose: it is the record that makes a project
# resolvable at all. If a run dies midway we would rather leave an orphaned data
# file behind than a session that resolves to a project with no data.
BUCKETS = ["data", "documents", "sessions"]
PAGE = 1000
MAX_DEPTH = 4
DEFAULT_DAYS = 180


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024 or unit == "GB":
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} GB"


def load_env() -> "tuple[str, str]":
    try:
        from dotenv import load_dotenv

        for candidate in (".env", "backend/.env", "../backend/.env"):
            if os.path.exists(candidate):
                load_dotenv(candidate)
                break
    except ImportError:
        pass
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        sys.exit("SUPABASE_URL / SUPABASE_KEY not set. Export them or add backend/.env.")
    return url, key


def list_all(client, bucket: str, prefix: str) -> list:
    """Every entry under prefix. storage3 defaults to limit=100, so paginate."""
    out = []
    offset = 0
    while True:
        try:
            page = client.storage.from_(bucket).list(
                prefix, {"limit": PAGE, "offset": offset}
            )
        except Exception as exc:
            print(f"  ! could not list {bucket}/{prefix}: {str(exc)[:100]}")
            return out
        if not page:
            break
        out.extend(page)
        if len(page) < PAGE:
            break
        offset += PAGE
    return out


def walk(client, bucket: str, prefix: str = "", depth: int = 0):
    """Yield (path, size_bytes, updated_at) for every object under prefix."""
    if depth > MAX_DEPTH:
        return
    for entry in list_all(client, bucket, prefix):
        name = entry.get("name")
        if not name:
            continue
        path = f"{prefix}/{name}" if prefix else name
        meta = entry.get("metadata") or {}
        size = meta.get("size")
        if entry.get("id") is None and size is None:
            yield from walk(client, bucket, path, depth + 1)
        elif size is not None:
            yield path, int(size), entry.get("updated_at") or entry.get("created_at")


def parse_ts(value: str):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def session_id_of(path: str) -> str:
    return path.split("/")[0].replace(".json", "")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=DEFAULT_DAYS,
                    help=f"retention window in days (default {DEFAULT_DAYS})")
    ap.add_argument("--apply", action="store_true",
                    help="actually delete. Without this, only reports.")
    ap.add_argument("--limit", type=int, default=0,
                    help="stop after this many sessions (0 = no limit)")
    args = ap.parse_args()

    if args.days < 30:
        sys.exit(f"Refusing to run with --days {args.days}: too aggressive for a "
                 "retention window. Use 30 or more.")

    url, key = load_env()
    try:
        from supabase import create_client
    except ImportError:
        sys.exit("pip install supabase")
    client = create_client(url, key)

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    mode = "APPLY (deletions are permanent)" if args.apply else "DRY RUN (nothing deleted)"
    print(f"Retention prune  window={args.days}d  cutoff={cutoff:%Y-%m-%d}  {mode}")
    print("=" * 74)

    # Newest timestamp per session across all buckets. A session is only expired
    # when everything belonging to it is older than the cutoff, so an old
    # session.json cannot strand a table the user edited last week.
    newest: dict = {}
    objects: dict = defaultdict(list)
    undated = set()

    for bucket in BUCKETS:
        for path, size, updated in walk(client, bucket):
            sid = session_id_of(path)
            objects[sid].append((bucket, path, size))
            ts = parse_ts(updated)
            if ts is None:
                undated.add(sid)
            elif sid not in newest or ts > newest[sid]:
                newest[sid] = ts

    expired = sorted(
        sid for sid, ts in newest.items()
        if ts < cutoff and sid not in undated
    )
    if args.limit:
        expired = expired[:args.limit]

    if undated:
        print(f"note: {len(undated)} session(s) had an object with no readable "
              f"timestamp and were skipped for safety\n")

    if not expired:
        print(f"Nothing older than {args.days} days. "
              f"{len(newest)} session(s) examined, none expired.")
        return

    total = 0
    for sid in expired:
        entries = objects[sid]
        size = sum(s for _, _, s in entries)
        total += size
        print(f"  {sid[:8]}  last modified {newest[sid]:%Y-%m-%d}  "
              f"{len(entries):>3} object(s)  {human(size):>10}")

    print("-" * 74)
    print(f"{len(expired)} session(s), {human(total)} of "
          f"{len(newest)} session(s) examined")

    if not args.apply:
        print(f"\nDry run. Re-run with --apply to delete. "
              f"Sanity-check a few ids above first.")
        return

    print("\nDeleting...")
    deleted = failed = 0
    for sid in expired:
        for bucket in BUCKETS:
            paths = [p for b, p, _ in objects[sid] if b == bucket]
            if not paths:
                continue
            try:
                client.storage.from_(bucket).remove(paths)
                deleted += len(paths)
            except Exception as exc:
                failed += len(paths)
                print(f"  ! {bucket}/{sid[:8]}: {str(exc)[:100]}")

    print(f"\nDeleted {deleted} object(s) across {len(expired)} session(s). "
          f"{failed} failure(s).")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
