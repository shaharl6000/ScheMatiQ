#!/usr/bin/env python3
"""Report how exposed the Supabase storage buckets are right now.

Three questions, answered from the live project rather than from memory:

    1. What kind of key is SUPABASE_KEY? A publishable key (legacy `anon`, or
       the newer `sb_publishable_`) is designed to ship in a browser bundle. If
       the backend runs on one, every storage permission it needs has to be
       granted to a credential that is safe to publish.
    2. Which buckets are marked Public? A public bucket serves its objects over
       an open URL with no key at all, which is republication, not just access.
    3. Can this key write? Run with the publishable key after locking policies
       down: an INSERT that still succeeds means a policy is still open.

Read-only by default. --probe-write is the only flag that mutates anything, and
it removes what it wrote.

Usage:
    python scripts/audit_storage_access.py                 # inventory + public probe
    python scripts/audit_storage_access.py --probe-write   # also test INSERT
    SUPABASE_KEY='<anon key>' python scripts/audit_storage_access.py --probe-write

Exit code is 1 if a bucket holding user data is public, or if the key is
publishable, so this can gate a deploy.

Environment variables required:
    SUPABASE_URL: Your Supabase project URL
    SUPABASE_KEY: The key to audit. Whichever key you pass is the key tested,
                  so pass the publishable one to check what an attacker sees.
"""

import argparse
import base64
import json
import os
import sys
import uuid

import requests

# Mirrors SupabaseStorageBackend.REQUIRED_BUCKETS. Duplicated on purpose: this
# script must run standalone, without importing the backend app.
REQUIRED_BUCKETS = [
    "sessions",
    "documents",
    "data",
    "exports",
    "datasets",
    "templates",
    "initial_schemas",
]

# Buckets whose contents are user research data. A public flag on any of these
# is a finding, not a configuration choice.
USER_DATA_BUCKETS = {"sessions", "documents", "data", "exports"}

PROBE_PREFIX = "_lockdown_probe"


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
    return url.rstrip("/"), key


def classify_key(key: str) -> "tuple[str, str]":
    """Return (kind, detail) where kind is 'server', 'publishable' or 'unknown'.

    Handles both key formats. The newer `sb_secret_` / `sb_publishable_` keys are
    opaque strings, not JWTs, so decoding a payload only works on legacy keys.
    """
    if key.startswith("sb_secret_"):
        return "server", "secret key (new format)"
    if key.startswith("sb_publishable_"):
        return "publishable", "publishable key (new format)"

    parts = key.split(".")
    if len(parts) == 3:
        try:
            payload = parts[1]
            payload += "=" * (-len(payload) % 4)
            claims = json.loads(base64.urlsafe_b64decode(payload))
        except (ValueError, json.JSONDecodeError):
            return "unknown", "looks like a JWT but the payload did not decode"
        role = claims.get("role", "<no role claim>")
        if role == "service_role":
            return "server", "legacy service_role JWT"
        if role == "anon":
            return "publishable", "legacy anon JWT"
        return "unknown", f"JWT with role={role!r}"

    return "unknown", "unrecognised key format"


def bucket_public_flags(client) -> "dict":
    """Map bucket name to its public flag, or {} if the key cannot list buckets."""
    try:
        buckets = client.storage.list_buckets()
    except Exception as exc:
        print(f"  ! could not list buckets: {str(exc)[:120]}")
        return {}
    flags = {}
    for bucket in buckets:
        if isinstance(bucket, dict):
            name, public = bucket.get("name"), bucket.get("public")
        else:
            name, public = getattr(bucket, "name", None), getattr(bucket, "public", None)
        if name:
            flags[name] = public
    return flags


def first_object(client, bucket: str) -> "str | None":
    """Path of any one object in the bucket, descending into folders."""
    return _walk(client, bucket, "", 0)


def _walk(client, bucket: str, prefix: str, depth: int) -> "str | None":
    if depth > 3:
        return None
    try:
        entries = client.storage.from_(bucket).list(prefix, {"limit": 20})
    except Exception:
        return None
    folders = []
    for entry in entries:
        name = entry.get("name")
        if not name:
            continue
        path = f"{prefix}/{name}" if prefix else name
        meta = entry.get("metadata") or {}
        if entry.get("id") is None and meta.get("size") is None:
            folders.append(path)
        else:
            return path
    for folder in folders:
        found = _walk(client, bucket, folder, depth + 1)
        if found:
            return found
    return None


def probe_public_url(url: str, bucket: str, path: str) -> "tuple[bool, int]":
    """GET the object with no credentials at all. 200 means it is served openly."""
    target = f"{url}/storage/v1/object/public/{bucket}/{requests.utils.quote(path)}"
    try:
        # No Authorization and no apikey header: this is the anonymous internet.
        response = requests.get(target, timeout=15)
    except requests.RequestException as exc:
        print(f"  ! request failed for {bucket}: {str(exc)[:100]}")
        return False, 0
    return response.status_code == 200, response.status_code


def probe_write(client, bucket: str) -> "tuple[str, str]":
    """Try one INSERT, then clean up. Returns (verdict, detail)."""
    path = f"{PROBE_PREFIX}/{uuid.uuid4().hex}.txt"
    try:
        client.storage.from_(bucket).upload(
            path, b"lockdown probe", {"content-type": "text/plain"}
        )
    except Exception as exc:
        return "DENIED", str(exc)[:90]
    try:
        client.storage.from_(bucket).remove([path])
        return "ALLOWED", "written and removed"
    except Exception as exc:
        return "ALLOWED", f"written but cleanup FAILED, remove {bucket}/{path} by hand ({str(exc)[:60]})"


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--probe-write",
        action="store_true",
        help="attempt one upload per bucket and delete it again",
    )
    args = ap.parse_args()

    try:
        from supabase import create_client
    except ImportError:
        sys.exit("supabase package not installed. Run: pip install supabase")

    url, key = load_env()
    client = create_client(url, key)

    findings = []

    print(f"Project: {url}")
    kind, detail = classify_key(key)
    label = {"server": "server-only", "publishable": "PUBLISHABLE", "unknown": "UNKNOWN"}[kind]
    print(f"Key:     {label} - {detail}")
    if kind == "publishable":
        print("         This key is safe to put in a browser bundle. Anything it can")
        print("         reach is reachable by anyone who has the key.")
        findings.append("backend key is publishable")
    elif kind == "unknown":
        print("         Could not classify. Check it by hand before trusting this run.")

    print("\nBuckets")
    flags = bucket_public_flags(client)
    if not flags:
        print("  (bucket list unavailable with this key, falling back to the known names)")
        flags = {name: None for name in REQUIRED_BUCKETS}

    for name in REQUIRED_BUCKETS:
        if name not in flags:
            print(f"  {name:<17} MISSING from the project")
            continue

        public = flags[name]
        state = {True: "PUBLIC", False: "private", None: "unknown"}[public]
        sample = first_object(client, name)

        line = f"  {name:<17} {state:<8}"
        if sample is None:
            line += " no object found to probe"
            print(line)
            continue

        exposed, status = probe_public_url(url, name, sample)
        if exposed:
            line += f" served with no key at all (HTTP {status})"
            findings.append(f"{name} is readable anonymously")
        else:
            line += f" anonymous read refused (HTTP {status})"
        print(line)

        if public and name in USER_DATA_BUCKETS and not exposed:
            findings.append(f"{name} is flagged Public and holds user data")

    if args.probe_write:
        print("\nWrite probe (one object per bucket, removed again)")
        for name in REQUIRED_BUCKETS:
            if name not in flags:
                continue
            verdict, detail = probe_write(client, name)
            print(f"  {name:<17} {verdict:<8} {detail}")
            if verdict == "ALLOWED" and kind == "publishable":
                findings.append(f"{name} accepts writes from a publishable key")

    print()
    if findings:
        print(f"{len(findings)} finding(s):")
        for finding in findings:
            print(f"  - {finding}")
        sys.exit(1)
    print("No findings. Buckets are private and the key is server-only.")


if __name__ == "__main__":
    main()
