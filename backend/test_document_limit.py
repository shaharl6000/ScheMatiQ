"""Quick manual test for document upload limit feature.

Run with: python test_document_limit.py
Requires: backend running on localhost:8000
"""
import requests
import tempfile
import os

BASE = "http://localhost:8000"


def test_config_endpoint():
    """Test /api/config returns expected values."""
    r = requests.get(f"{BASE}/api/config")
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    data = r.json()
    assert "max_documents" in data, "Missing max_documents"
    assert "developer_mode" in data, "Missing developer_mode"
    assert isinstance(data["max_documents"], int), "max_documents should be int"
    assert isinstance(data["developer_mode"], bool), "developer_mode should be bool"
    print(f"  Config: max_documents={data['max_documents']}, developer_mode={data['developer_mode']}")
    return data


def test_upload_over_limit():
    """Test that uploading >MAX_DOCUMENTS files is rejected.

    This requires a valid QBSD session. We create one via /api/qbsd/configure,
    then try to upload too many files.
    """
    # Step 1: Create a minimal QBSD session
    config_payload = {
        "query": "test query",
        "docs_path": None,
        "upload_pending": True,
        "max_keys_schema": 10,
        "documents_batch_size": 1,
        "schema_creation_backend": {
            "provider": "openai",
            "model": "gpt-4o-mini",
            "temperature": 0,
        },
        "value_extraction_backend": {
            "provider": "openai",
            "model": "gpt-4o-mini",
            "temperature": 0,
        },
        "output_path": "test_output.json",
    }

    r = requests.post(f"{BASE}/api/qbsd/configure", json=config_payload)
    if r.status_code != 200:
        print(f"  SKIP: Could not create session ({r.status_code}: {r.text[:200]})")
        print("  (This is expected if no API keys are configured)")
        return None

    session_id = r.json()["session_id"]
    print(f"  Created session: {session_id}")

    # Step 2: Create 25 tiny temp files and try to upload them
    config = requests.get(f"{BASE}/api/config").json()
    num_files = config["max_documents"] + 5  # Over the limit

    files = []
    temp_files = []
    for i in range(num_files):
        tf = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        tf.write(f"Test document {i} content".encode())
        tf.close()
        temp_files.append(tf.name)
        files.append(("files", (f"doc_{i}.txt", open(tf.name, "rb"), "text/plain")))

    try:
        r = requests.post(f"{BASE}/api/load/add-documents/{session_id}", files=files)
        print(f"  Upload {num_files} files: status={r.status_code}")

        if r.status_code == 400:
            detail = r.json().get("detail", "")
            print(f"  Correctly rejected: {detail}")
            return True
        else:
            print(f"  UNEXPECTED: Expected 400, got {r.status_code}")
            print(f"  Response: {r.text[:300]}")
            return False
    finally:
        # Close file handles and clean up
        for _, (_, fh, _) in files:
            fh.close()
        for path in temp_files:
            os.unlink(path)


def test_bypass_without_dev_mode():
    """Test that bypass_limit=true is ignored when DEVELOPER_MODE is false."""
    config = requests.get(f"{BASE}/api/config").json()
    if config["developer_mode"]:
        print("  SKIP: DEVELOPER_MODE is true, can't test bypass rejection")
        return None

    # Same as above but with bypass_limit=true
    config_payload = {
        "query": "test bypass",
        "docs_path": None,
        "upload_pending": True,
        "max_keys_schema": 10,
        "documents_batch_size": 1,
        "schema_creation_backend": {
            "provider": "openai",
            "model": "gpt-4o-mini",
            "temperature": 0,
        },
        "value_extraction_backend": {
            "provider": "openai",
            "model": "gpt-4o-mini",
            "temperature": 0,
        },
        "output_path": "test_output.json",
    }

    r = requests.post(f"{BASE}/api/qbsd/configure", json=config_payload)
    if r.status_code != 200:
        print(f"  SKIP: Could not create session ({r.status_code})")
        return None

    session_id = r.json()["session_id"]
    num_files = config["max_documents"] + 5

    files = []
    temp_files = []
    for i in range(num_files):
        tf = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        tf.write(f"Test document {i}".encode())
        tf.close()
        temp_files.append(tf.name)
        files.append(("files", (f"doc_{i}.txt", open(tf.name, "rb"), "text/plain")))

    try:
        # bypass_limit=true should be ignored since DEVELOPER_MODE=false
        r = requests.post(
            f"{BASE}/api/load/add-documents/{session_id}?bypass_limit=true",
            files=files,
        )
        print(f"  Upload {num_files} with bypass_limit=true: status={r.status_code}")

        if r.status_code == 400:
            print("  Correctly rejected (bypass ignored without DEVELOPER_MODE)")
            return True
        else:
            print(f"  UNEXPECTED: bypass should have been ignored, got {r.status_code}")
            return False
    finally:
        for _, (_, fh, _) in files:
            fh.close()
        for path in temp_files:
            os.unlink(path)


if __name__ == "__main__":
    print("=" * 60)
    print("Document Upload Limit - Manual Tests")
    print("=" * 60)

    print("\n1. Config endpoint:")
    try:
        test_config_endpoint()
        print("   PASS")
    except Exception as e:
        print(f"   FAIL: {e}")

    print("\n2. Upload over limit:")
    try:
        result = test_upload_over_limit()
        if result is True:
            print("   PASS")
        elif result is None:
            print("   SKIPPED")
        else:
            print("   FAIL")
    except Exception as e:
        print(f"   FAIL: {e}")

    print("\n3. Bypass without developer mode:")
    try:
        result = test_bypass_without_dev_mode()
        if result is True:
            print("   PASS")
        elif result is None:
            print("   SKIPPED")
        else:
            print("   FAIL")
    except Exception as e:
        print(f"   FAIL: {e}")

    print("\n" + "=" * 60)
