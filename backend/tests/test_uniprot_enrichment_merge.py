"""UniProt enrichment merges its results into the current file under the session
write lock, so edits made during the slow lookups are kept and persisted."""

import importlib
import json
from pathlib import Path

from app.services import session_data_store

# app.services exposes a service *instance* under the module's name.
ues = importlib.import_module("app.services.uniprot_enrichment_service")


def _write(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


def _read(path: Path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


async def test_edit_made_during_lookup_survives_and_result_is_persisted(tmp_path, monkeypatch):
    data = tmp_path / "data.jsonl"
    _write(data, [
        {"_row_name": "r1", "_source_document": "d1", "protein_name": "NPM1", "note": "old"},
        {"_row_name": "r2", "_source_document": "d2", "protein_name": "TP53", "note": "old"},
    ])

    def fake_lookup(protein, organism, alt_names):
        if protein == "NPM1":
            # A user edits another cell and renames r2's protein mid-run.
            rows = _read(data)
            rows[0]["note"] = "edited"
            rows[1]["protein_name"] = "BRCA1"
            _write(data, rows)
        return ({"acc": protein}, "tier", None)

    persisted = []

    async def fake_persist(session_id, local_path, storage=None):
        persisted.append(Path(local_path))

    monkeypatch.setattr(ues, "lookup_protein", fake_lookup)
    monkeypatch.setattr(ues, "parse_hit", lambda hit: {"uniprot_accession": "ACC-" + hit["acc"]})
    monkeypatch.setattr(session_data_store, "persist_session_data_file", fake_persist)

    svc = ues.UniProtEnrichmentService.__new__(ues.UniProtEnrichmentService)
    svc._ensure_columns_on_session = lambda session_id: None
    await svc._enrich_rows("s-uniprot", data)

    r1, r2 = _read(data)
    assert r1["note"] == "edited"  # concurrent edit kept
    assert r1["uniprot_accession"] == "ACC-NPM1"
    # r2's protein changed after its lookup started: left for the next run.
    assert r2["protein_name"] == "BRCA1"
    assert not r2.get("uniprot_accession")
    assert persisted == [data]
