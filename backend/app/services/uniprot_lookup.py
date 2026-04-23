"""Stateless UniProt lookup primitives.

Ported from research/experiments/schematiq/NES_expand/enrichment/enrich_from_uniprot.py.
Uses only Python stdlib (urllib) so no new dependencies are added.

The public surface for callers:
- lookup_protein(name, organism, alt_names) -> (hit|None, tier, error|None)
- parse_hit(hit) -> dict of UniProt enrichment columns
- extract_value(cell) -> plain value from a polymorphic cell
- is_protein_like_unit(unit_name, unit_definition) -> bool
- find_input_columns(row_data) -> (protein_col, organism_col, alt_names_col)
- UNIPROT_SCHEMA_COLUMNS -> [(name, definition, data_type), ...]
"""

import re
import urllib.parse
import urllib.request
from typing import Optional

UNIPROT_SEARCH_URL = "https://rest.uniprot.org/uniprotkb/search"
UNIPROT_FIELDS = "accession,gene_names,cc_subcellular_location,xref_pdb,go_p,go_f,go_c,length,sequence"

# Cell status markers (match the existing _cell_status vocabulary styled by the frontend)
EXTERNAL = "external_source"
NO_CHANGE = "no_change"

# Common organism -> NCBI taxon ID mapping
TAXON_MAP = {
    "homo sapiens": 9606,
    "human": 9606,
    "mus musculus": 10090,
    "mouse": 10090,
    "rattus norvegicus": 10116,
    "rat": 10116,
    "saccharomyces cerevisiae": 559292,
    "baker's yeast": 559292,
    "schizosaccharomyces pombe": 4896,
    "fission yeast": 4896,
    "drosophila melanogaster": 7227,
    "fruit fly": 7227,
    "arabidopsis thaliana": 3702,
    "gallus gallus": 9031,
    "chicken": 9031,
    "xenopus laevis": 8355,
    "xenopus": 8355,
    "caenorhabditis elegans": 6239,
    "canis familiaris": 9615,
    "dog": 9615,
    "sus scrofa": 9823,
    "pig": 9823,
    "macaca mulatta": 9544,
    "aspergillus nidulans": 162425,
    "oryza sativa": 4530,
    "rice": 4530,
    "danio rerio": 7955,
    "zebrafish": 7955,
    "bos taurus": 9913,
    "oryctolagus cuniculus": 9986,
    "rabbit": 9986,
}

VIRAL_KEYWORDS = [
    "virus", "viral", "herpes", "hiv", "htlv", "adenovirus", "papillomavirus",
    "influenza", "sars", "coronavirus", "ebola", "hepatitis", "parvovirus",
    "baculovirus", "torovirus", "retrovirus", "lentivirus", "densovirus",
    "newcastle", "sendai", "paramyxo", "chikungunya", "tilapia",
]

# Columns added by UniProt enrichment, with schema metadata for session registration.
UNIPROT_SCHEMA_COLUMNS = [
    ("uniprot_accession", "UniProt accession ID for the protein.", "string"),
    ("uniprot_url", "Direct URL to the UniProt entry page.", "string"),
    ("gene_symbol", "Primary gene symbol from UniProt (e.g., NPM1, TP53).", "string"),
    ("subcellular_localization", "Subcellular localization annotation from UniProt.", "string"),
    ("protein_length", "Full protein length in amino acids from UniProt.", "number"),
    ("go_biological_process", "Gene Ontology biological process terms from UniProt.", "string"),
    ("go_molecular_function", "Gene Ontology molecular function terms from UniProt.", "string"),
    ("go_cellular_component", "Gene Ontology cellular component terms from UniProt.", "string"),
    ("pdb_ids", "PDB structure IDs cross-referenced from UniProt.", "array"),
    ("alphafold_url", "AlphaFold predicted structure URL.", "string"),
]

UNIPROT_COLUMNS = [c[0] for c in UNIPROT_SCHEMA_COLUMNS]


# Keyword allowlist for observation-unit detection. Conservative by design:
# matches entities UniProt can actually resolve.
PROTEIN_LIKE_UNIT_KEYWORDS = {
    "protein", "proteins", "enzyme", "enzymes", "kinase", "kinases",
    "phosphatase", "phosphatases", "receptor", "receptors",
    "transcription factor", "antibody", "gene product",
    "nes protein", "nuclear export signal",
}


def is_protein_like_unit(unit_name: Optional[str], unit_definition: Optional[str] = None) -> bool:
    """Return True if the observation unit is plausibly resolvable in UniProt.

    Uses a keyword allowlist against the unit name; falls back to the definition
    only when the name misses and the definition strongly implies a protein
    context (mentions 'protein' alongside 'sequence' / 'uniprot' / 'gene').
    """
    if not unit_name:
        return False
    name = unit_name.lower().strip()
    if name in PROTEIN_LIKE_UNIT_KEYWORDS:
        return True
    if any(kw in name for kw in PROTEIN_LIKE_UNIT_KEYWORDS):
        return True
    defn = (unit_definition or "").lower()
    return "protein" in defn and ("sequence" in defn or "uniprot" in defn or "gene" in defn)


def find_input_columns(row: dict):
    """Resolve the row columns carrying protein name, organism, and alt names.

    Handles ScheMatiQ's flat runtime jsonl: cells live at the row top level
    (e.g. `row['protein_name']`) and metadata keys are `_`-prefixed. Matches
    columns by normalized name with exact-preference, falling back to a
    substring match for organism/alt-names (so `protein_species` resolves as
    organism). Returns (protein_col, organism_col, alt_names_col); protein_col
    is None when no protein-identifier column exists, in which case the service
    should skip enrichment.
    """
    cols = [k for k in row.keys() if not str(k).startswith("_")]
    norm = {re.sub(r"[^a-z0-9]", "", str(k).lower()): k for k in cols}

    # Exact-normalized match first. protein_name is kept strict (common English
    # words like 'name' or 'protein' alone would match too many unrelated cols).
    exact_aliases = {
        "protein_name": ["proteinname", "proteinofinterest"],
        "organism": ["organism", "species", "taxon"],
        "alternative_names": ["alternativenames", "altnames", "synonyms", "aliases", "genenames"],
    }
    # Substring fallback for cases like `protein_species`, `host_organism`.
    substr_aliases = {
        "protein_name": [],
        "organism": ["species", "organism", "taxon"],
        "alternative_names": ["synonym", "alias", "altname"],
    }

    def match(target: str) -> Optional[str]:
        for kw in exact_aliases[target]:
            if kw in norm:
                return norm[kw]
        for kw in substr_aliases[target]:
            for n, orig in norm.items():
                if kw in n:
                    return orig
        return None

    p = match("protein_name")
    if not p:
        return None, None, None
    return p, match("organism"), match("alternative_names")


def extract_value(v):
    """Extract plain value from a cell (str, dict with 'answer', list, or None)."""
    if v is None:
        return None
    if isinstance(v, str):
        return v.strip() if v.strip() else None
    if isinstance(v, dict):
        ans = v.get("answer")
        if isinstance(ans, str):
            return ans.strip() if ans.strip() else None
        return ans
    return str(v)


def normalize_organism(organism_field):
    """Extract organism string, taxon id, and viral flag from a polymorphic field."""
    if organism_field is None:
        return None, None, False

    if isinstance(organism_field, str):
        org_str = organism_field.strip()
    elif isinstance(organism_field, list):
        for o in organism_field:
            s = extract_value(o) if isinstance(o, dict) else (o.strip() if isinstance(o, str) else None)
            if s and "human" in s.lower():
                org_str = s
                break
        else:
            org_str = extract_value(organism_field[0]) if organism_field else None
            if not org_str and len(organism_field) > 0:
                org_str = str(organism_field[0]).strip()
    elif isinstance(organism_field, dict):
        org_str = extract_value(organism_field)
    else:
        org_str = str(organism_field).strip()

    if isinstance(org_str, list):
        human = next((o for o in org_str if isinstance(o, str) and "human" in o.lower()), None)
        org_str = human or (org_str[0] if org_str else None)

    if not org_str or not isinstance(org_str, str):
        return None, None, False

    org_lower = org_str.lower()
    is_viral = any(kw in org_lower for kw in VIRAL_KEYWORDS)

    clean = re.sub(r"\s*\(.*?\)\s*", " ", org_str).strip().lower()
    paren_match = re.search(r"\(([^)]+)\)", org_str)
    paren_name = paren_match.group(1).strip().lower() if paren_match else None

    taxon_id = TAXON_MAP.get(clean)
    if taxon_id is None and paren_name:
        taxon_id = TAXON_MAP.get(paren_name)
    if taxon_id is None:
        words = clean.split()
        if len(words) >= 2:
            taxon_id = TAXON_MAP.get(f"{words[0]} {words[1]}")

    return org_str, taxon_id, is_viral


def extract_gene_symbols(alt_names_field):
    """Extract likely gene symbols from an alternative-names cell."""
    raw = extract_value(alt_names_field)
    if not raw:
        return []

    if isinstance(raw, list):
        parts = raw
    else:
        parts = re.split(r"[;,]\s*", str(raw))

    symbols = []
    for part in parts:
        part = str(part).strip()
        if re.match(r"^[A-Z][A-Z0-9]{1,11}$", part):
            symbols.append(part)
    return symbols


def clean_protein_name(name):
    """Remove paper-context suffixes like [paper_name...] from protein names."""
    return re.sub(r"\s*\[.*?\]\s*$", "", name).strip()


def query_uniprot(query_str, fields=UNIPROT_FIELDS, size=5):
    """Query the UniProt REST API. Returns (rows_list, error_msg_or_None)."""
    params = {
        "query": query_str,
        "fields": fields,
        "format": "tsv",
        "size": str(size),
    }
    url = f"{UNIPROT_SEARCH_URL}?{urllib.parse.urlencode(params)}"

    req = urllib.request.Request(url)
    req.add_header("User-Agent", "ScheMatiQ-Enrichment/1.0 (+https://github.com/shaharl6000/QueryDiscovery)")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            text = resp.read().decode("utf-8")
    except Exception as e:
        return [], str(e)

    lines = text.strip().split("\n")
    if len(lines) < 2:
        return [], None

    headers = lines[0].split("\t")
    results = []
    for line in lines[1:]:
        vals = line.split("\t")
        row = dict(zip(headers, vals))
        results.append(row)

    return results, None


def pick_best_hit(hits, protein_name):
    """Pick the best UniProt hit; prefers exact name match, else first."""
    if not hits:
        return None
    if len(hits) == 1:
        return hits[0]

    clean_name = clean_protein_name(protein_name).lower()
    for hit in hits:
        rec_name = hit.get("Protein names", "").lower()
        if clean_name in rec_name:
            return hit
    return hits[0]


def lookup_protein(protein_name, organism_field, alt_names_field):
    """Three-tier UniProt lookup. Returns (hit|None, tier_used, error|None)."""
    if not protein_name:
        return None, "no_input", None

    name = clean_protein_name(str(protein_name))
    if not name:
        return None, "no_input", None

    org_str, taxon_id, is_viral = normalize_organism(organism_field)

    # Tier 1: exact protein name + organism + reviewed
    if taxon_id and not is_viral:
        query = f'protein_name:"{name}" AND organism_id:{taxon_id} AND reviewed:true'
        hits, _err = query_uniprot(query)
        if hits:
            return pick_best_hit(hits, name), "tier1_name", None
        query = f'({name}) AND organism_id:{taxon_id} AND reviewed:true'
        hits, _err = query_uniprot(query, size=3)
        if hits:
            return pick_best_hit(hits, name), "tier1_fuzzy", None

    # Tier 2: gene symbol from alternative_names
    gene_symbols = extract_gene_symbols(alt_names_field)
    if gene_symbols and taxon_id:
        for symbol in gene_symbols[:3]:
            query = f"gene:{symbol} AND organism_id:{taxon_id} AND reviewed:true"
            hits, _err = query_uniprot(query, size=3)
            if hits:
                return pick_best_hit(hits, name), "tier2_gene", None

    # Tier 3: viral fallback
    if is_viral and org_str:
        virus_name = re.sub(r"\s*\(.*?\)\s*", " ", org_str).strip()
        query = f'protein_name:"{name}" AND taxonomy_name:"{virus_name}"'
        hits, _err = query_uniprot(query)
        if hits:
            return pick_best_hit(hits, name), "tier3_viral", None
        if gene_symbols:
            for symbol in gene_symbols[:2]:
                query = f'gene:{symbol} AND taxonomy_name:"{virus_name}"'
                hits, _err = query_uniprot(query, size=3)
                if hits:
                    return pick_best_hit(hits, name), "tier3_viral_gene", None

    # Tier 3b: no-organism fallback
    if not org_str or (not taxon_id and not is_viral):
        query = f'protein_name:"{name}" AND reviewed:true'
        hits, _err = query_uniprot(query, size=3)
        if hits:
            return pick_best_hit(hits, name), "tier3_no_org", None
        if gene_symbols:
            for symbol in gene_symbols[:2]:
                query = f"gene:{symbol} AND reviewed:true"
                hits, _err = query_uniprot(query, size=3)
                if hits:
                    return pick_best_hit(hits, name), "tier3_no_org_gene", None

    return None, "no_match", None


def parse_hit(hit):
    """Parse a UniProt TSV hit into enrichment columns, or None if unparseable."""
    accession = hit.get("Entry", "").strip()
    if not accession:
        return None

    gene_names = hit.get("Gene Names", "").strip()
    gene_symbol = gene_names.split()[0] if gene_names else None

    subcel = hit.get("Subcellular location [CC]", "").strip()
    subcel = re.sub(r"^SUBCELLULAR LOCATION:\s*", "", subcel)
    subcel = subcel if subcel else None

    pdb_raw = hit.get("PDB", "").strip()
    pdb_ids = [p.strip() for p in pdb_raw.split(";") if p.strip()] if pdb_raw else None

    go_p = hit.get("Gene Ontology (biological process)", "").strip() or None
    go_f = hit.get("Gene Ontology (molecular function)", "").strip() or None
    go_c = hit.get("Gene Ontology (cellular component)", "").strip() or None

    length_str = hit.get("Length", "").strip()
    length = int(length_str) if length_str.isdigit() else None

    return {
        "uniprot_accession": accession,
        "uniprot_url": f"https://www.uniprot.org/uniprotkb/{accession}/entry",
        "gene_symbol": gene_symbol,
        "subcellular_localization": subcel,
        "protein_length": length,
        "go_biological_process": go_p,
        "go_molecular_function": go_f,
        "go_cellular_component": go_c,
        "pdb_ids": pdb_ids,
        "alphafold_url": f"https://alphafold.ebi.ac.uk/entry/{accession}",
    }
