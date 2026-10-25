"""Compare a ScheMatiQ NES table with NESdb using regular expressions only - no Gemini calls.

Runs the same pipeline as compare_with_nesdb_v2.py (NESdb loading, row-to-NES matching, UniProt check,
per-NES and per-signal reports, precision/recall), but the free-text columns that v2 hands to Gemini are
scored by rules built from the values NESdb and the extraction actually use:

  * evidence             NESdb records a closed list of evidence kinds (LMB sensitive, binds CRM1, mislocalized
                         in xpo1 mutants, reporter activity with CRM1 co-expression, competition with other CRM1
                         cargos, permeabilized-cell assay, KPT sensitive). Both sides are reduced to the kinds
                         they state - in the vocabulary or in prose ("retained in the nucleus by leptomycin B",
                         "co-immunoprecipitates with CRM1") - and match when they share one. A clause that
                         negates ("no CRM1 binding was detected", "LMB insensitive", "CRM1-independent") states none.
  * secondary structure  helix / strand / loop-turn-coil / disordered / extended; match when they share one.
  * other residues       residue numbers ("S89", "Ser 392", "H46R"), ranges ("516-646") and NESdb's
                         "418 PEPTIDE 492" regions; match when a residue or range overlaps.
  * organism             species by binomial or common name (human, murine, baker's yeast...), from NESdb's own
                         list; viruses by name with type numbers and known synonyms normalised.
  * alternative names    v2's name rules, plus species prefixes (hMLH1, mBmal1), yeast "p" suffixes (Gln3p),
                         names in brackets and gene symbols inside NESdb's run-together names - against NESdb's
                         names and the protein's UniProt names (protein and gene names and synonyms, fetched once
                         by NESdb's accession into uniprot_names.json next to the NESdb cache; --no-uniprot-fetch
                         uses only what is cached). A symbol inside a relation to another protein ("Homologous to
                         SIR2") or another family member (Akt3 for Akt1) does not match.
  * mutations            v2's notations, plus "LL779/781AA", "LSL289ASA", "368A/370A", "Leu-19/21-Ala" and prose
                         ("leucines at positions 83, 87 and 91 to alanine"); match on a shared (position, new residue).

  * NES columns          stricter than v2 (which takes any 8 identical residues in a row, or any overlapping range):
                         a peptide is aligned residue by residue on NESdb's NES and every aligned residue must be
                         identical (--nes-identity 0.9 allows one in ten to differ), over at least 8 aligned
                         residues (or the whole shorter one; a shifted boundary is fine), and, when NESdb has the protein
                         sequence, must sit at NESdb's position in it. A cell with a peptide is judged by the
                         peptide only, so right coordinates don't rescue a wrong peptide. A range alone matches
                         when at least half of it lies on the NES ("438-447" in "321-504", not "112-172" for 149-158).

Which NES a row is about, the protein sequence, UniProt IDs and PMIDs keep v2's rules.

Writes the same files as v2 (see compare_with_nesdb_v2.py), into a "regex" folder next to the CSV unless
--out-dir is given, so v2's results are not overwritten.

Usage (from the repo root):
    .venv\\Scripts\\python.exe backend\\scripts\\compare_with_nesdb_regex.py <export.csv> [--nes-identity 0.9]
        [v2 options except --llm-*]
"""
import json
import re
import sys
from pathlib import Path

import compare_with_nesdb_v2 as v2
from compare_with_nesdb_v2 import has_value, norm

# ---------------------------------------------------------------- evidence

CRM1 = r"(?:crm-?1p?|xpo-?1p?|exportin(?:[- ]?1)?|crmi)"
EVIDENCE_KINDS = {
    "LMB sensitive": re.compile(r"\blmb\b|leptomycin|\blb treatment"),
    "KPT sensitive": re.compile(r"\bkpt\b|kpt-?\d+|selinexor|verdinexor"),
    "binds CRM1": re.compile(
        rf"\bbind\w*\b[^;]{{0,40}}{CRM1}|{CRM1}[^;]{{0,25}}\b(?:binds?|binding|interaction|complex)"
        rf"|\b(?:interact\w*|associat\w*|complex\w*|co-?immunoprecipit\w*|co-?precipit\w*|pull-?down|two-hybrid)"
        rf"\b[^;]{{0,40}}{CRM1}"),
    "mislocalized in xpo1 mutants": re.compile(
        rf"{CRM1}[-\s]?1?\b[^;]{{0,30}}(?:mutant|temperature|\bts\b|strain)|mislocali[sz]\w* in {CRM1}"),
    "reporter activity with CRM1 co-expression": re.compile(
        rf"(?:co-?express\w*|over-?express\w*)[^;]{{0,30}}{CRM1}|{CRM1}[^;]{{0,20}}(?:co-?express\w*|over-?express\w*)"),
    "inhibited by other CRM1 cargos": re.compile(r"other (?:crm1 )?cargo|\bcompet\w*[^;]{0,30}(?:cargo|rev|nes)"),
    "permeabilized-cell assay": re.compile(r"permeabili[sz]\w*|digitonin"),
}
NEGATION_RE = re.compile(r"\b(?:no|not|non|without|failed|insensitive|independent|unaffected|refractory|neither|nor)\b")
CLAUSE_SPLIT_RE = re.compile(r";|\.(?:\s|$)|\bbut\b|\bhowever\b|\bwhereas\b")


def evidence_kinds(text: str) -> set[str]:
    """The evidence kinds the text states. A mention negated close by ("no CRM1 binding", "LMB insensitive",
    "not blocked by LMB") does not count; a negation elsewhere in the sentence does not cancel it."""
    kinds = set()
    for clause in CLAUSE_SPLIT_RE.split(str(text or "").lower().replace("-independent", " independent")):
        for kind, rx in EVIDENCE_KINDS.items():
            if any(not NEGATION_RE.search(clause[max(0, m.start() - 30):m.end() + 15]) for m in rx.finditer(clause)):
                kinds.add(kind)
    return kinds


def cmp_evidence(value, entry, args):
    truth = entry.get("evidence", "")
    expected = evidence_kinds(truth)
    if not has_value(truth) or not expected:
        return truth, None
    return truth, bool(expected & evidence_kinds(value))


# ---------------------------------------------------------------- secondary structure

STRUCTURE_KINDS = {
    "helix": re.compile(r"helix|helices|helical|α|\balpha\b|\ba-hel"),
    "strand": re.compile(r"strand|sheet|β|\bbeta\b|\bb\d\b"),
    "loop": re.compile(r"loop|turn|coil"),
    "disordered": re.compile(r"disorder|unstructured|random coil"),
    "extended": re.compile(r"extended"),
}


def structure_kinds(text: str) -> set[str]:
    t = str(text or "").lower()
    return {kind for kind, rx in STRUCTURE_KINDS.items() if rx.search(t)}


def cmp_structure(value, entry, args):
    truth = entry.get("secondary_structure", "")
    expected = structure_kinds(truth)
    if not has_value(truth) or not expected:
        return truth, None
    return truth, bool(expected & structure_kinds(value))


# ---------------------------------------------------------------- other residues

_AA3 = "|".join(v2.THREE_LETTER)
RESIDUE_RE = re.compile(rf"\b(?:[ACDEFGHIKLMNPQRSTVWY]|(?i:{_AA3})[\s-]?)(\d{{1,4}})(?:[ACDEFGHIKLMNPQRSTVWY])?\b")
REGION_RE = re.compile(r"\b(\d{1,4})\s+[A-Z][A-Z\s]*?\s+(\d{1,4})\b")  # NESdb's "418 LTTGENY... 492"


def residue_spans(text: str) -> list[tuple[int, int]]:
    text = str(text or "")
    spans = [(int(a), int(b)) for a, b in REGION_RE.findall(text)]
    spans += [(int(a), int(b)) for a, b in v2.RANGE_RE.findall(text)]
    spans += [(int(p), int(p)) for p in RESIDUE_RE.findall(text)]
    return [(min(a, b), max(a, b)) for a, b in spans]


def cmp_residues(value, entry, args):
    truth = entry.get("other_residues", "")
    expected = residue_spans(truth)
    if not has_value(truth) or not expected:
        return truth, None
    # A range counts when at least half of it lies on NESdb's residues, so "1-285" does not match "126 VGK 128".
    return truth, any(min(b, d) - max(a, c) + 1 >= (b - a + 1) / 2
                      for a, b in residue_spans(value) for c, d in expected)


# ---------------------------------------------------------------- organism

COMMON_SPECIES = {
    "human": "homo sapiens", "humans": "homo sapiens", "mouse": "mus musculus", "mice": "mus musculus",
    "murine": "mus musculus", "rat": "rattus norvegicus", "rats": "rattus norvegicus",
    "baker s yeast": "saccharomyces cerevisiae", "budding yeast": "saccharomyces cerevisiae",
    "fission yeast": "schizosaccharomyces pombe", "african clawed frog": "xenopus laevis",
    "fruit fly": "drosophila melanogaster", "chicken": "gallus gallus", "zebrafish": "danio rerio",
    "pig": "sus scrofa", "porcine": "sus scrofa", "dog": "canis familiaris", "canine": "canis familiaris",
    "cow": "bos taurus", "bovine": "bos taurus", "rhesus macaque": "macaca mulatta", "american toad": "bufo americanus",
}
VIRUS_SYNONYMS = [  # (pattern, replacement) on norm()ed text
    (r"\bminute virus of mice\b", "murine minute virus"),
    (r"\bpseudorabies virus\b", "suid alphaherpesvirus 1"),
    (r"\bnucleopolyhedrovirus\b", "nuclear polyhedrosis virus"),
    (r"\bsars coronavirus\b|\bsevere acute respiratory syndrome coronavirus\b", "sars cov"),
    (r"\bparainfluenza (\d) virus\b", r"parainfluenza virus \1"),
    (r"\bt cell\b", "t-cell"),
]
ROMAN = {"i": "1", "ii": "2", "iii": "3", "iv": "4", "v": "5"}
_known_species: set[str] = set()


def known_species() -> set[str]:
    """Binomials NESdb uses, plus those the common names stand for."""
    if not _known_species:
        cache = v2.DEFAULT_CACHE
        for e in json.loads(cache.read_text(encoding="utf-8")) if cache.exists() else []:
            words = norm(re.split(r"\(", e.get("organism", ""))[0]).split()
            if len(words) >= 2 and "virus" not in " ".join(words):
                _known_species.add(" ".join(words[:2]))
        _known_species.update(COMMON_SPECIES.values())
    return _known_species


def species(text: str) -> set[str]:
    t = f" {norm(text)} "
    found = {s for s in known_species() if f" {s} " in t}
    found |= {s for s in known_species() if f" {s[0]} {s.split()[1]} " in t}  # "S. cerevisiae"
    found |= {s for name, s in COMMON_SPECIES.items() if f" {name} " in t}
    return found


def virus_text(text: str) -> str:
    t = norm(text)
    for pattern, repl in VIRUS_SYNONYMS:
        t = re.sub(pattern, repl, t)
    t = re.sub(r"\b(type|htlv|hiv|virus) (i{1,3}|iv|v)\b", lambda m: f"{m.group(1)} {ROMAN[m.group(2)]}", t)
    return re.sub(r"\btype (\d)", r"\1", t)


def is_virus(text: str) -> bool:
    return bool(re.search(r"virus|viral|phage|\bhiv\b|\bhtlv\b|\bebv\b|\bhpv\b", norm(text)))


def cmp_organism(value, entry, args):
    truth = entry.get("organism", "")
    if not has_value(truth):
        return truth, None
    if not is_virus(truth):
        expected, stated = species(truth), species(value)
        if expected and stated:  # a value naming a species is right only if it is NESdb's ("Mouse" != Human)
            return truth, bool(expected & stated)
    elif species(value) and not is_virus(value):  # the host ("Human"), not the virus ("Human herpesvirus 1")
        return truth, False
    # A genus alone ("Xenopus"), or a virus: v2's rule, then again with virus names normalised.
    return truth, v2.cmp_organism(value, entry, args)[1] or any(
        virus_text(value) in virus_text(part) or virus_text(part) in virus_text(value)
        for part in re.split(r"[()]", truth) if len(virus_text(part)) >= 4) and bool(virus_text(value))


# ---------------------------------------------------------------- alternative names

def name_keys(text: str) -> set[str]:
    """Spellings of each name in the text: as written, without a species or protein prefix (hMLH1, cZyx, pUL84), without a yeast
    protein's trailing p (Gln3p), and the names in brackets on their own."""
    keys = set()
    for piece in re.split(r"[,;/()\[\]]", str(text or "")):
        piece = piece.strip()
        variants = {piece}
        if re.fullmatch(r"[hmxdyzcp][A-Z0-9][A-Za-z0-9-]+", piece):  # hMLH1, mBmal1, cZyx, pUL84
            variants.add(piece[1:])
        for v in list(variants):
            if re.fullmatch(r"[A-Za-z0-9-]*\d[pP]", v):
                variants.add(v[:-1])
        for v in variants:
            keys |= {k for k in v2.compact_name(v) if len(k) >= 2 and k not in v2.NO_VALUE}
    return keys


# A symbol inside a name that is about another protein: "Homologous to SIR2", "Modulator of KLF7 activity",
# "TAP-like protein", "Rev homolog", "Baculoviral IAP repeat-containing".
RELATION_BEFORE_RE = re.compile(r"\b(?:to|of|for|by|with|against)\s*$", re.I)
RELATION_AFTER_RE = re.compile(r"^(?:-?\s*(?:like|homolog\w*|related|interacting|binding|associated|activity|repeat"
                               r"|target|substrate|regulated|dependent|responsive|family|domain|motif)\b)", re.I)


# A family name followed by the member's number or Greek letter: "MAPKK 1", "MST-1", "aPKC-lambda" name one member,
# so "MAPKK" alone does not match them ("E1A 32 kDa" is a size, not a member).
MEMBER_AFTER_RE = re.compile(r"^[\s-]?(?:\d{1,2}|[α-ω]|alpha|beta|gamma|delta|epsilon|zeta|eta|theta|iota|kappa"
                             r"|lambda)\b(?!\s*kda)", re.I)


def names_protein(symbol: str, text: str) -> bool:
    """The symbol occurs in the text as a word that names the protein, not as part of a relation to another
    and not as the family of a numbered member."""
    for m in re.finditer(rf"(?<![A-Za-z0-9]){re.escape(symbol)}(?![A-Za-z0-9])", text, re.I):
        after = text[m.end():]
        if not (RELATION_BEFORE_RE.search(text[:m.start()]) or RELATION_AFTER_RE.search(after)
                or MEMBER_AFTER_RE.search(after)):
            return True
    return False


# A member number is one digit (Akt1, MEK2, Fbxo7); two or more are a size or a clone (Stat91 = p91, PR61).
SYMBOL_FAMILY_RE = re.compile(r"\b([A-Za-z]{2,})-?(\d)\b")


def family_clash(value: str, nesdb: str) -> bool:
    """The value names another member of the protein's family: Akt3 (or Akt2) for Akt1, ADD2 for ADD1,
    AMPKα1 for AMPKα2."""
    def members(text):
        for g, word in v2.GREEK.items():
            text = text.replace(g, word)
        return {(f.lower(), n) for f, n in SYMBOL_FAMILY_RE.findall(text)}
    ours, theirs = members(value), members(nesdb)
    their_families = {f for f, _ in theirs}
    return any(f in their_families and (f, n) not in theirs for f, n in ours)


SYMBOL_RE = re.compile(r"\b(?=[A-Za-z0-9-]*\d)(?=[A-Za-z0-9-]*[A-Za-z]{2})[A-Za-z][A-Za-z0-9-]{2,}\b")


UNIPROT_NAMES_URL = "https://rest.uniprot.org/uniprotkb/accessions"
UNIPROT_NAMES_CACHE = v2.DEFAULT_CACHE.with_name("uniprot_names.json")
_uniprot_names: dict[str, list[str]] = {}


def names_from_uniprot(record: dict) -> list[str]:
    """Protein names (recommended, alternative, submitted, short, and those of the chains a polyprotein contains)
    and gene names (name, synonyms, ORF and locus names) of one UniProt record."""
    names = []

    def protein(desc):
        for group in [desc.get("recommendedName")] + desc.get("alternativeNames", []) + desc.get("submissionNames", []):
            if group:
                names.append(group.get("fullName", {}).get("value", ""))
                names.extend(s.get("value", "") for s in group.get("shortNames", []))
        for part in desc.get("includes", []) + desc.get("contains", []):
            protein(part)

    protein(record.get("proteinDescription", {}))
    for gene in record.get("genes", []):
        names.append(gene.get("geneName", {}).get("value", ""))
        for key in ("synonyms", "orfNames", "orderedLocusNames"):
            names.extend(s.get("value", "") for s in gene.get(key, []))
    # UniProt names can hold commas ("Nuclear factor, erythroid derived 2, like 1"), which the name rules split on.
    return list(dict.fromkeys(n.replace(",", " ") for n in names if n))


def load_uniprot_names(fetch: bool) -> None:
    """Fill _uniprot_names for every NESdb accession, from the cache and - for the missing ones - from UniProt."""
    import requests

    if UNIPROT_NAMES_CACHE.exists():
        _uniprot_names.update(json.loads(UNIPROT_NAMES_CACHE.read_text(encoding="utf-8")))
    entries = json.loads(v2.DEFAULT_CACHE.read_text(encoding="utf-8")) if v2.DEFAULT_CACHE.exists() else []
    missing = sorted({e["uniprot"].strip().split("-")[0] for e in entries if e.get("uniprot", "").strip()}
                     - set(_uniprot_names))
    if not missing or not fetch:
        return
    print(f"Fetching UniProt names for {len(missing)} accessions")
    for i in range(0, len(missing), 100):
        batch = missing[i:i + 100]
        reply = requests.get(UNIPROT_NAMES_URL, params={"accessions": ",".join(batch), "format": "json",
                                                        "fields": "accession,protein_name,gene_names"}, timeout=60)
        reply.raise_for_status()
        for record in reply.json().get("results", []):
            _uniprot_names[record["primaryAccession"]] = names_from_uniprot(record)
        for acc in batch:  # obsolete or merged accessions: remember that there is nothing to fetch
            _uniprot_names.setdefault(acc, [])
    UNIPROT_NAMES_CACHE.write_text(json.dumps(_uniprot_names, indent=1, ensure_ascii=False), encoding="utf-8")


# A UniProt name about another protein ("Regulatory protein SIR2 homolog 1", "Rev homolog"): fine to equal
# as a whole, but not to look a word up in.
RELATION_NAME_RE = re.compile(r"\b(?:homolog\w*|ortholog\w*|like|related|similar)\b", re.I)


def with_uniprot_names(entry: dict, relations: bool) -> dict:
    """The entry with its protein's UniProt names added to NESdb's alternative names."""
    extra = _uniprot_names.get(entry.get("uniprot", "").strip().split("-")[0], [])
    if not relations:
        extra = [n for n in extra if not RELATION_NAME_RE.search(n)]
    return {**entry, "alt_names": "; ".join([entry.get("alt_names", "")] + extra)} if extra else entry


def cmp_alt_names(value, entry, args):
    truth, hit = v2.cmp_names(value, entry, args)  # NESdb's own names, as the cells file shows them
    nesdb_all = "; ".join([entry.get("full_name", ""), entry.get("name", "")]
                          + with_uniprot_names(entry, True).get("alt_names", "").split("; "))
    if not has_value(value) or family_clash(value, nesdb_all):
        return truth, False if has_value(value) else hit
    if hit or v2.cmp_names(value, with_uniprot_names(entry, False), args)[1]:
        return truth, True
    if name_keys(value) & name_keys(nesdb_all):
        return truth, True
    entry = with_uniprot_names(entry, False)
    nesdb = "; ".join([entry.get("full_name", ""), entry.get("name", ""), entry.get("alt_names", "")])
    # A gene symbol (letters and a digit: MDM2, NMD3) or a symbol-like name (E7, NS2, E1A) standing as a name
    # inside NESdb's names: "HPV16 E7", "NS2-P", "p53-binding protein Mdm2", "Early E1A 32 kDa protein".
    items = [x.strip() for x in re.split(r"[,;/]", value) if x.strip()]
    symbols = set(SYMBOL_RE.findall(value)) | {x for x in items if len(x) >= 2
                                               and re.fullmatch(r"[A-Za-z]*[A-Z0-9][A-Za-z0-9]*", x)}
    if any(names_protein(s, nesdb) for s in symbols):
        return truth, True
    # The same name with a generic word swapped: "DEAD box helicase 17" = "DEAD box protein 17".
    generic = v2.STOPWORDS | v2.GENERIC_NAME_WORDS | {"protein", "helicase"}
    core = [set(norm(x).split()) - generic for x in items]
    nesdb_core = [set(norm(x).split()) - generic for x in re.split(r"[,;/]", nesdb)]
    return truth, any(len(c) >= 2 and c in nesdb_core for c in core)


# ---------------------------------------------------------------- mutations

AA_NAMES = {"alanine": "A", "arginine": "R", "asparagine": "N", "aspartate": "D", "aspartic acid": "D",
            "cysteine": "C", "glutamine": "Q", "glutamate": "E", "glutamic acid": "E", "glycine": "G",
            "histidine": "H", "isoleucine": "I", "leucine": "L", "lysine": "K", "methionine": "M",
            "phenylalanine": "F", "proline": "P", "serine": "S", "threonine": "T", "tryptophan": "W",
            "tyrosine": "Y", "valine": "V"}
_AA_WORD = "|".join(sorted(AA_NAMES, key=len, reverse=True))
_AA_ANY = rf"(?:{_AA_WORD}|{_AA3})"
_ONE = "ACDEFGHIKLMNPQRSTVWY"


def aa_letter(word: str) -> str:
    w = word.lower()
    if w not in AA_NAMES and w not in v2.THREE_LETTER and w.endswith("s"):
        w = w[:-1]
    return AA_NAMES.get(w) or v2.THREE_LETTER.get(w, "")


# Leu400 to alanine, Phe404 to serine
PAIR_RE = re.compile(rf"\b{_AA_ANY}s?[\s-]?(\d{{1,4}})\s*(?:to|->|→|>)\s*(?:an?\s+)?({_AA_ANY})s?\b", re.I)
# Leu-19/21-Ala, Leu 72, 75 to Ala
LIST3_RE = re.compile(rf"\b{_AA_ANY}[\s-]*(\d{{1,4}}(?:\s*(?:/|,|and)\s*\d{{1,4}})+)\s*(?:-|to|→|->)\s*({_AA_ANY})\b", re.I)
# LL779/781AA, LI104/5 to AG
RUN_LIST_RE = re.compile(rf"\b([{_ONE}]{{2,}})(\d{{1,4}}(?:\s*/\s*\d{{1,4}})+)\s*(?:to|->|→)?\s*([{_ONE}]{{2,}})\b")
# LSL289ASA
RUN_RE = re.compile(rf"\b([{_ONE}]{{2,}})(\d{{1,4}})([{_ONE}]{{2,}})\b")
# 368A/370A/441A - only as a slash list, so "293T cells" is not read as a mutation
BARE_LIST_RE = re.compile(rf"(?<![\w])\d{{2,4}}[{_ONE}](?:\s*/\s*\d{{2,4}}[{_ONE}])+\b")
BARE_RE = re.compile(rf"(\d{{2,4}})([{_ONE}])")
# "... to alanine", "alanine substitution of ...", "alanine was substituted for ..."
TARGET_RES = [re.compile(rf"\b(?:to|with|into|by)\s+(?:an?\s+)?({_AA_ANY})s?\b", re.I),
              re.compile(rf"\b({_AA_ANY})s?[\s-]+(?:substitution|substituted|replacement|scan)", re.I),
              re.compile(rf"\b({_AA_WORD})s?\s+(?:was|were)\s+substituted", re.I)]
_ITEM = rf"(?:{_AA_ANY}[\s-]?|[{_ONE}])?\d{{1,4}}"  # 91, L91, Ile90, Leu-91
POSITIONS_RE = re.compile(rf"\b(?:{_AA_ANY}s?|residues?|positions?|amino acids?)(?![a-z])(?:\s+(?:at|residues?|positions?))*"
                          rf"[\s-]*({_ITEM}(?:\s*(?:,\s*(?:and|or)?|/|\band\b|\bor\b|&)\s*{_ITEM})*)", re.I)


def expand(positions: str) -> list[str]:
    """'779/781' -> 779, 781; '104/5' -> 104, 105 (a later number written with only its last digits)."""
    nums = re.findall(r"\d+", positions)
    out = []
    for n in nums:
        if out and len(n) < len(out[0]):
            n = out[0][:len(out[0]) - len(n)] + n
        out.append(n)
    return out


def prose_mutations(text: str) -> set[tuple[str, str]]:
    found = set()
    for pos, new in PAIR_RE.findall(text):
        found.add((pos, aa_letter(new)))
    for positions, new in LIST3_RE.findall(text):
        found |= {(p, aa_letter(new)) for p in expand(positions)}
    for old, positions, new in RUN_LIST_RE.findall(text):
        ps = expand(positions)
        news = new if len(new) == len(ps) else new[0] * len(ps) if len(set(new)) == 1 else ""
        found |= set(zip(ps, news))
    for old, start, new in RUN_RE.findall(text):
        if len(old) == len(new):
            found |= {(str(int(start) + i), b) for i, (a, b) in enumerate(zip(old, new)) if a != b}
    for bare in BARE_LIST_RE.findall(text):
        found |= set(BARE_RE.findall(bare))
    for clause in re.split(r";|\.(?:\s|$)", text):
        if re.search(r"\bdelet", clause, re.I):
            continue
        targets = {aa_letter(m) for rx in TARGET_RES for m in rx.findall(clause)} - {""}
        if len(targets) == 1:
            new = targets.pop()
            for positions in POSITIONS_RE.findall(clause):
                found |= {(p, new) for p in re.findall(r"\d{1,4}", positions)}
    return {(p, n) for p, n in found if n}


def mutation_set(text: str) -> set[tuple[str, str]]:
    return v2_mutation_set(text) | prose_mutations(text or "")


# ---------------------------------------------------------------- NES signals

MIN_IDENTITY = 1.0  # share of aligned residues that must be identical (--nes-identity)


def align(pep: str, nes: str) -> tuple[int, int, int]:
    """Best ungapped alignment of the peptide on NESdb's NES, residue by residue:
    (identical residues, aligned residues, offset of the peptide's first residue in the NES)."""
    best = (0, 0, 0)
    shortest = min(8, len(pep), len(nes))
    if shortest < 8:  # a short NES shifted by a residue ("SHLVLI" on "HLVLIG") aligns over one residue less
        shortest = max(5, shortest - 1)
    for off in range(-len(pep) + shortest, len(nes) - shortest + 1):
        pairs = [(a, nes[i + off]) for i, a in enumerate(pep) if 0 <= i + off < len(nes)]
        same = sum(a == b for a, b in pairs)
        if len(pairs) >= shortest and (not best[1] or same / len(pairs) > best[0] / best[1]
                                       or same / len(pairs) == best[0] / best[1] and same > best[0]):
            best = (same, len(pairs), off)
    return best


def peptide_matches(pep: str, s: dict, protein: str) -> bool:
    """The peptide is this NES: aligned over at least 8 residues (or all of the shorter one) with 90% of them
    identical, and - when the protein sequence is known and the peptide is in it - at NESdb's position."""
    same, aligned, off = align(pep, s["seq"].upper())
    if not aligned or same / aligned < MIN_IDENTITY:
        return False
    places = [m.start() + 1 for m in re.finditer(f"(?={re.escape(pep)})", protein)] if protein else []
    return not places or any(p <= s["end"] and p + len(pep) - 1 >= s["start"] for p in places)


def cell_peptides(value: str) -> list[str]:
    whole = v2.residues(value)
    return [whole] if whole else v2.AA_RE.findall(value.upper())


def cmp_signals(value, entry, args):
    """A cell with a peptide is judged by its peptide alone (right coordinates don't rescue a wrong peptide);
    a cell with only a residue range matches when at least half of that range lies on the NES."""
    signals = v2.entry_signals(entry)
    truth = ", ".join(v2.signal_text(s) for s in signals) or entry.get("signals_text", "")
    if not signals:
        return truth, None
    protein = re.sub(r"\s+", "", entry.get("sequence") or "").upper()
    peptides = cell_peptides(value)
    if peptides:
        return truth, any(peptide_matches(p, s, protein) for p in peptides for s in signals)
    spans = [(min(int(a), int(b)), max(int(a), int(b))) for a, b in v2.RANGE_RE.findall(value)]
    return truth, any(min(b, s["end"]) - max(a, s["start"]) + 1 >= (b - a + 1) / 2 for a, b in spans for s in signals)


# ---------------------------------------------------------------- run

v2_mutation_set = v2.mutation_set
REGEX_COMPARATORS = {
    "alt_names": cmp_alt_names,
    "organism": cmp_organism,
    "evidence": cmp_evidence,
    "secondary_structure": cmp_structure,
    "other_residues": cmp_residues,
    "signals": cmp_signals,
}


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if any(a.startswith("--llm") for a in argv):
        sys.exit("compare_with_nesdb_regex.py makes no LLM calls; use compare_with_nesdb_v2.py for --llm-* options")
    if "--nes-identity" in argv:
        i = argv.index("--nes-identity")
        global MIN_IDENTITY
        MIN_IDENTITY = float(argv[i + 1])
        del argv[i:i + 2]
    if "--out-dir" not in argv and argv and not argv[0].startswith("-"):
        argv += ["--out-dir", str(Path(argv[0]).parent / "regex")]
    load_uniprot_names(fetch="--no-uniprot-fetch" not in argv)
    v2.COMPARATORS.update(REGEX_COMPARATORS)
    v2.mutation_set = mutation_set  # the mutations comparators look it up at call time
    v2.cmp_signals = cmp_signals  # a peptide in a "sequence" column is scored as an NES too
    v2.__doc__ = __doc__
    v2.main(argv)


if __name__ == "__main__":
    main()
