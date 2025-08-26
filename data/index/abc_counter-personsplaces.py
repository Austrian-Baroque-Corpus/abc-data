#!/usr/bin/env python3
"""
Count how often each <persName> and <placeName> (by @key) occurs per TEI-XML file.

For both persons and places:
- "lemma" (display name) is built from the @lemma values of descendant <w> elements
  within <persName>/<placeName>. If no @lemma is present on any descendant <w>,
  fallback to concatenated <w> texts (not normalized).
- "variations" are distinct surface forms from concatenated <w> texts (deduplicated).
- Output JSON has two top-level keys: "persons" and "places".
  Each is a list of records with: key, lemma, TOTAL, files[] (only nonzero), variations.

Example JSON record:
{
  "key": "pers123",
  "lemma": "Karl von Habsburg",
  "TOTAL": 4,
  "files": [
    {"file": "file1.xml", "count": 3},
    {"file": "file2.xml", "count": 1}
  ],
  "variations": ["Karl v. Habsburg", "Karl von Habsburg"]
}

Requirements:
  pip install acdh-tei-pyutils pandas lxml
"""

from collections import Counter, defaultdict
from pathlib import Path
import re
import json

import pandas as pd
from acdh_tei_pyutils.tei import TeiReader

# ----------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------

XML_DIR = Path("data/editions")   # folder containing the TEI-XML files
GLOB    = "*.xml"         # file pattern for TEI files inside XML_DIR

# ----------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------

def xpath_tag(tag: str) -> str:
    """Return a namespace-agnostic XPath for a given tag name (e.g. 'persName')."""
    return f'//*[local-name()="{tag}"]'

# XPath snippets to locate <w> descendants
XPATH_W_NODES  = './/*[local-name()="w"]'
XPATH_W_TEXTS  = './/*[local-name()="w"]/text()'

# Regex for normalizing whitespace
_space_re = re.compile(r"\s+")

def collapse_spaces(s: str) -> str:
    """Collapse multiple spaces/tabs/newlines into a single space and trim edges."""
    return _space_re.sub(" ", s).strip()

def build_lemma_from_w(el) -> str:
    """
    Build the 'lemma' for a persName/placeName element:
    - Prefer concatenating all @lemma attributes of descendant <w>.
    - Fallback: concatenate surface text of descendant <w>.
    """
    w_nodes = el.xpath(XPATH_W_NODES)
    w_lemmas = [w.get("lemma") for w in w_nodes if w.get("lemma")]
    if w_lemmas:
        return collapse_spaces(" ".join(w_lemmas))
    w_texts = el.xpath(XPATH_W_TEXTS)
    if w_texts:
        return collapse_spaces(" ".join([t for t in w_texts if t is not None]))
    return ""

def surface_from_w(el) -> str:
    """
    Construct a surface-form string (variation) from the text content of descendant <w>.
    Example: <persName><w>Karl</w> <w>von</w> <w>Habsburg</w></persName> → "Karl von Habsburg"
    """
    w_texts = el.xpath(XPATH_W_TEXTS)
    if not w_texts:
        return ""
    return collapse_spaces(" ".join([t for t in w_texts if t is not None]))

def count_keys_in_file(xml_path: Path, tag: str):
    """
    Parse a TEI file and count occurrences of <persName> or <placeName>.
    
    Parameters:
        xml_path: Path to TEI-XML file
        tag: 'persName' or 'placeName'
    
    Returns:
        counts: Counter mapping key -> frequency in this file
        local_key_to_lemma: dict mapping key -> first lemma seen for that key in this file
        local_variations: dict mapping key -> set of surface-form variations
    """
    doc = TeiReader(str(xml_path))
    hits = doc.tree.xpath(xpath_tag(tag))

    counts = Counter()
    local_key_to_lemma = {}
    local_variations = defaultdict(set)

    for el in hits:
        key = el.get("key")
        if not key:
            # Skip entities without @key (not uniquely identifiable)
            continue

        # Count how many times this key appears
        counts[key] += 1

        # Build lemma from <w @lemma> or <w> text
        lemma = build_lemma_from_w(el)
        if key not in local_key_to_lemma and lemma:
            local_key_to_lemma[key] = lemma

        # Add surface variation
        surface = surface_from_w(el)
        if surface:
            local_variations[key].add(surface)

    return counts, local_key_to_lemma, local_variations

def assemble_records(files, per_file_counts, key_to_lemma, key_to_variations):
    """
    Assemble all collected data into JSON records for one entity type (persons/places).
    
    Parameters:
        files: list of Path objects for all input files
        per_file_counts: dict[file_name -> Counter(key -> count)]
        key_to_lemma: dict[key -> lemma string]
        key_to_variations: dict[key -> set of variation strings]
    
    Returns:
        List of dicts, each representing one entity with fields:
        - key
        - lemma
        - TOTAL
        - files (list of {"file", "count"} only for nonzero counts)
        - variations
    """
    # Gather all unique keys across all files
    all_keys = set()
    for counts in per_file_counts.values():
        all_keys.update(counts.keys())

    if not all_keys:
        return []

    # Build DataFrame: rows = keys, cols = filenames
    file_names = [f.name for f in files]
    df = pd.DataFrame(index=sorted(all_keys), columns=file_names, dtype="Int64")

    # Fill counts per file
    for fname, counts in per_file_counts.items():
        s = pd.Series(counts, name=fname, dtype="Int64")
        df[fname] = s.reindex(df.index).fillna(0).astype("Int64")

    # Insert key and lemma columns
    df.insert(0, "key", df.index)
    df.insert(1, "lemma", df["key"].map(lambda k: key_to_lemma.get(k) if key_to_lemma.get(k) is not None else ""))

    # Compute TOTAL counts across all files
    df["TOTAL"] = df.select_dtypes("number").sum(axis=1)

    # Sort by TOTAL desc, then lemma, then key
    df = df.sort_values(by=["TOTAL", "lemma", "key"], ascending=[False, True, True])

    # Convert numeric columns to ints
    count_cols = [c for c in df.columns if c not in {"key", "lemma"}]
    df[count_cols] = df[count_cols].fillna(0).astype(int)

    # Build final list of records
    records = []
    for k, row in df.iterrows():
        rec = {
            "key": row["key"],
            "lemma": row["lemma"],
            "TOTAL": int(row["TOTAL"]),
        }
        # Include only nonzero file counts
        files_list = [
            {"file": col, "count": int(row[col])}
            for col in file_names if int(row[col]) > 0
        ]
        rec["files"] = files_list
        rec["variations"] = sorted(key_to_variations.get(k, set()))
        records.append(rec)

    return records

# ----------------------------------------------------------------------
# MAIN SCRIPT
# ----------------------------------------------------------------------

def main():
    files = sorted(XML_DIR.glob(GLOB))
    if not files:
        raise SystemExit(f"No XML files found in {XML_DIR.resolve()} matching {GLOB}")

    # --- Collect data for persons ---
    per_file_counts_persons = {}
    key_to_lemma_persons = {}
    key_to_variations_persons = defaultdict(set)

    # --- Collect data for places ---
    per_file_counts_places = {}
    key_to_lemma_places = {}
    key_to_variations_places = defaultdict(set)

    # Process each file once, extract both persName and placeName
    for f in files:
        # Persons
        p_counts, p_k2l, p_vars = count_keys_in_file(f, "persName")
        per_file_counts_persons[f.name] = p_counts
        for k, l in p_k2l.items():
            if k not in key_to_lemma_persons and l:
                key_to_lemma_persons[k] = l
        for k, vs in p_vars.items():
            key_to_variations_persons[k].update(vs)

        # Places
        pl_counts, pl_k2l, pl_vars = count_keys_in_file(f, "placeName")
        per_file_counts_places[f.name] = pl_counts
        for k, l in pl_k2l.items():
            if k not in key_to_lemma_places and l:
                key_to_lemma_places[k] = l
        for k, vs in pl_vars.items():
            key_to_variations_places[k].update(vs)

    # Assemble final records
    persons_records = assemble_records(files, per_file_counts_persons, key_to_lemma_persons, key_to_variations_persons)
    places_records  = assemble_records(files, per_file_counts_places,  key_to_lemma_places,  key_to_variations_places)

    # Wrap in top-level dict
    out = {"persons": persons_records, "places": places_records}

    # Write JSON to file
    out_path = Path("data/index/output/abc_register-personsplaces.json")
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(
        f"Wrote persons={len(persons_records)}, places={len(places_records)} "
        f"to {out_path.resolve()}"
    )

if __name__ == "__main__":
    main()