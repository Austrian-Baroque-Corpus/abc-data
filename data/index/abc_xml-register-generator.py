#!/usr/bin/env python3
"""
Render two XML index files (persons and places) from entity_counts_by_file.json using Jinja2.

New in this version:
- Persons and places are grouped by their 'type' and sorted by type.
- Each type becomes its own nested list (<listPerson> or <listPlace>) inside the main list.

Input JSON (per entity) must include:
  key, lemma, TOTAL, files[], variations[], type

Templates (external files in TEMPLATES_DIR):
  - persons.xml.jinja
  - places.xml.jinja

Outputs:
  - index_persons.xml
  - index_places.xml

Requirements:
  pip install jinja2
"""

from pathlib import Path
import json
from collections import defaultdict
from jinja2 import Environment, FileSystemLoader
from slugify import slugify

# ----------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------

INPUT_JSON   = Path("abc-data/data/index/output/abc_register-personsplaces.json")
TEMPLATES_DIR = Path("abc-data/data/index/templates")
PERSONS_TPL    = "persons.xml.jinja"
PLACES_TPL     = "places.xml.jinja"
OUT_PERSONS = Path("abc-data/data/index/output/abc_register_persons.xml")
OUT_PLACES  = Path("abc-data/data/index/output/abc_register_places.xml")

# ----------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------

def load_data(path: Path) -> dict:
    """Load JSON into dict."""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def normalize_records(records: list[dict]) -> list[dict]:
    """
    Normalize JSON records to a consistent shape for Jinja.
    Ensures a 'type' bucket exists (falls back to 'unspecified' if missing/empty).
    """
    out = []
    for r in records:
        entity_type = (r.get("type") or "").strip() or "unspecified"
        out.append({
            "key": r.get("key", ""),
            "key2": slugify(r.get("key","")),
            "lemma": r.get("lemma", ""),
            "variations": r.get("variations", []) or [],
            "files": r.get("files", []) or [],
            "totalcount": r.get("TOTAL", 0),
            "type": entity_type,
        })
    return out

def group_by_type(objects: list[dict]) -> list[dict]:
    """
    Group normalized records by x['type'].
    Returns a sorted list of buckets:
      [
        {"type": "noble", "objects": [ ... ]},
        {"type": "royalty", "objects": [ ... ]},
        {"type": "unspecified", "objects": [ ... ]},
      ]
    Within each bucket, entities keep their original order (which normally
    reflects the JSON generator’s sorting by TOTAL, lemma, key).
    """
    buckets = defaultdict(list)
    for x in objects:
        buckets[x["type"]].append(x)
    # Sort buckets by type name (case-insensitive)
    ordered = []
    for t in sorted(buckets.keys(), key=lambda s: s.casefold()):
        ordered.append({"type": t, "objects": buckets[t]})
    return ordered

def render_to_file(template_name: str, *, groups: list[dict], out_path: Path) -> None:
    """
    Render a template with grouped data.
    Template context:
      - groups: list of {"type": str, "objects": [entities]}
    """
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template(template_name)
    xml = template.render(groups=groups)
    out_path.write_text(xml, encoding="utf-8")
    print(f"Wrote {out_path.resolve()}")

# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------

def main():
    data = load_data(INPUT_JSON)

    # Persons / Places raw
    persons_raw = data.get("persons", [])
    places_raw  = data.get("places",  [])

    # Normalize
    persons_norm = normalize_records(persons_raw)
    places_norm  = normalize_records(places_raw)

    # Group by type (sorted buckets)
    persons_groups = group_by_type(persons_norm)
    places_groups  = group_by_type(places_norm)

    # Render
    render_to_file(PERSONS_TPL, groups=persons_groups, out_path=OUT_PERSONS)
    render_to_file(PLACES_TPL,  groups=places_groups,  out_path=OUT_PLACES)

if __name__ == "__main__":
    main()