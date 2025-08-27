#!/usr/bin/env python3
"""
Render two XML index files (persons and places) from a JSON file using Jinja2.

Input JSON structure (per entity):
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

Templates (external files, stored in TEMPLATES_DIR):
  - persons.xml.jinja
  - places.xml.jinja

Each template loops over "objects" (list of entities).
Example loop inside the Jinja template for persons:

{% for x in objects %}
  <person xml:id="{{ x.key }}">
    <persName type="main">{{ x.lemma }}</persName>
    {% for y in x.variations %}
    <persName type="variation">{{ y }}</persName>
    {% endfor %}
    {% for z in x.files %}
    <count type="file" file="{{ z.file }}">
      {{ z.count }} {% if z.count == 1 %}mention{% else %}mentions{% endif %}
    </count>
    {% endfor %}
    <count type="total">{{ x.totalcount }}</count>
  </person>
{% endfor %}

Outputs:
  - abc_register_persons.xml
  - abc_register_places.xml

Requirements:
  pip install jinja2
"""

from pathlib import Path
import json
from jinja2 import Environment, FileSystemLoader
from slugify import slugify

# ----------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------

INPUT_JSON   = Path("data/index/output/abc_register-personsplaces.json")
TEMPLATES_DIR = Path("data/index/templates")
PERSONS_TEMPLATE_FILE = "persons.xml.jinja"
PLACES_TEMPLATE_FILE  = "places.xml.jinja"
OUT_PERSONS = Path("data/index/output/abc_register_persons.xml")
OUT_PLACES  = Path("data/index/output/abc_register_places.xml")

# ----------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------

def load_data(path: Path) -> dict:
    """Load JSON data from disk into a Python dict."""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def normalize_records(records: list[dict]) -> list[dict]:
    """
    Normalize JSON records into a shape that's convenient for Jinja templates.
    
    Input (from JSON):
      {
        "key": "pers123",
        "lemma": "Karl von Habsburg",
        "TOTAL": 4,
        "files": [{"file": "file1.xml", "count": 3}],
        "variations": ["Karl v. Habsburg"]
      }

    Output (for Jinja):
      {
        "key": "pers123",
        "lemma": "Karl von Habsburg",
        "variations": ["Karl v. Habsburg"],
        "files": [{"file": "file1.xml", "count": 3}],
        "totalcount": 4
      }
    """
    out = []
    for r in records:
        out.append({
            "key": r.get("key", ""),
            "key2": slugify(r.get("key","")),
            "lemma": r.get("lemma", ""),
            "variations": r.get("variations", []) or [],
            "files": r.get("files", []) or [],
            "totalcount": r.get("TOTAL", 0),
        })
    return out

def render_to_file(template_name: str, objects: list[dict], out_path: Path) -> None:
    """
    Render a given Jinja template with a list of objects and write the result to disk.
    
    Parameters:
        template_name: filename of the template (inside TEMPLATES_DIR)
        objects: list of normalized records (passed as 'objects' into the template)
        out_path: output file path for the rendered XML
    """
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=False,       # don't escape XML characters
        trim_blocks=True,       # strip the first newline after a block
        lstrip_blocks=True,     # strip leading spaces inside blocks
    )
    template = env.get_template(template_name)
    xml = template.render(objects=objects)
    out_path.write_text(xml, encoding="utf-8")
    print(f"Wrote {out_path.resolve()}")

# ----------------------------------------------------------------------
# MAIN SCRIPT
# ----------------------------------------------------------------------

def main():
    # Load full JSON structure
    data = load_data(INPUT_JSON)

    # Split persons and places
    persons_raw = data.get("persons", [])
    places_raw  = data.get("places",  [])

    # Normalize records for Jinja
    persons = normalize_records(persons_raw)
    places  = normalize_records(places_raw)

    # Render output files
    render_to_file(PERSONS_TEMPLATE_FILE, persons, OUT_PERSONS)
    render_to_file(PLACES_TEMPLATE_FILE, places, OUT_PLACES)

if __name__ == "__main__":
    main()