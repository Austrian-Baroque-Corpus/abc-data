#!/usr/bin/env python3
"""
Create a JSON summary of persons (<persName>) and places (<placeName>) across TEI-XML files.

New in this version:
- Also extract the element-level @type for BOTH persons and places.
  For each unique @key, we keep the first non-empty @type observed across the corpus.

For both persons and places:
- "lemma" (display name) is built from @lemma values of descendant <w> elements;
  if none exist, fallback to concatenated <w> text (not normalized).
- "variations" are unique surface forms from concatenated <w> texts.
- Output JSON has two top-level keys: "persons" and "places".
  Each list contains records with: key, type, lemma, TOTAL, files[] (nonzero only), variations.

Example JSON person record:
{
  "key": "pers123",
  "type": "noble",
  "lemma": "Karl von Habsburg",
  "TOTAL": 4,
  "files": [
    {"file": "issue_1898-01-02.xml", "count": 3},
    {"file": "issue_1898-01-09.xml", "count": 1}
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
import xml.etree.ElementTree as ET

import pandas as pd
from acdh_tei_pyutils.tei import TeiReader

# ----------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------

XML_DIR = Path("data/editions")   # folder containing the TEI-XML files
GLOB    = "*.xml"         # pattern for TEI files inside XML_DIR

TERMLABELS_PERSONS = Path("data/index/output/termlabels-persnames.xml")
TERMLABELS_PLACES  = Path("data/index/output/termlabels-placenames.xml")

# ----------------------------------------------------------------------
# FALLBACK LABEL DATA
# Mirrors the content of the termlabels XML files above.
# Used automatically when those files are not found.
# ----------------------------------------------------------------------

_FALLBACK_LABELS_PERSONS = {
    "abrahamaSanctaClara": "Abrahamus a Sancta Clara",
    "abrahamdeSanctaClara": "Abraham de Sancta Clara",
    "absalon": "Absalon (Absolon)",
    "achatiusaSanctusAlexius": "Achatius a Sancto Alexio",
    "adamusaSanctacrux": "Adamus a Sancta Cruce",
    "adrianusdander": "Adrianus II",
    "adrianusdvier": "Adrianus IV",
    "aegidiusaSanctusLeopoldus": "Aegidius a Sancto Leopoldo",
    "albrechtFreiherrvonZimmern": "Albrecht (Freiherr) von Zimmern",
    "alexanderIII": "Alexander III",
    "alexanderMagnus": "Alexander Magnus (Alexander der Große)",
    "alexanderabSanctusMichael": "Alexander a Sancto Michaele",
    "alexanderdI": "Alexander I",
    "alexanderdsechs": "Alexander VI",
    "alexanderdvier": "Alexander IV",
    "ambrosius": "Ambrosius",
    "annaSusannaedelFrauvonPazzoaufHerteneckgeborenGreylinvonRosenstock": "Anna Susanna Edle Frau von Pazzo auff Herteneck",
    "annaaSBartholomäus": "Anna a Sancto Bartholomäo",
    "anselmusaSanctusChristophorus": "Anselmus a Sancto Christophoro",
    "anselmusabSanctaPelagia": "Anselmus a Sancta Pelagia",
    "antoniusabSanctusFranciscusLaicus": "Antonius a Sancto Francisco",
    "assuerus": "Asverus",
    "augustinusaSanctaMonica": "Augustinus a Sancta Monica",
    "augustinusaSanctanativitasbeatusMariavirgo": "Augustinus a Sancta Nativitate B. M. V.",
    "aurelium": "Aurelius",
    "baronius": "Baronius",
    "bajacetes": "Bajazetes (Bajazethes)",
    "balthasardiaz": "Balthasar Diaz",
    "balthasaraSanctusPaulus": "Balthasar a Sancto Paulo",
    "baptistamanni": "Johann Baptista Manni",
    "benedictaabhumilitasChristus": "Benedicta ab humilitate Christi",
    "benedictusdvierzehn": "Benedictus XIV",
    "biga": "Biga[?]",
    "blos": "Blosius",
    "bonfinus": "Bonfin[?]",
    "bonifaciusdI": "Bonifacius I",
    "bonifaciusdneun": "Bonifacius IX",
    "cain": "Cain (Kain)",
    "carlLudwigdheiligrömischReichGrafvonHofkirchen": "Carl Ludwig Reichsgraf von Hofkirchen",
    "carolusabAssumptioneBV": "Carolus ab Assumptione B. V.",
    "carolusdfünf": "Carolus V",
    "casparusabSanctusAngeloCustode": "Casparus a Sancto Angelo",
    "casparusabSanctusJustinus": "Casparus a Sancto Justino",
    "cassianusabSanctusElisäus": "Cassianus a Sancto Elisäo",
    "christophorusIgnatiusedelHerrvonQuarientundRaall": "Christophorus Ignatius Edler Herr von Quarient und Raall",
    "chrysostomos": "Chrysostomos (Chrysostomus)",
    "cœlius": "Cölius",
    "cuspinian": "Cuspinian",
    "clemensI": "Clemens I",
    "clemensaSanctusElzearius": "Clemens a Sancto Elzeario",
    "clemensdsechs": "Clemens VI",
    "cleopatra": "Cleopatra (Kleopatra)",
    "conradBalthasardheiligrömischReichGrafundHerrvonStarenberg": "Conrad Balthasar Reichsgraf von Starenberg",
    "croesus": "Crösus (Krösus)",
    "datan": "Datan",
    "eliasabSanctusJanuarius": "Elias a Sancto Januario",
    "evaBarbaraRechbergeringeborenBamblin": "Eva Barbara Rechbergerin, geb. Bamblin",
    "enoch": "Enoch (Enosch)",
    "felixddrei": "Felix III",
    "ferdinandusddrei": "Ferdinandus III",
    "franzMaximiliandheiligrömischReichGrafvonMollart": "Franz Maximilian Reichsgraf von Mollart",
    "fridericuspulcher": "Fridericus Pulcher",
    "friedrichddrei": "Friedrich III",
    "gabrielli": "Gabriellus",
    "gratianusabSanctaMaria": "Gratianus a Sancta Maria",
    "gregoriusaSanctusJohannes": "Gregorius a Sancto Joanne",
    "gregoriusddreizehn": "Gregorius XIII",
    "gregoriusdneun": "Gregorius IX",
    "gregoriusdvier": "Gregorius IV",
    "gregoriusdvierzehn": "Gregorius XIV",
    "habacuc": "Habakuk (Habacuc)",
    "helenaFreiinvonSchwartzenhorngeborenvonVeldtegg": "Helena Freiin von Schwartzenhorn, geb. von Veldtegg",
    "henricusabSanctaAnna": "Henricus a Sancta Anna",
    "hermanNoltaeus": "Herman Noltäus",
    "hi": "Hi[?]",
    "hironymusJosephabSanctaAnna": "Hironymus Joseph a Sancta Anna",
    "honoriusdander": "Honorius II",
    "honoriusddrei": "Honorius III",
    "aii": "Alexander II",
    "bii": "Bonifacius II",
    "cii": "Clemens II",
    "biii": "Bonifacius III",
    "ciii": "Clemens III",
    "ijob": "Hiob (Job)",
    "innocentiusI": "Innocentius I",
    "innocentiusddritt": "Innocentius III",
    "innocentiusdvier": "Innocentius IV",
    "aiv": "Alexander IV",
    "biv": "Bonifacius IV",
    "jacobMont": "Jacobus Montanus",
    "janNic": "Jan. Nic.[?]",
    "jeremia": "Jeremia (Jeremias)",
    "jesaja": "Jesaja (Jesajas)",
    "johannaBayr=HuberinvonHuebgeborenKreßlinvonGeraltenberg": "Johanna Bayr=Huberin von Hueb, geb. Kreßlin von Geraltenberg",
    "johannIgnatiusSpindlerFreiherrundedelHerrzuWildenstein": "Johann Ignatius Spindler Freyherr zu Wildenstein",
    "johannechius": "Johannes Echius",
    "johannesdTäufer": "Johannes der Täufer (Joannes Baptista)",
    "johannesVolckhardusdheiligrömischReichReichsgrafvonConzin": "Johannes Volckhardus Reichsgraf von Conzin",
    "johannesdeus": "Johannes de Deo",
    "jonas": "Jonas (Jona)",
    "josephusabannuntiatiobeatusMariavirgo": "Josephus ab Annunciatione B. M. V.",
    "judas": "Judas (Judas Iskariot)",
    "juliusCäsar": "Julius Cäsar",
    "ladislausaSanctaPeregrina": "Ladislaus a Sancta Peregrina",
    "laym": "Laym[?]",
    "lucas": "Lucas (Lukas)",
    "luciusddrei": "Lucius III",
    "ludovicusHörnikus": "Ludwig von Hörnigk",
    "marcus": "Marcus (Markus)",
    "mariaElisabethaZorningeborenBüringerin": "Maria Elisabetha Zornin, geb. Büringerin",
    "mariaEuphrosinaOesterreicheringeborenSchmidin": "Maria Euphrosina Oesterreicherin, geb. Schmidin",
    "mariaJulianaPaulingeborenOstermayrin": "Maria Juliana Paulin, geb. Ostermayrin",
    "mariaLuciaTittmanningeringeborenvonMangen": "Maria Lucia Tittmanningerin, geb. von Mangen",
    "susannaSabinaHäiminvonHeilingthalgeborenLerbmannin": "Susanna Sabina Häimin von Heilingthal, geb. Lerbmannin",
    "martiusTurbo": "Martius Turbo",
    "mathäusabSanctusFranciscus": "Mathäus a Sancto Francisco",
    "maximilianusdander": "Maximilianus II",
    "maximilianusderst": "Maximilianus I",
    "maximinusabSanctusSimonStock": "Maximinus a Sancto Simone Stock",
    "michaelhoffman": "Michael Hoffman",
    "monica": "Monica (Monika)",
    "nStockdejus": "N. Stockdejus",
    "nicephorus": "Nicephorus",
    "nicolausddrei": "Nicolaus III",
    "octaviuspanzius": "Octavius Panzius",
    "paulusddrei": "Paulus III",
    "peterBonaventuraedelvonCrololantza": "Peter Bonaventura Edler von Crololantza",
    "piusdfünf": "Pius V",
    "piusdvier": "Pius IV",
    "quintindheiligrömischReichGrafJörger": "Quintin Reichsgraf Jörger",
    "raphaelabSanctusMathäo": "Raphael a Sancto Mathäo",
    "richardusII": "Richardus II",
    "richardusaSanctusPetrus": "Richardus a Sancto Petro",
    "roa": "Roa[?]",
    "rudolphusaSanctaAnna": "Rudolphus a Sancta Anna",
    "rudolphCarlKhazzius": "Rudolph Carl Khazzius",
    "rudolphdander": "Rudolph II",
    "sales": "Sales",
    "sebastian": "Sebastian(us)",
    "severinusaSanctaRegina": "Severinus a Sancta Regina",
    "sidon": "Sidonius",
    "simeon1": "Simeon",
    "simeon2": "Simeon",
    "sixtusSecundus": "Sixtus II",
    "spiridionabSanctusSerapio": "Spiridion a Sancta Serapione",
    "stephanusSecundus": "Stephan II",
    "süleyman": "Soliman",
    "sylvesterdePetraSancta": "Sylvester de Petra Sancta",
    "thecuitis": "Thecuitis",
    "thomasAngelus": "Thomas Angelus",
    "tobit": "Tobit",
    "tom": "Tom.[?]",
    "urbandVIII": "Urbanus VIII",
    "urbanusSeptimus": "Urbanus VII",
    "av": "Alexander V",
    "bv": "Bonifacius V",
    "valentinusaSanctaElisabetha": "Valentinus a Sancta Elisabetha",
    "vatablus": "Vatablus",
    "vespasianus": "Vespasian(us)",
    "victorddrei": "Victor III",
    "wenceslausaSanctusAugustinus": "Wenceslaus a Sancto Augustino",
    "wilhelmJohannAntoniusdheiligrömischReichGrafvonDhaun": "Wilhelm Johann Antonius Reichsgraf von Dhaun",
    "inx": "Innocentius X",
    "inxi": "Innocentius XI",
    "inxii": "Innocentius XII",
}

_FALLBACK_LABELS_PLACES = {
    "undefined": "unbekannt",
    "loau": "Niederösterreich",
    "gali": "Galiläa",
    "wels": "Welschland",
    "swis": "Schweitzerland",
    "mabr": "Mariabrunn",
    "maze": "Mariazell",
    "prug": "Bruck an der Leitha",
    "wrNe": "Wiener Neustadt",
    "galiM": "Galiläisches Meer",
    "nijm": "Nijmegen",
    "ulm": "Ulm",
    "taxa": "Taxa",
    "vienStockimEisen": "Stock im Eisen",
    "vienArsenal-Cordina": "Arsenal-Cordina",
    "vienAugustiner-Cordina": "Augustiner-Cordina",
    "vienBiber-Cordina": "Biber-Cordina",
    "vienBraun-Bastei": "Braun-Bastei",
    "vienBraun-Cordina": "Braun-Cordina",
    "vienBurg-Bastei": "Burg-Bastei",
    "vienBurg-Cordina": "Burg-Cordina",
    "vienDominikaner-Bastei": "Dominikaner-Bastei",
    "vienElend-Bastei": "Elend-Bastei",
    "vienKärntner-Bastei": "Kärntner-Bastei",
    "vienKärntner-Cordina": "Kärntner-Cordina",
    "vienLöwel-Bastei": "Löwel-Bastei",
    "vienLöwel-Cordina": "Löwel-Cordina",
    "vienMelker-Bastei": "Melker-Bastei",
    "vienMönch-Cordina": "Mönch-Cordina",
    "vienNeueBastei": "Neue Bastei",
    "vienNeuesWerk": "Neues Werk",
    "vienSchotten-Cordina": "Schotten-Cordina",
    "vienStubentor-Cordina": "Stubentor-Cordina",
    "vienWasserkunst-Bastei": "Wasserkunst-Bastei",
    "vienZwölfApostel": "Zwölf Apostel",
    "vienAlterFleischmarckt": "Alter Fleischmarckt",
    "vienNeuerMarkt": "Neuer Markt",
}

# ----------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------

def xpath_tag(tag: str) -> str:
    """Namespace-agnostic XPath for a given tag name (e.g., 'persName')."""
    return f'//*[local-name()="{tag}"]'

# XPath snippets to locate <w> descendants
XPATH_W_NODES  = './/*[local-name()="w"]'
XPATH_W_TEXTS  = './/*[local-name()="w"]/text()'

# Regex for normalizing whitespace
_space_re = re.compile(r"\s+")

def collapse_spaces(s: str) -> str:
    """Collapse multiple spaces/tabs/newlines into a single space and trim edges."""
    return _space_re.sub(" ", s).strip()

def clean_lemma(s: str) -> str:
    """Remove '(span) ' prefix from lemma strings."""
    return s.replace("(span) ", "")

def build_lemma_from_w(el) -> str:
    """
    Build the 'lemma' string for a persName/placeName element:
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
    Example: <persName><w>Karl</w><w>von</w><w>Habsburg</w></persName> → "Karl von Habsburg"
    """
    w_texts = el.xpath(XPATH_W_TEXTS)
    if not w_texts:
        return ""
    return collapse_spaces(" ".join([t for t in w_texts if t is not None]))

def load_termlabels(path: Path, fallback: dict | None = None) -> dict:
    """
    Parse a termlabels XML file and return a dict mapping key -> label string.
    Expected structure: <termLib><set><terms><term key="...">label</term>...
    If the file does not exist, returns the fallback dict (or empty dict if none given).
    """
    if not path.exists():
        if fallback is not None:
            print(f"  {path} not found — using built-in fallback labels.")
        return fallback.copy() if fallback else {}
    root = ET.parse(path).getroot()
    return {
        term.get("key"): (term.text or "").strip()
        for term in root.iter("term")
        if term.get("key") and term.text
    }


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
        local_key_to_type: dict mapping key -> first non-empty element-level @type
    """
    doc = TeiReader(str(xml_path))
    hits = doc.tree.xpath(xpath_tag(tag))

    counts = Counter()
    local_key_to_lemma = {}
    local_variations = defaultdict(set)
    local_key_to_type = {}

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

        # Capture the element's @type (first non-empty encountered per file)
        t = el.get("type")
        if t and (key not in local_key_to_type):
            local_key_to_type[key] = t

    return counts, local_key_to_lemma, local_variations, local_key_to_type

def assemble_records(
    files,
    per_file_counts,
    key_to_lemma,
    key_to_variations,
    key_to_type=None,   # dict for type strings (persons/places), or None to omit
    key_to_label=None,  # dict for display labels from termlabels, or None to omit
):
    """
    Assemble collected data into JSON records for one entity type (persons/places).

    Parameters:
        files: list of Path objects for all input files
        per_file_counts: dict[file_name -> Counter(key -> count)]
        key_to_lemma: dict[key -> lemma string]
        key_to_variations: dict[key -> set of variation strings]
        key_to_type: dict[key -> type string] (optional; if provided, included in output)
        key_to_label: dict[key -> label string] from termlabels (optional)

    Returns:
        List of dicts, each representing one entity with fields:
        - key
        - type        # included only if key_to_type is provided
        - label       # included only if key_to_label is provided and key has a label
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
    df.insert(1, "lemma", df["key"].map(lambda k: clean_lemma(key_to_lemma.get(k)) if key_to_lemma.get(k) is not None else ""))

    # Compute TOTAL counts across all files
    df["TOTAL"] = df.select_dtypes("number").sum(axis=1)

    # Sort by TOTAL desc, then lemma, then key (stable/human-friendly)
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
        # Include type if provided (persons and places now)
        if key_to_type is not None:
            rec["type"] = key_to_type.get(k, "")

        # Include label if provided and present for this key
        if key_to_label is not None:
            label = key_to_label.get(k, "")
            if label:
                rec["label"] = label

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

    # Load termlabels (key -> display label); fall back to built-in dicts if files absent
    labels_persons = load_termlabels(TERMLABELS_PERSONS, fallback=_FALLBACK_LABELS_PERSONS)
    labels_places  = load_termlabels(TERMLABELS_PLACES,  fallback=_FALLBACK_LABELS_PLACES)
    print(f"Loaded {len(labels_persons)} person labels, {len(labels_places)} place labels")

    # --- Collect data for persons ---
    per_file_counts_persons = {}
    key_to_lemma_persons = {}
    key_to_variations_persons = defaultdict(set)
    key_to_type_persons = {}

    # --- Collect data for places ---
    per_file_counts_places = {}
    key_to_lemma_places = {}
    key_to_variations_places = defaultdict(set)
    key_to_type_places = {}

    # Process each file once, extract both persName and placeName
    for f in files:
        # Persons
        p_counts, p_k2l, p_vars, p_k2t = count_keys_in_file(f, "persName")
        per_file_counts_persons[f.name] = p_counts
        for k, l in p_k2l.items():
            if k not in key_to_lemma_persons and l:
                key_to_lemma_persons[k] = l
        for k, vs in p_vars.items():
            key_to_variations_persons[k].update(vs)
        for k, t in p_k2t.items():
            # keep the first non-empty type observed globally for this key
            if k not in key_to_type_persons and t:
                key_to_type_persons[k] = t

        # Places
        pl_counts, pl_k2l, pl_vars, pl_k2t = count_keys_in_file(f, "placeName")
        per_file_counts_places[f.name] = pl_counts
        for k, l in pl_k2l.items():
            if k not in key_to_lemma_places and l:
                key_to_lemma_places[k] = l
        for k, vs in pl_vars.items():
            key_to_variations_places[k].update(vs)
        for k, t in pl_k2t.items():
            if k not in key_to_type_places and t:
                key_to_type_places[k] = t

    # Assemble final records (include 'type' for both)
    persons_records = assemble_records(
        files,
        per_file_counts_persons,
        key_to_lemma_persons,
        key_to_variations_persons,
        key_to_type=key_to_type_persons,
        key_to_label=labels_persons,
    )
    places_records  = assemble_records(
        files,
        per_file_counts_places,
        key_to_lemma_places,
        key_to_variations_places,
        key_to_type=key_to_type_places,
        key_to_label=labels_places,
    )

    # Wrap in top-level dict
    out = {"persons": persons_records, "places": places_records}

    # Write JSON to file
    out_path = Path("data/index/output/abacus_index-personsplaces.json")
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(
        f"Wrote persons={len(persons_records)}, places={len(places_records)} "
        f"to {out_path.resolve()}"
    )

if __name__ == "__main__":
    main()