
"""
Full pipeline: PDF extraction → chunking → triple extraction → Neo4j ingestion
"""
import os, re, json, time
from pathlib import Path
from tqdm import tqdm
from collections import Counter
from src.config import (
    OPENAI_API_KEY, MODEL_NAME, NEO4J_URI, NEO4J_USER,
    NEO4J_PASSWORD, NEO4J_DATABASE, CHUNK_SIZE, CHUNK_OVERLAP
)

# ── Paths ────────────────────────────────────────────────────────────────────
BASE        = Path("../data/text")
PDF_ROOT    = BASE / "city resilience plans PUBLIC"
TEXT_DIR    = Path("./data/extracted_texts")
CHUNK_DIR   = Path("./data/chunks")
TRIPLET_DIR = Path("./data/triplets")
for d in [TEXT_DIR, CHUNK_DIR, TRIPLET_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Ontology ─────────────────────────────────────────────────────────────────
ONTOLOGY = {
    "node_types": [
        {"label": "City",             "props": ["name", "administrative_level", "climate_zone", "population", "gdp_per_capita", "region", "is_case_study"]},
        {"label": "UrbanZone",        "props": ["zone_type", "area_km2", "population_density", "land_use_type"]},
        {"label": "Infrastructure",   "props": ["infra_type", "infra_color", "capacity", "condition", "service_coverage"]},
        {"label": "ExposureUnit",     "props": ["population_count", "asset_value", "vulnerable_ratio", "social_capital_index"]},
        {"label": "ClimateHazard",    "props": ["hazard_type", "frequency", "severity", "trend", "return_period", "spatial_extent"]},
        {"label": "Vulnerability",    "props": ["vuln_type", "exposure_score", "sensitivity_score", "adaptive_capacity_score", "affected_group"]},
        {"label": "Actor",            "props": ["name", "sector", "role", "authority_level", "resources", "is_bridge_organization"]},
        {"label": "Policy",           "props": ["policy_name", "policy_type", "level", "year_issued", "year_expired", "legal_binding"]},
        {"label": "Mechanism",        "props": ["mechanism_type", "source_of_funding", "legal_basis", "scale_usd"]},
        {"label": "Constraint",       "props": ["constraint_type", "severity_score", "affected_stakeholder", "is_structural"]},
        {"label": "AdaptationAction", "props": ["action_name", "status", "spatial_scale", "adaptation_type", "start_year", "end_year", "cost_usd", "co_benefits"]},
        {"label": "Outcome",          "props": ["outcome_type", "indicator", "value", "unit", "is_co_benefit", "evidence_quality"]},
        {"label": "Indicator",        "props": ["indicator_name", "unit", "baseline", "target", "data_source"]},
        {"label": "ResilienceState",  "props": ["resilience_type", "measurement", "recovery_time", "absorption_capacity", "transformation_capacity"]},
        {"label": "TimePoint",        "props": ["year", "period", "policy_cycle"]},
    ],
    "relation_types": [
        {"type": "HAS_ZONE",            "from": "City",             "to": "UrbanZone",        "rel_props": []},
        {"type": "HAS_INFRASTRUCTURE",  "from": "UrbanZone",        "to": "Infrastructure",   "rel_props": []},
        {"type": "SERVES",              "from": "Infrastructure",   "to": "ExposureUnit",     "rel_props": ["service_level"]},
        {"type": "EXPERIENCES",         "from": "City",             "to": "ClimateHazard",    "rel_props": []},
        {"type": "AFFECTS_ZONE",        "from": "ClimateHazard",    "to": "UrbanZone",        "rel_props": ["impact_severity"]},
        {"type": "EXPOSES",             "from": "ClimateHazard",    "to": "ExposureUnit",     "rel_props": []},
        {"type": "WORSENS",             "from": "ClimateHazard",    "to": "Vulnerability",    "rel_props": []},
        {"type": "EXPERIENCES_VULN",    "from": "ExposureUnit",     "to": "Vulnerability",    "rel_props": []},
        {"type": "ISSUED_BY",           "from": "Policy",           "to": "Actor",            "rel_props": []},
        {"type": "MANDATES",            "from": "Policy",           "to": "AdaptationAction", "rel_props": []},
        {"type": "IMPLEMENTS",          "from": "Actor",            "to": "AdaptationAction", "rel_props": []},
        {"type": "PARTICIPATES_IN",     "from": "Actor",            "to": "AdaptationAction", "rel_props": []},
        {"type": "COORDINATES_WITH",    "from": "Actor",            "to": "Actor",            "rel_props": []},
        {"type": "REPORTS_TO",          "from": "Actor",            "to": "Actor",            "rel_props": []},
        {"type": "MANAGES",             "from": "Actor",            "to": "Mechanism",        "rel_props": []},
        {"type": "FACES",               "from": "Actor",            "to": "Constraint",       "rel_props": []},
        {"type": "LOCATED_IN",          "from": "AdaptationAction", "to": "City",             "rel_props": []},
        {"type": "TARGETS_ZONE",        "from": "AdaptationAction", "to": "UrbanZone",        "rel_props": []},
        {"type": "ADDRESSES",           "from": "AdaptationAction", "to": "ClimateHazard",    "rel_props": []},
        {"type": "REDUCES",             "from": "AdaptationAction", "to": "Vulnerability",    "rel_props": []},
        {"type": "IMPROVES",            "from": "AdaptationAction", "to": "Infrastructure",   "rel_props": []},
        {"type": "FACILITATED_BY",      "from": "AdaptationAction", "to": "Mechanism",        "rel_props": []},
        {"type": "HINDERED_BY",         "from": "AdaptationAction", "to": "Constraint",       "rel_props": []},
        {"type": "PRODUCES",            "from": "AdaptationAction", "to": "Outcome",          "rel_props": []},
        {"type": "MEASURES",            "from": "Indicator",        "to": "Outcome",          "rel_props": []},
        {"type": "MONITORS",            "from": "Actor",            "to": "Indicator",        "rel_props": []},
        {"type": "REFLECTS",            "from": "Outcome",          "to": "ResilienceState",  "rel_props": []},
        {"type": "STARTED_AT",          "from": "AdaptationAction", "to": "TimePoint",        "rel_props": []},
        {"type": "ISSUED_AT",           "from": "Policy",           "to": "TimePoint",        "rel_props": []},
        {"type": "RECORDED_AT",         "from": "Indicator",        "to": "TimePoint",        "rel_props": ["value"]},
    ]
}

node_labels        = [n["label"] for n in ONTOLOGY["node_types"]]
relation_types     = [r["type"]  for r in ONTOLOGY["relation_types"]]
REL_DOMAIN_RANGE   = {r["type"]: (r["from"], r["to"]) for r in ONTOLOGY["relation_types"]}
NODE_ALLOWED_PROPS = {n["label"]: n["props"] for n in ONTOLOGY["node_types"]}
NODE_PRIMARY_KEY   = {
    "City": "name", "UrbanZone": "name", "Infrastructure": "name",
    "ExposureUnit": "name", "ClimateHazard": "name", "Vulnerability": "name",
    "Actor": "name", "Policy": "policy_name", "Mechanism": "name",
    "Constraint": "name", "AdaptationAction": "action_name", "Outcome": "name",
    "Indicator": "indicator_name", "ResilienceState": "name", "TimePoint": "year",
}

ONTOLOGY_NODE_STR = "\n".join(
    f"  {n['label']:20s} props: {n['props']}" for n in ONTOLOGY["node_types"]
)
ONTOLOGY_REL_STR = "\n".join(
    f"  {r['type']:25s} ({r['from']} → {r['to']})"
    + (f"  [rel_props: {r['rel_props']}]" if r["rel_props"] else "")
    for r in ONTOLOGY["relation_types"]
)

# ── Neo4j connection ─────────────────────────────────────────────────────────
from neo4j import GraphDatabase

class Neo4jKG:
    def __init__(self, uri, user, password, database):
        self.driver   = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    def run(self, query, params=None):
        with self.driver.session(database=self.database) as session:
            return [dict(r) for r in session.run(query, params or {})]

    def run_write(self, query, params=None):
        with self.driver.session(database=self.database) as session:
            return session.execute_write(lambda tx: list(tx.run(query, params or {})))

    def close(self):
        self.driver.close()

# ── PDF discovery ─────────────────────────────────────────────────────────────
def discover_pdfs(root: Path) -> list:
    """Walk PDF_ROOT and collect metadata for each PDF."""
    pdf_files = []
    for pdf_path in sorted(root.rglob("*.pdf")):
        parts = pdf_path.relative_to(root).parts
        region = parts[0] if len(parts) >= 2 else "Unknown"
        city   = parts[1] if len(parts) >= 2 else pdf_path.stem
        doc_id = re.sub(r'[^\w]', '_', pdf_path.stem)[:60]
        pdf_files.append({"path": pdf_path, "city": city, "region": region, "doc_id": doc_id})
    return pdf_files

# ── PDF → text ────────────────────────────────────────────────────────────────
import fitz

PAGE_NUM_PATTERNS = [
    r'^\s*\d{1,3}\s*$', r'^\s*[-–—]\s*\d{1,3}\s*[-–—]\s*$',
    r'^\s*[Pp]age\s+\d+\s*(of\s+\d+)?\s*$', r'^\s*\d+\s*/\s*\d+\s*$',
]
HEADER_FOOTER_PATTERNS = [
    r'(?i)www\.', r'(?i)©\s*\d{4}', r'(?i)all\s+rights\s+reserved',
    r'(?i)^\s*table\s+of\s+contents?\s*$', r'(?i)^\s*contents?\s*$',
]
TOC_PATTERN = re.compile(r'^.{5,80}\.{4,}\s*\d{1,3}\s*$')

def is_noise_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped: return False
    for pat in PAGE_NUM_PATTERNS:
        if re.match(pat, stripped): return True
    for pat in HEADER_FOOTER_PATTERNS:
        if re.search(pat, stripped): return True
    return bool(TOC_PATTERN.match(stripped))

def clean_page_text(text: str) -> str:
    lines = text.split('\n')
    cleaned = []
    for i, line in enumerate(lines):
        if (i < 2 or i >= len(lines) - 2):
            if is_noise_line(line) or (len(line.strip()) < 8 and line.strip()):
                continue
        elif is_noise_line(line):
            continue
        cleaned.append(line)
    text = '\n'.join(cleaned)
    text = re.sub(r'(\w)-\n(\w)', r'\1\2', text)
    text = re.sub(r'(?<![.!?:])\n(?![A-Z\n•\-\*\d])', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()

def pdf_to_text(pdf_info: dict, out_dir: Path) -> Path | None:
    """Extract and clean text from a single PDF, cache result as .txt."""
    out_path = out_dir / f"{pdf_info['doc_id']}.txt"
    if out_path.exists():
        return out_path
    try:
        doc = fitz.open(str(pdf_info["path"]))
        pages = []
        for i in range(len(doc)):
            text = doc[i].get_text("text")
            if len(text.strip()) < 50: continue
            toc_lines = sum(1 for l in text.split('\n') if l.strip() and TOC_PATTERN.match(l.strip()))
            total_lines = len([l for l in text.split('\n') if l.strip()])
            if total_lines and toc_lines / total_lines > 0.4: continue
            cleaned = clean_page_text(text)
            if cleaned:
                pages.append(f"[Page {i+1}]\n{cleaned}")
        out_path.write_text("\n\n".join(pages), encoding="utf-8")
        return out_path
    except Exception as e:
        print(f"  ERROR extracting {pdf_info['path'].name}: {e}")
        return None

# ── Chunking ──────────────────────────────────────────────────────────────────
from langchain.text_splitter import RecursiveCharacterTextSplitter
import tiktoken

enc      = tiktoken.get_encoding("cl100k_base")
splitter = RecursiveCharacterTextSplitter(
    chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP,
    length_function=lambda t: len(enc.encode(t)),
    separators=["\n\n", "\n", ". ", " ", ""]
)

def chunk_texts(text_files: list) -> list:
    """Split extracted texts into overlapping chunks with metadata."""
    chunks_path = CHUNK_DIR / "all_chunks.json"
    if chunks_path.exists():
        print("Loading cached chunks...")
        with open(chunks_path, encoding="utf-8") as f:
            return json.load(f)

    all_chunks = []
    for tf in tqdm(text_files, desc="Chunking"):
        raw = tf["text_path"].read_text(encoding="utf-8")
        lines = []
        for line in raw.split('\n'):
            s = line.strip()
            if re.match(r'^\[Page \d+\]$', s): continue
            if re.search(r'https?://|www\.|\.gov/|\.org/|\.com/', s) and len(s) < 150: continue
            if 0 < len(s.split()) <= 2: continue
            lines.append(line)
        raw = re.sub(r'\n{3,}', '\n\n', '\n'.join(lines)).strip()
        chunks = splitter.split_text(raw)
        valid  = [c for c in chunks if len(enc.encode(c)) >= 80
                  and sum(1 for ch in c if ch.isalpha()) / max(len(c), 1) >= 0.45]
        for i, chunk in enumerate(valid):
            all_chunks.append({
                "city": tf["city"], "region": tf.get("region", ""),
                "doc_id": tf["doc_id"], "source": tf["path"].name,
                "chunk_id": f"{tf['doc_id']}_chunk_{i:04d}",
                "chunk_idx": i, "total_chunks": len(valid),
                "text": chunk, "token_count": len(enc.encode(chunk)),
            })
    with open(chunks_path, 'w', encoding='utf-8') as f:
        json.dump(all_chunks, f, ensure_ascii=False, indent=2)
    print(f"Total chunks: {len(all_chunks)}")
    return all_chunks

# ── Triple extraction ─────────────────────────────────────────────────────────
from openai import OpenAI
openai_client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM_PROMPT = f"""You are a knowledge graph extraction specialist for urban climate adaptation research.

Your task is to extract structured triplets from policy documents written by city governments.

## DOMAIN FOCUS
Urban climate adaptation: how cities identify climate hazards, design and implement adaptation actions,
govern those actions through policies and institutions, overcome constraints, and evaluate outcomes.
Prioritize: CAUSAL CHAINS, GOVERNANCE STRUCTURES, SPECIFIC INTERVENTIONS, MEASURABLE OUTCOMES.

## ONTOLOGY
### Node Types:
{ONTOLOGY_NODE_STR}

### Relation Types:
{ONTOLOGY_REL_STR}

## EXTRACTION RULES
1. STRICT ONTOLOGY COMPLIANCE: only use listed labels and relation types.
2. SPECIFICITY OVER GENERALITY: extract named, specific entities.
3. CAUSAL PREFERENCE: prioritize cause-effect, mandate, implementation, constraint relationships.
4. EVIDENCE REQUIRED: every triplet must be directly supported by the text.
5. CONFIDENCE: HIGH = explicitly stated | MEDIUM = clearly implied | LOW = reasonably inferred

## OUTPUT FORMAT
Return ONLY a JSON object with key "triplets". No explanation, no markdown.
{{
  "triplets": [
    {{
      "subject": "Entity Name", "subject_type": "NodeLabel", "subject_props": {{}},
      "relation": "RELATION_TYPE", "rel_properties": {{}},
      "object": "Entity Name", "object_type": "NodeLabel", "object_props": {{}},
      "confidence": "HIGH|MEDIUM|LOW", "evidence": "direct quote ≤15 words"
    }}
  ]
}}
"""

def make_user_msg(chunk: dict) -> str:
    return (f"DOCUMENT CONTEXT\nCity: {chunk['city']}\nRegion: {chunk.get('region','')}\n"
            f"Document: {chunk['source']}\nPosition: chunk {chunk['chunk_idx']+1} of {chunk['total_chunks']}\n\n"
            f"TEXT TO EXTRACT FROM:\n{chunk['text']}")

def extract_triplets(chunk: dict, max_retries: int = 3) -> list:
    """Extract triplets from a single chunk using GPT."""
    for attempt in range(max_retries):
        try:
            response = openai_client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": make_user_msg(chunk)}
                ],
                temperature=0.0, max_tokens=2000,
                response_format={"type": "json_object"}
            )
            triplets = json.loads(response.choices[0].message.content).get("triplets", [])
            if isinstance(triplets, dict):
                triplets = list(triplets.values())[0]
            for t in triplets:
                t["city"] = chunk["city"]
                t["chunk_id"] = chunk["chunk_id"]
                t["source"] = chunk["source"]
            return triplets
        except Exception as e:
            print(f"    Attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)
    return []

def run_extraction(all_chunks: list):
    """Extract triplets from all chunks with resume support."""
    raw_path  = TRIPLET_DIR / "triplets_raw.json"
    done_path = TRIPLET_DIR / "done_chunks.json"
    all_triplets = []
    done_chunks  = set()

    if raw_path.exists():
        with open(raw_path, encoding="utf-8") as f:
            all_triplets = json.load(f)
    if done_path.exists():
        with open(done_path, encoding="utf-8") as f:
            done_chunks = set(json.load(f))

    remaining = [c for c in all_chunks if c["chunk_id"] not in done_chunks]
    print(f"Total: {len(all_chunks)} | Done: {len(done_chunks)} | Remaining: {len(remaining)}")

    for i, chunk in enumerate(tqdm(remaining, desc="Extracting")):
        triplets = extract_triplets(chunk)
        if triplets:
            all_triplets.extend(triplets)
            done_chunks.add(chunk["chunk_id"])
        time.sleep(0.3)
        if (i + 1) % 100 == 0:
            with open(raw_path, 'w', encoding='utf-8') as f:
                json.dump(all_triplets, f, ensure_ascii=False, indent=2)
            with open(done_path, 'w', encoding='utf-8') as f:
                json.dump(list(done_chunks), f)

    with open(raw_path, 'w', encoding='utf-8') as f:
        json.dump(all_triplets, f, ensure_ascii=False, indent=2)
    with open(done_path, 'w', encoding='utf-8') as f:
        json.dump(list(done_chunks), f)
    print(f"Extracted: {len(all_triplets)} raw triplets")
    return all_triplets

# ── Validation & cleaning ─────────────────────────────────────────────────────
import pandas as pd

def validate_and_clean(all_triplets: list) -> list:
    """Validate ontology compliance, normalize names, deduplicate."""
    df = pd.DataFrame(all_triplets)

    def is_valid(row) -> bool:
        s_type = row.get("subject_type", "")
        o_type = row.get("object_type", "")
        rel    = row.get("relation", "")
        if s_type not in node_labels or o_type not in node_labels: return False
        if rel not in relation_types: return False
        exp_from, exp_to = REL_DOMAIN_RANGE[rel]
        return s_type == exp_from and o_type == exp_to

    df["valid"] = df.apply(is_valid, axis=1)
    df_valid = df[df["valid"]].copy()

    def normalize(name: str) -> str:
        return re.sub(r'\s+', ' ', str(name)).strip().title()

    df_valid["subject"] = df_valid["subject"].apply(normalize)
    df_valid["object"]  = df_valid["object"].apply(normalize)
    df_valid = df_valid[(df_valid["subject"].str.len() > 2) & (df_valid["object"].str.len() > 2)]
    df_dedup = df_valid.drop_duplicates(subset=["subject", "relation", "object"], keep="first").reset_index(drop=True)

    clean_path = TRIPLET_DIR / "triplets_clean.json"
    df_dedup.to_json(clean_path, orient="records", force_ascii=False, indent=2)
    print(f"Raw: {len(df)} | Valid: {len(df_valid)} | After dedup: {len(df_dedup)}")
    return df_dedup.to_dict(orient="records")

# ── Neo4j ingestion ───────────────────────────────────────────────────────────
def get_primary_value(t: dict, side: str) -> str:
    node_type = t[f"{side}_type"]
    pk        = NODE_PRIMARY_KEY.get(node_type, "name")
    props     = t.get(f"{side}_props") or {}
    return props.get(pk) or t.get(side, "unknown")

def safe_props(props: dict, allowed: list) -> dict:
    if not props: return {}
    return {k: v for k, v in props.items() if k in allowed and v is not None and v != "null"}

def build_query(t: dict):
    s_type  = t["subject_type"]; o_type = t["object_type"]; rel = t["relation"]
    s_pk    = NODE_PRIMARY_KEY.get(s_type, "name")
    o_pk    = NODE_PRIMARY_KEY.get(o_type, "name")
    s_val   = get_primary_value(t, "subject")
    o_val   = get_primary_value(t, "object")
    s_props = safe_props(t.get("subject_props"), NODE_ALLOWED_PROPS.get(s_type, []))
    o_props = safe_props(t.get("object_props"),  NODE_ALLOWED_PROPS.get(o_type, []))
    r_props = t.get("rel_properties") or {}
    query = f"""
    MERGE (s:{s_type} {{{s_pk}: $s_val}})
      ON CREATE SET s += $s_props, s.created_at = datetime()
      ON MATCH  SET s += $s_props, s.updated_at = datetime()
    MERGE (o:{o_type} {{{o_pk}: $o_val}})
      ON CREATE SET o += $o_props, o.created_at = datetime()
      ON MATCH  SET o += $o_props, o.updated_at = datetime()
    MERGE (s)-[r:{rel}]->(o)
      ON CREATE SET r += $r_props, r.confidence = $confidence,
        r.evidence = $evidence, r.city = $city,
        r.source = $source, r.chunk_id = $chunk_id, r.created_at = datetime()
    """
    params = {
        "s_val": s_val, "o_val": o_val, "s_props": s_props, "o_props": o_props,
        "r_props": r_props, "confidence": t.get("confidence", "MEDIUM"),
        "evidence": t.get("evidence", ""), "city": t.get("city", ""),
        "source": t.get("source", ""), "chunk_id": t.get("chunk_id", ""),
    }
    return query, params

def write_to_neo4j(triplets: list):
    """Write validated triplets to Neo4j with MERGE (idempotent)."""
    kg = Neo4jKG(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE)

    # Create uniqueness constraints
    for label, prop in NODE_PRIMARY_KEY.items():
        try:
            kg.run_write(f"CREATE CONSTRAINT {label.lower()}_{prop}_unique IF NOT EXISTS FOR (n:{label}) REQUIRE n.{prop} IS UNIQUE")
        except Exception: pass

    success, errors = 0, []
    for t in tqdm(triplets, desc="Writing to Neo4j"):
        try:
            query, params = build_query(t)
            kg.run_write(query, params)
            success += 1
        except Exception as e:
            errors.append({"triplet": t, "error": str(e)})

    print(f"Success: {success} | Errors: {len(errors)}")
    kg.close()

# ── Main pipeline entry point ─────────────────────────────────────────────────
def run_full_pipeline():
    """Run the complete KG build pipeline end to end."""
    # 1. Discover PDFs
    pdf_files = discover_pdfs(PDF_ROOT)
    print(f"Found {len(pdf_files)} PDFs")

    # 2. Extract text
    text_files = []
    for pdf_info in tqdm(pdf_files, desc="Extracting PDFs"):
        tf = pdf_to_text(pdf_info, TEXT_DIR)
        if tf:
            text_files.append({**pdf_info, "text_path": tf})

    # 3. Chunk
    all_chunks = chunk_texts(text_files)

    # 4. Extract triplets
    all_triplets = run_extraction(all_chunks)

    # 5. Validate & clean
    clean_triplets = validate_and_clean(all_triplets)

    # 6. Write to Neo4j
    write_to_neo4j(clean_triplets)
    print("Pipeline complete.")

if __name__ == "__main__":
    run_full_pipeline()