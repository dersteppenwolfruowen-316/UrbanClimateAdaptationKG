import json
import time
from openai import OpenAI
from ontology.schema import ONTOLOGY
from config import OPENAI_API_KEY, MODEL_NAME


client = OpenAI(api_key=OPENAI_API_KEY)


# =========================================================
# ---- Build Allowed Types From Layered Ontology ---------
# =========================================================

def get_allowed_entity_types(ontology):
    entity_types = set()
    for layer, content in ontology.items():
        if layer == "relationships":
            continue
        for entity_type in content.keys():
            entity_types.add(entity_type)
    return entity_types


ALLOWED_ENTITY_TYPES = get_allowed_entity_types(ONTOLOGY)
ALLOWED_REL_TYPES = set(ONTOLOGY["relationships"].keys())


# =========================================================
# ---------------- Graph Validation -----------------------
# =========================================================

def validate_graph(graph_json):
    """
    Validate extracted graph against ontology.
    - Remove invalid entity types
    - Remove invalid relationship types
    - Enforce domain → range constraints
    - Remove orphan relations
    """

    valid_entities = {}
    valid_relationships = []

    # --------------------
    # Validate Entities
    # --------------------
    for ent in graph_json.get("entities", []):
        if ent.get("type") not in ALLOWED_ENTITY_TYPES:
            continue

        if not ent.get("id"):
            continue

        if not ent["id"].islower():
            continue

        if "properties" not in ent:
            ent["properties"] = {}

        valid_entities[ent["id"]] = ent

    # --------------------
    # Validate Relationships
    # --------------------
    for rel in graph_json.get("relationships", []):

        if rel.get("type") not in ALLOWED_REL_TYPES:
            continue

        sid = rel.get("source_id")
        tid = rel.get("target_id")

        if sid not in valid_entities:
            continue

        if tid not in valid_entities:
            continue

        domain = ONTOLOGY["relationships"][rel["type"]]["domain"]
        range_ = ONTOLOGY["relationships"][rel["type"]]["range"]

        source_type = valid_entities[sid]["type"]
        target_type = valid_entities[tid]["type"]

        if source_type != domain:
            continue

        if target_type != range_:
            continue

        if "properties" not in rel:
            rel["properties"] = {}

        valid_relationships.append(rel)

    return {
        "entities": list(valid_entities.values()),
        "relationships": valid_relationships
    }


# =========================================================
# ---------------- LLM Extraction -------------------------
# =========================================================

def extract_causal_graph(chunk, system_prompt, client, max_retries=3):
    """
    Extract structured multi-layer causal graph from one chunk.
    """

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=MODEL_NAME,
                temperature=0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": f"PROCESS THIS TEXT CHUNK:\n{chunk.page_content}"
                    }
                ]
            )

            raw = response.choices[0].message.content
            parsed = json.loads(raw)

            validated = validate_graph(parsed)

            return validated

        except Exception as e:
            print(f"[Retry {attempt+1}/{max_retries}] Extraction failed: {e}")
            time.sleep(2)

    print("⚠ Extraction failed after retries. Returning empty graph.")
    return {"entities": [], "relationships": []}


# =========================================================
# ----------- Batch Extraction Wrapper --------------------
# =========================================================

def extract_from_documents(chunks, system_prompt, client):
    all_graphs = []

    for i, chunk in enumerate(chunks):
        print(f"Processing chunk {i+1}/{len(chunks)}")

        graph = extract_causal_graph(chunk, system_prompt, client)

        print(
            f"  → Entities: {len(graph['entities'])}, "
            f"Relationships: {len(graph['relationships'])}"
        )

        all_graphs.append(graph)

        time.sleep(1)

    return all_graphs