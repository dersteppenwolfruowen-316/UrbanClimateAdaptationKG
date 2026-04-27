
"""
GraphRAG QA pipeline: vector retrieval, entity linking,
multi-hop graph traversal, Cypher generation, answer generation.
"""
import json
from openai import OpenAI
from neo4j import GraphDatabase
from src.config import (
    OPENAI_API_KEY, MODEL_NAME, EMBEDDING_MODEL,
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE
)

# ── Clients ───────────────────────────────────────────────────────────────────
client = OpenAI(api_key=OPENAI_API_KEY)


class Neo4jKG:
    def __init__(self, uri, user, password, database):
        self.driver   = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    def run(self, query, params=None):
        with self.driver.session(database=self.database) as session:
            return [dict(r) for r in session.run(query, params or {})]

    def run_write(self, query, params=None):
        with self.driver.session(database=self.database) as session:
            session.execute_write(lambda tx: tx.run(query, params or {}))

    def close(self):
        self.driver.close()


kg = Neo4jKG(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE)

# ── Embedding ─────────────────────────────────────────────────────────────────
def embed(text: str) -> list:
    """Embed text using OpenAI embedding model."""
    response = client.embeddings.create(model=EMBEDDING_MODEL, input=text)
    return response.data[0].embedding

# ── Vector retrieval ──────────────────────────────────────────────────────────
def vector_retrieve(query: str, top_k: int = 5) -> dict:
    """Retrieve top-k semantically similar nodes across 5 node types."""
    q_vec = embed(query)

    actions = kg.run("""
        CALL db.index.vector.queryNodes('node_embedding', $k, $vec)
        YIELD node, score
        MATCH (node)-[:LOCATED_IN]->(c:City)
        OPTIONAL MATCH (node)-[:ADDRESSES]->(h:ClimateHazard)
        OPTIONAL MATCH (node)-[:PRODUCES]->(o:Outcome)
        RETURN node.action_name AS name, 'AdaptationAction' AS type,
               c.name AS city, collect(distinct h.name)[..3] AS hazards,
               collect(distinct o.name)[..3] AS outcomes, score
        ORDER BY score DESC
    """, {"k": top_k, "vec": q_vec})

    hazards = kg.run("""
        CALL db.index.vector.queryNodes('hazard_embedding', $k, $vec)
        YIELD node, score
        MATCH (c:City)-[:EXPERIENCES]->(node)
        OPTIONAL MATCH (a:AdaptationAction)-[:ADDRESSES]->(node)
        RETURN node.name AS name, 'ClimateHazard' AS type,
               collect(distinct c.name)[..4] AS cities,
               collect(distinct a.action_name)[..3] AS actions, score
        ORDER BY score DESC
    """, {"k": top_k, "vec": q_vec})

    policies = kg.run("""
        CALL db.index.vector.queryNodes('policy_embedding', $k, $vec)
        YIELD node, score
        OPTIONAL MATCH (node)-[:MANDATES]->(a:AdaptationAction)-[:LOCATED_IN]->(c:City)
        OPTIONAL MATCH (node)-[:ISSUED_BY]->(actor:Actor)
        RETURN node.policy_name AS name, 'Policy' AS type,
               node.policy_type AS policy_type, node.level AS level,
               collect(distinct c.name)[..3] AS cities,
               collect(distinct actor.name)[..2] AS issuers, score
        ORDER BY score DESC
    """, {"k": top_k, "vec": q_vec})

    actors = kg.run("""
        CALL db.index.vector.queryNodes('actor_embedding', $k, $vec)
        YIELD node, score
        OPTIONAL MATCH (node)-[:IMPLEMENTS]->(a:AdaptationAction)-[:LOCATED_IN]->(c:City)
        RETURN node.name AS name, 'Actor' AS type,
               node.sector AS sector, node.role AS role,
               collect(distinct c.name)[..3] AS cities,
               collect(distinct a.action_name)[..3] AS actions, score
        ORDER BY score DESC
    """, {"k": top_k, "vec": q_vec})

    outcomes = kg.run("""
        CALL db.index.vector.queryNodes('outcome_embedding', $k, $vec)
        YIELD node, score
        OPTIONAL MATCH (a:AdaptationAction)-[:PRODUCES]->(node)
        OPTIONAL MATCH (a)-[:LOCATED_IN]->(c:City)
        RETURN node.name AS name, 'Outcome' AS type,
               node.outcome_type AS outcome_type,
               collect(distinct a.action_name)[..3] AS actions,
               collect(distinct c.name)[..3] AS cities, score
        ORDER BY score DESC
    """, {"k": top_k, "vec": q_vec})

    return {"actions": actions, "hazards": hazards, "policies": policies,
            "actors": actors, "outcomes": outcomes}

# ── Entity linking ────────────────────────────────────────────────────────────
def entity_link(query: str) -> list:
    """Extract entities from query with LLM, then fuzzy-match to graph nodes."""
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content":
            f"Extract entities from this query for a climate adaptation knowledge graph.\n"
            f"Return JSON only with keys: cities, hazards, actions, policies, actors (all lists).\n"
            f"Query: {query}"}],
        response_format={"type": "json_object"}, temperature=0.0
    )
    entities   = json.loads(response.choices[0].message.content)
    candidates = []

    # Match each entity type against the graph
    for city in entities.get("cities", []):
        candidates.extend(kg.run(
            "MATCH (c:City) WHERE toLower(c.name) CONTAINS toLower($name) "
            "RETURN c.name AS name, 'City' AS type, elementId(c) AS eid LIMIT 3",
            {"name": city}))
    for hazard in entities.get("hazards", []):
        candidates.extend(kg.run(
            "MATCH (h:ClimateHazard) WHERE toLower(h.name) CONTAINS toLower($name) "
            "RETURN h.name AS name, 'ClimateHazard' AS type, elementId(h) AS eid LIMIT 3",
            {"name": hazard}))
    for action in entities.get("actions", []):
        candidates.extend(kg.run(
            "MATCH (a:AdaptationAction) WHERE toLower(a.action_name) CONTAINS toLower($name) "
            "RETURN a.action_name AS name, 'AdaptationAction' AS type, elementId(a) AS eid LIMIT 3",
            {"name": action}))
    for policy in entities.get("policies", []):
        candidates.extend(kg.run(
            "MATCH (p:Policy) WHERE toLower(p.policy_name) CONTAINS toLower($name) "
            "RETURN p.policy_name AS name, 'Policy' AS type, elementId(p) AS eid LIMIT 3",
            {"name": policy}))
    for actor in entities.get("actors", []):
        candidates.extend(kg.run(
            "MATCH (a:Actor) WHERE toLower(a.name) CONTAINS toLower($name) "
            "RETURN a.name AS name, 'Actor' AS type, elementId(a) AS eid LIMIT 3",
            {"name": actor}))

    if not candidates:
        return []

    # LLM filter: keep only directly relevant entities
    candidate_labels = [f"{e['type']}: {e['name']}" for e in candidates]
    filter_response  = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content":
            f'Given this query: "{query}"\n\nSelect ONLY directly relevant entities.\n'
            f'Candidates:\n{json.dumps(candidate_labels, indent=2)}\n\n'
            f'Return JSON only with key: "selected" (list of strings, exactly as shown).'}],
        response_format={"type": "json_object"}, temperature=0.0
    )
    selected = json.loads(filter_response.choices[0].message.content).get("selected", [])
    return [e for e in candidates if f"{e['type']}: {e['name']}" in selected]

# ── Multi-hop graph traversal ─────────────────────────────────────────────────
def multihop_retrieve(linked_entities: list, max_hops: int = 2) -> dict:
    """Traverse causal chains and governance structures around linked entities."""
    if not linked_entities:
        return {}

    subgraphs = {}
    for entity in linked_entities:
        eid, name, etype = entity['eid'], entity['name'], entity['type']

        if etype == 'City':
            causal = kg.run("""
                MATCH (c:City)-[:EXPERIENCES]->(h:ClimateHazard)
                      <-[:ADDRESSES]-(a:AdaptationAction)-[:PRODUCES]->(o:Outcome)
                WHERE elementId(c) = $eid
                RETURN c.name AS city, h.name AS hazard, a.action_name AS action,
                       collect(distinct o.name)[..3] AS outcomes,
                       a.cost_usd AS cost, a.status AS status LIMIT 10
            """, {"eid": eid})
            governance = kg.run("""
                MATCH (p:Policy)-[:MANDATES]->(a:AdaptationAction)-[:LOCATED_IN]->(c:City)
                WHERE elementId(c) = $eid
                OPTIONAL MATCH (actor:Actor)-[:IMPLEMENTS]->(a)
                RETURN p.policy_name AS policy, a.action_name AS action,
                       collect(distinct actor.name)[..3] AS implementors LIMIT 8
            """, {"eid": eid})
            subgraphs[name] = {"causal_chains": causal, "governance": governance}

        elif etype == 'ClimateHazard':
            result = kg.run("""
                MATCH (c:City)-[:EXPERIENCES]->(h:ClimateHazard)
                      <-[:ADDRESSES]-(a:AdaptationAction)
                WHERE elementId(h) = $eid
                OPTIONAL MATCH (a)-[:PRODUCES]->(o:Outcome)
                RETURN c.name AS city, a.action_name AS action,
                       a.adaptation_type AS type,
                       collect(distinct o.name)[..2] AS outcomes LIMIT 15
            """, {"eid": eid})
            subgraphs[name] = {"city_responses": result}

        elif etype == 'AdaptationAction':
            result = kg.run("""
                MATCH (a:AdaptationAction) WHERE elementId(a) = $eid
                OPTIONAL MATCH (a)-[:LOCATED_IN]->(c:City)
                OPTIONAL MATCH (a)-[:ADDRESSES]->(h:ClimateHazard)
                OPTIONAL MATCH (a)-[:PRODUCES]->(o:Outcome)
                OPTIONAL MATCH (actor:Actor)-[:IMPLEMENTS]->(a)
                OPTIONAL MATCH (p:Policy)-[:MANDATES]->(a)
                OPTIONAL MATCH (a)-[:FACILITATED_BY]->(m:Mechanism)
                RETURN a.action_name AS action, a.status AS status, a.cost_usd AS cost,
                       c.name AS city, collect(distinct h.name) AS hazards,
                       collect(distinct o.name) AS outcomes,
                       collect(distinct actor.name) AS implementors,
                       collect(distinct p.policy_name) AS policies,
                       collect(distinct m.name) AS mechanisms
            """, {"eid": eid})
            subgraphs[name] = {"action_detail": result}

        elif etype == 'Policy':
            result = kg.run("""
                MATCH (p:Policy) WHERE elementId(p) = $eid
                OPTIONAL MATCH (p)-[:MANDATES]->(a:AdaptationAction)-[:LOCATED_IN]->(c:City)
                OPTIONAL MATCH (p)-[:ISSUED_BY]->(actor:Actor)
                OPTIONAL MATCH (a)-[:ADDRESSES]->(h:ClimateHazard)
                RETURN p.policy_name AS policy, p.policy_type AS policy_type, p.level AS level,
                       collect(distinct c.name)[..4] AS cities,
                       collect(distinct a.action_name)[..5] AS actions,
                       collect(distinct actor.name)[..3] AS issuers,
                       collect(distinct h.name)[..3] AS hazards_addressed LIMIT 10
            """, {"eid": eid})
            subgraphs[name] = {"policy_detail": result}

        elif etype == 'Actor':
            result = kg.run("""
                MATCH (actor:Actor) WHERE elementId(actor) = $eid
                OPTIONAL MATCH (actor)-[:IMPLEMENTS]->(a:AdaptationAction)-[:LOCATED_IN]->(c:City)
                OPTIONAL MATCH (actor)-[:COORDINATES_WITH]->(other:Actor)
                RETURN actor.name AS actor, actor.sector AS sector, actor.role AS role,
                       collect(distinct c.name)[..4] AS cities,
                       collect(distinct a.action_name)[..5] AS actions,
                       collect(distinct other.name)[..3] AS collaborators LIMIT 10
            """, {"eid": eid})
            subgraphs[name] = {"actor_detail": result}

    return subgraphs

# ── Subgraph path extraction ──────────────────────────────────────────────────
def subgraph_retrieve(linked_entities: list) -> list:
    """Find shortest paths between all pairs of linked entities (1-3 hops)."""
    if not linked_entities:
        return []
    eids = [e['eid'] for e in linked_entities]
    return kg.run("""
        UNWIND $eids AS eid1
        UNWIND $eids AS eid2
        WITH eid1, eid2 WHERE eid1 < eid2
        MATCH path = shortestPath((n1)-[*1..3]-(n2))
        WHERE elementId(n1) = eid1 AND elementId(n2) = eid2
        RETURN [node in nodes(path) |
                coalesce(node.name, node.action_name, node.policy_name)] AS node_names,
               [rel in relationships(path) | type(rel)] AS rel_types
        LIMIT 10
    """, {"eids": eids})

# ── LLM Cypher generation ─────────────────────────────────────────────────────
CYPHER_SYSTEM = """You are a Neo4j Cypher expert for an urban climate adaptation knowledge graph.

Node properties (use ONLY these exact property names):
- City: name, region, country, population
- ClimateHazard: name, hazard_type, severity
- AdaptationAction: action_name, adaptation_type, status, cost_usd, co_benefits
- Outcome: name, outcome_type, magnitude
- Actor: name, sector, role
- Policy: policy_name, policy_type, level, year
- Mechanism: name, mechanism_type
- Constraint: name, constraint_type
- Vulnerability: name, vulnerability_type

Key relationships:
  (City)-[:EXPERIENCES]->(ClimateHazard)
  (ClimateHazard)<-[:ADDRESSES]-(AdaptationAction)
  (AdaptationAction)-[:PRODUCES]->(Outcome)
  (AdaptationAction)-[:LOCATED_IN]->(City)
  (Policy)-[:MANDATES]->(AdaptationAction)
  (Actor)-[:IMPLEMENTS]->(AdaptationAction)
  (Policy)-[:ISSUED_BY]->(Actor)
  (ClimateHazard)-[:WORSENS]->(Vulnerability)
  (Actor)-[:COORDINATES_WITH]->(Actor)

CRITICAL Rules:
1. Return ONLY valid Cypher. No explanation, no markdown, no backticks.
2. Always LIMIT results (max 20).
3. Use OPTIONAL MATCH for non-essential relationships.
4. Use toLower() for string matching.
5. Always return node properties, NOT node objects.
6. For AdaptationAction use a.action_name (NOT a.name).
7. For Policy use p.policy_name (NOT p.name).
"""

def llm_cypher_generate(query: str) -> str:
    """Generate a Cypher query for the given natural language query."""
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": CYPHER_SYSTEM},
            {"role": "user",   "content": f"Generate a Cypher query for: {query}"}
        ],
        temperature=0.0
    )
    cypher = response.choices[0].message.content.strip()
    return cypher.replace("```cypher", "").replace("```", "").strip()

def safe_cypher_execute(cypher: str) -> list:
    """Execute Cypher with error handling."""
    try:
        return kg.run(cypher)
    except Exception as e:
        print(f"    Cypher execution failed: {e}")
        return []

# ── Context serialization ─────────────────────────────────────────────────────
def serialize_context(vector_results: dict, graph_results: dict,
                      cypher_results: list, subgraph_paths: list) -> str:
    """Serialize all retrieved evidence into a single context string for LLM."""
    parts = []

    if vector_results.get("actions"):
        parts.append("=== Relevant Adaptation Actions (semantic search) ===")
        for r in vector_results["actions"][:5]:
            parts.append(f"• [{r.get('city')}] {r.get('name')}\n"
                        f"  Addresses: {', '.join(r.get('hazards', []))}\n"
                        f"  Outcomes: {', '.join(r.get('outcomes', []))}\n"
                        f"  Similarity: {r.get('score', 0):.3f}")

    if vector_results.get("hazards"):
        parts.append("\n=== Related Climate Hazards (semantic search) ===")
        for r in vector_results["hazards"][:3]:
            parts.append(f"• {r.get('name')} — experienced by: {', '.join(r.get('cities', []))}\n"
                        f"  Addressed by: {', '.join(r.get('actions', []))}")

    if vector_results.get("policies"):
        parts.append("\n=== Relevant Policies (semantic search) ===")
        for r in vector_results["policies"][:3]:
            parts.append(f"• {r.get('name')} [{r.get('level')}]\n"
                        f"  Cities: {', '.join(r.get('cities', []))}\n"
                        f"  Issued by: {', '.join(r.get('issuers', []))}")

    if vector_results.get("actors"):
        parts.append("\n=== Relevant Actors (semantic search) ===")
        for r in vector_results["actors"][:3]:
            parts.append(f"• {r.get('name')} [{r.get('sector')}]\n"
                        f"  Cities: {', '.join(r.get('cities', []))}\n"
                        f"  Actions: {', '.join(r.get('actions', []))}")

    if vector_results.get("outcomes"):
        parts.append("\n=== Relevant Outcomes (semantic search) ===")
        for r in vector_results["outcomes"][:3]:
            parts.append(f"• {r.get('name')} [{r.get('outcome_type')}]\n"
                        f"  Produced by: {', '.join(r.get('actions', []))}\n"
                        f"  Cities: {', '.join(r.get('cities', []))}")

    if graph_results:
        parts.append("\n=== Graph Traversal Results ===")
        for entity_name, data in graph_results.items():
            parts.append(f"\nSubgraph around [{entity_name}]:")
            if "causal_chains" in data:
                for chain in data["causal_chains"][:5]:
                    parts.append(f"  {chain.get('city')} → {chain.get('hazard')} "
                                 f"→ {chain.get('action')} → {chain.get('outcomes', [])}")
            if "governance" in data:
                for g in data["governance"][:3]:
                    parts.append(f"  Policy: {g.get('policy')} → {g.get('action')} "
                                 f"(implemented by: {g.get('implementors', [])})")
            if "city_responses" in data:
                for r in data["city_responses"][:5]:
                    parts.append(f"  {r.get('city')}: {r.get('action')} → {r.get('outcomes', [])}")
            if "action_detail" in data and data["action_detail"]:
                d = data["action_detail"][0]
                parts.append(f"  Action: {d.get('action')} | City: {d.get('city')}\n"
                             f"  Hazards: {d.get('hazards', [])} | Outcomes: {d.get('outcomes', [])}")
            if "policy_detail" in data and data["policy_detail"]:
                d = data["policy_detail"][0]
                parts.append(f"  Policy: {d.get('policy')} [{d.get('level')}]\n"
                             f"  Cities: {d.get('cities', [])} | Issued by: {d.get('issuers', [])}")
            if "actor_detail" in data and data["actor_detail"]:
                d = data["actor_detail"][0]
                parts.append(f"  Actor: {d.get('actor')} [{d.get('sector')}]\n"
                             f"  Cities: {d.get('cities', [])} | Actions: {d.get('actions', [])}")

    if subgraph_paths:
        parts.append("\n=== Connecting Paths in Graph ===")
        for path in subgraph_paths[:5]:
            nodes = path.get("node_names", [])
            rels  = path.get("rel_types", [])
            path_str = ""
            for i, node in enumerate(nodes):
                path_str += str(node)
                if i < len(rels):
                    path_str += f" -[{rels[i]}]→ "
            parts.append(f"  {path_str}")

    if cypher_results:
        parts.append("\n=== Structured Query Results ===")
        for row in cypher_results[:8]:
            parts.append(f"  {dict(row)}")

    return "\n".join(parts)

# ── Answer generation ─────────────────────────────────────────────────────────
ANSWER_SYSTEM = """You are an expert on urban climate adaptation policy.
Answer the user's question based ONLY on the provided knowledge graph context.

Rules:
1. Ground every claim in the retrieved context — do not hallucinate.
2. Cite specific cities, actions, or policies from the context.
3. If the context is insufficient, say so explicitly.
4. Structure your answer clearly with key findings first.
5. Keep answers concise (150-250 words).
"""

def generate_answer(query: str, context: str) -> str:
    """Generate a grounded answer from the serialized context."""
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": ANSWER_SYSTEM},
            {"role": "user",   "content": f"Context from knowledge graph:\n{context}\n\nQuestion: {query}"}
        ],
        temperature=0.2, max_tokens=500
    )
    return response.choices[0].message.content

# ── Reasoning path validation ─────────────────────────────────────────────────
def validate_reasoning_paths(subgraph_paths: list) -> dict:
    """Check well-formedness of graph paths (nodes = edges + 1, no nulls)."""
    if not subgraph_paths:
        return {"total_paths": 0, "well_formed": 0, "ill_formed": 0, "well_formed_rate": 1.0}
    total, ill = len(subgraph_paths), 0
    for path in subgraph_paths:
        nodes = path.get("node_names", [])
        rels  = path.get("rel_types", [])
        if len(nodes) != len(rels) + 1 or any(n is None for n in nodes):
            ill += 1
    well_formed = total - ill
    return {"total_paths": total, "well_formed": well_formed, "ill_formed": ill,
            "well_formed_rate": round(well_formed / total, 3) if total > 0 else 1.0}

# ── Full QA pipeline ──────────────────────────────────────────────────────────
def qa_pipeline(query: str, verbose: bool = True) -> dict:
    """
    Full GraphRAG pipeline:
    1. Vector retrieval → 2. Entity linking → 3. Multi-hop traversal →
    4. Subgraph paths → 5. Path validation → 6. Cypher generation →
    7. Context serialization → 8. Answer generation
    """
    if verbose:
        print(f"\n{'='*60}\nQuery: {query}\n{'='*60}")

    if verbose: print("\n[1] Vector retrieval (5 node types)...")
    vector_results = vector_retrieve(query, top_k=5)

    if verbose: print("[2] Entity linking (with LLM filter)...")
    linked = entity_link(query)
    if verbose: print(f"    Linked: {[e['name'] for e in linked]}")

    if verbose: print("[3] Multi-hop graph traversal...")
    graph_results = multihop_retrieve(linked, max_hops=2)

    if verbose: print("[4] Subgraph path extraction...")
    subgraph_paths = subgraph_retrieve(linked)

    if verbose: print("[5] Validating reasoning paths...")
    path_validity = validate_reasoning_paths(subgraph_paths)
    if verbose: print(f"    Well-formed rate: {path_validity['well_formed_rate']:.3f} "
                      f"({path_validity['well_formed']}/{path_validity['total_paths']})")

    if verbose: print("[6] LLM Cypher generation...")
    cypher = llm_cypher_generate(query)
    if verbose: print(f"    Generated: {cypher[:100]}...")
    cypher_results = safe_cypher_execute(cypher)

    if verbose: print("[7] Serializing context...")
    context = serialize_context(vector_results, graph_results, cypher_results, subgraph_paths)

    if verbose: print("[8] Generating answer...")
    answer = generate_answer(query, context)

    if verbose:
        print(f"\n{'─'*60}\nANSWER:\n{answer}\n{'─'*60}")

    return {
        "query": query,
        "linked_entities": [dict(e) for e in linked],
        "cypher": cypher,
        "cypher_results": [dict(r) for r in cypher_results],
        "context": context,
        "answer": answer,
        "vector_hits": {k: len(v) for k, v in vector_results.items()},
        "graph_hits": sum(len(v) for v in graph_results.values()),
        "path_validity": path_validity,
    }


# ── JSON serializer helper ────────────────────────────────────────────────────
def neo4j_json_serializer(obj):
    if hasattr(obj, 'isoformat'): return obj.isoformat()
    if hasattr(obj, '__str__'):   return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')