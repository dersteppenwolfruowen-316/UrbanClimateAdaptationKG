# run_query_pipeline.py
import os
import json
from neo4j import GraphDatabase
from openai import OpenAI
from utils.graph_merge import merge_graphs  

# -----------------------------
# 1. config Neo4j 和 OpenAI
# -----------------------------
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "kg_reader"
NEO4J_PASSWORD = "kgpassword123"
NEO4J_DB = "BeijingKG"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD), encrypted=False)

# -----------------------------
# 2. implement Cypher
# -----------------------------
def execute_cypher(query: str):
    with driver.session(database=NEO4J_DB) as session:
        result = session.run(query)
        return [record.data() for record in result]

# -----------------------------
# 3. sub graph json
# -----------------------------
def convert_to_subgraph(records):
    nodes = {}
    relationships = []
    for r in records:
        for key, val in r.items():
            if hasattr(val, "id"):  # 节点
                if val.id not in nodes:
                    nodes[val.id] = dict(val)
                    nodes[val.id]["_label"] = list(val.labels)
            elif hasattr(val, "start_node"):  # 关系
                relationships.append({
                    "source": val.start_node.id,
                    "target": val.end_node.id,
                    "type": val.type,
                    **dict(val)
                })
    return {"nodes": list(nodes.values()), "relationships": relationships}

# -----------------------------
# 4. generate query plan
# -----------------------------
def generate_query_plan(question: str):
    """
    使用 LLM 自动生成 query plan (多条 Cypher)
    输出 JSON 列表，每条元素 {"description":..., "cypher":...}
    """
    prompt = f"""
You are an expert in Neo4j knowledge graphs about urban climate adaptation.

Task:
Given a natural language question, generate a JSON query plan.
Each element should contain:
- "description": what this query extracts
- "cypher": the Cypher query to extract relevant nodes and relationships

Important:
- Include multi-hop OPTIONAL MATCH or APOC path subgraph extraction if needed
- Return only valid JSON

Question: {question}
Graph schema:
- Labels: Actor, City, AdaptationAction, Outcome, ClimateHazard, Mechanism, Constraint
- Relationships: all types
"""
    response = client.chat.completions.create(
        model="gpt-5-mini",
        messages=[{"role":"user","content":prompt}]
    )
    return json.loads(response.choices[0].message.content)

# -----------------------------
# 5. merged sub graph
# -----------------------------
def execute_query_plan(query_plan):
    all_subgraphs = []
    for step in query_plan:
        cypher = step["cypher"]
        records = execute_cypher(cypher)
        subgraph = convert_to_subgraph(records)
        all_subgraphs.append(subgraph)
    merged_graph = merge_graphs(all_subgraphs)
    return merged_graph

# -----------------------------
# 6. LLM Reasoning
# -----------------------------
def reasoning(question, merged_graph):
    prompt = f"""
Question: {question}
Subgraph JSON: {merged_graph}
Please reason based on the graph structure and give the final answer.
"""
    response = client.chat.completions.create(
        model="gpt-5-mini",
        messages=[{"role":"user","content":prompt}]
    )
    return response.choices[0].message.content

# -----------------------------
# 7. main
# -----------------------------
def pipeline_qa(question: str):
    print("Generating query plan...")
    query_plan = generate_query_plan(question)
    print("Executing query plan...")
    merged_graph = execute_query_plan(query_plan)
    print("Merged subgraph nodes:", len(merged_graph["nodes"]))
    print("Merged subgraph relationships:", len(merged_graph["relationships"]))
    print("Running LLM reasoning...")
    answer = reasoning(question, merged_graph)
    return answer

# -----------------------------
# 8. example
# -----------------------------
if __name__ == "__main__":
    questions = [
        "Which adaptation actions are implemented for Heatwave in Beijing?",
        "Which mechanisms produce the strongest outcomes?",
        "Which actors manage the most mechanisms?",
    ]

    for q in questions:
        print("\n" + "="*60)
        print(f"Question: {q}")
        print("="*60)
        answer = pipeline_qa(q)
        print("\nFinal Answer:\n", answer)