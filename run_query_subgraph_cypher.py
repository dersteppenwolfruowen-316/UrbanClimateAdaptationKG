import json
from neo4j import GraphDatabase
from openai import OpenAI
from utils.graph_merge import merge_graphs

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("kg_reader","kgpassword123"))
client = OpenAI(api_key="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")

def execute_cypher(query):
    with driver.session(database="BeijingKGV2") as session:
        result = session.run(query)
        return [record.data() for record in result]

def convert_to_subgraph(records):
    nodes = {}
    relationships = []
    for r in records:
        for key, val in r.items():
            if hasattr(val, "id"):
                if val.type is None:
                    nodes[val.id] = dict(val)
                    nodes[val.id]['_label'] = list(val.labels)
                else:
                    relationships.append({
                        "source": val.start_node.id,
                        "target": val.end_node.id,
                        "type": val.type,
                        **dict(val)
                    })
    return {"nodes": list(nodes.values()), "relationships": relationships}

# query plan
def generate_query_plan(question):
    prompt = f"""
You are an expert in Neo4j knowledge graphs.
Generate a JSON query plan (description + cypher) for this question: {question}
"""
    response = client.chat.completions.create(
        model="gpt-5-mini",
        messages=[{"role":"user","content":prompt}]
    )
    return json.loads(response.choices[0].message.content)

def reasoning_new(question):
    query_plan = generate_query_plan(question)
    all_subgraphs = []
    for step in query_plan:
        cypher = step['cypher']
        records = execute_cypher(cypher)
        subgraph = convert_to_subgraph(records)
        all_subgraphs.append(subgraph)

    merged_graph = merge_graphs(all_subgraphs)
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

# Example usage
question = "What adaptation actions are implemented for Heatwave?"
answer = reasoning_new(question)
print(answer)