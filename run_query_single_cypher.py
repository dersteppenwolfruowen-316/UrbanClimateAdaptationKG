from neo4j import GraphDatabase
from openai import OpenAI

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("kg_reader","kgpassword123"))

client = OpenAI(api_key="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")

def execute_cypher(query):
    with driver.session(database="BeijingKGV2") as session:
        result = session.run(query)
        return [record.data() for record in result]

def reasoning_old(question, cypher_query):
    results = execute_cypher(cypher_query)
    prompt = f"""
Question: {question}
Query Results: {results}
Please reason and provide the final answer.
"""
    response = client.chat.completions.create(
        model="gpt-5-mini",
        messages=[{"role":"user","content":prompt}]
    )
    return response.choices[0].message.content

# Example usage
question = "What adaptation actions are implemented for Heatwave?"
cypher_query = "MATCH (h:ClimateHazard {name: 'Heatwave'})<-[:ADDRESSES]-(a:AdaptationAction) RETURN h, a"
answer = reasoning_old(question, cypher_query)
print(answer)