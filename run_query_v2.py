# run_query_v2.py

from graph.neo4j_query import Neo4jQA


qa = Neo4jQA(
    uri="bolt://localhost:7687",
    user="kg_reader",
    password="kgpassword123",
    database="beijingkgv2"
)


questions = [
    "Which mechanism type dominates Beijing adaptation governance?",
    "What adaptation actions are located in Beijing?",
    "Which actors manage the most mechanisms?",
    "Which adaptation actions lack facilitation mechanisms?",
    "Which hazards are not addressed by any adaptation action?"
]


for q in questions:
    print("\n" + "="*60)
    print("Question:", q)
    print("="*60)
    print(qa.ask(q))


qa.close()