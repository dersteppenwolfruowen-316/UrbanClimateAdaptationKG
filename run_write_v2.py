import json
import time
from ontology.schema import ONTOLOGY
from config import OPENAI_API_KEY, MODEL_NAME
from ingestion.pdf_loader import load_pdf
from ingestion.chunker import chunk_documents
from extraction.llm_extractor import extract_from_documents
from utils.graph_merge import merge_graphs
from graph.neo4j_writer import Neo4jWriter
from extraction.prompt import EXTRACTION_PROMPT
from openai import OpenAI

client = OpenAI(api_key=OPENAI_API_KEY)

# =========================================================
# 1. Load PDF
# =========================================================
file_path = "/Users/ruowen_vagabond/Desktop/Knowledge Graph - urban climate adaptation/data/text/50-climate-solutions-prc-cities.pdf"
pages = load_pdf(file_path)
chunks = chunk_documents(pages)
print(f"Loaded {len(chunks)} chunks")

# =========================================================
# 2. Extract KG
# =========================================================
kg_results = extract_from_documents(
    chunks,
    system_prompt=EXTRACTION_PROMPT,
    client=client
)
print("Extraction completed.")

# =========================================================
# 3. Merge
# =========================================================
merged_graph = merge_graphs(kg_results)
print("Merged entities:", len(merged_graph["entities"]))
print("Merged relationships:", len(merged_graph["relationships"]))

# =========================================================
# 4. Write to Neo4j
# =========================================================
writer = Neo4jWriter(
    uri="bolt://localhost:7687",
    user="kg_reader",
    password="kgpassword123",
    database="beijingkgv2"
)
writer.write_graph(merged_graph)
writer.close()
print("Graph written to beijingkgv2 successfully.")