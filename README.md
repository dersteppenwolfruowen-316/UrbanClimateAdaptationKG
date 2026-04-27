# Urban Climate Adaptation Knowledge Graph

A GraphRAG system for querying urban climate adaptation policies across 82 cities worldwide. Built with Neo4j, LangChain, and GPT-4o-mini.

[![Python](https://img.shields.io/badge/Python-3.10+-blue)](https://python.org)
[![Neo4j](https://img.shields.io/badge/Neo4j-5.x-green)](https://neo4j.com)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

## Overview

This project constructs a knowledge graph from 82 urban climate policy PDFs spanning cities across North America, Europe, Asia, and Africa. It supports natural language querying through a hybrid GraphRAG pipeline that combines vector search with multi-hop graph traversal.

**Key finding:** GraphRAG outperforms pure vector retrieval by **+69.3% in Answer Relevancy**, with the largest gap in multi-hop queries (+160%).

## Architecture

```
PDF Documents (82 cities)
        ↓
Text Extraction & Chunking (PyMuPDF + LangChain)
        ↓
Triple Extraction (GPT-4o-mini + Ontology V3)
        ↓
Knowledge Graph (Neo4j · 1,523 nodes · 1,121 edges)
        ↓
GraphRAG Pipeline
├── Vector Retrieval (text-embedding-3-small)
├── Entity Linking (LLM fuzzy match)
├── Multi-hop Graph Traversal (2-3 hops)
├── Cypher Generation (LLM)
└── Answer Generation (GPT-4o-mini)
```

## Knowledge Graph Stats

| Metric | Value |
|--------|-------|
| Cities | 36 |
| Total Nodes | 1,523 |
| Total Relationships | 1,121 |
| AdaptationActions | 642 |
| ClimateHazards | 205 |
| Policies | 106 |
| Actors | 119 |

## Evaluation Results

Evaluated on 25 queries (10 single-hop, 10 multi-hop, 5 cross-city) using [RAGAS](https://github.com/explodinggradients/ragas) framework (Es et al., 2023).

| System | Faithfulness | Answer Relevancy |
|--------|-------------|-----------------|
| **GraphRAG (ours)** | 0.699 | **0.740** |
| VectorRAG (baseline) | 0.708 | 0.437 |

**By query type (Answer Relevancy):**

| Query Type | GraphRAG | VectorRAG | Δ |
|------------|----------|-----------|---|
| Single-hop | 0.844 | 0.574 | +0.270 |
| Multi-hop | 0.658 | 0.253 | +0.405 |
| Cross-city | 0.693 | 0.532 | +0.161 |

Reasoning Path Well-formed Rate: **1.000**

## Ontology (V3)

Five subsystems aligned with subgraph retrieval logic:

- **UrbanSystem**: City, UrbanZone, Infrastructure, ExposureUnit
- **ClimateRisk**: ClimateHazard, Vulnerability
- **Governance**: Actor, Policy, Mechanism, Constraint
- **Intervention**: AdaptationAction
- **Evaluation**: Outcome, Indicator, ResilienceState, TimePoint

Core causal chain: `City -[EXPERIENCES]→ ClimateHazard ←[ADDRESSES]- AdaptationAction -[PRODUCES]→ Outcome`

## Quick Start

**1. Clone and install**
```bash
git clone https://github.com/dersteppenwolfruowen-316/UrbanClimateAdaptationKG.git
cd UrbanClimateAdaptationKG
pip install -r requirements.txt
```

**2. Configure environment**
```bash
cp .env.example .env
# Edit .env with your OpenAI API key and Neo4j credentials
```

**3. Run a query**
```python
from src.pipeline import qa_pipeline

result = qa_pipeline("What adaptation strategies does Houston use to address flooding?")
print(result['answer'])
```

**4. Run evaluation**

Open `notebook/evaluation.ipynb` and run all cells.

## Demo

> 🌐 Live demo: *coming soon*

<!-- Replace with your Streamlit Cloud URL after deployment -->

## Project Structure

```
urban-climate-kg/
├── src/
│   ├── config.py           # Environment configuration
│   ├── kg_builder.py       # PDF → chunks → triples → Neo4j
│   ├── graph_analysis.py   # GDS algorithms + community detection
│   └── pipeline.py         # GraphRAG QA pipeline
├── notebook/
│   ├── KG_5 cities.ipynb   # Main pipeline notebook
│   └── evaluation.ipynb    # RAGAS evaluation
├── data/
│   ├── ragas_results.csv
│   └── evaluation_summary.json
├── .env.example
└── requirements.txt
```

## Data

Policy documents are sourced from public urban resilience plans. See `data/README.md` for sources.

## References

- Es, S. et al. (2023). RAGAS: Automated Evaluation of Retrieval Augmented Generation. arXiv:2309.15217
- Edge, M. et al. (2024). From Local to Global: A Graph RAG Approach to Query-Focused Summarization. arXiv:2404.16130
- Zhu, Y. et al. (2025). Croppable knowledge graph embedding. ACL 2025.
- Lee, J. & Whang, J. J. (2025). Structure is all you need. ICML 2025.
- Wang, J. et al. (2024). Learning to plan for retrieval-augmented LLMs from knowledge graphs. arXiv:2406.14282
- Ning, Y. & Liu, H. (2024). UrbanKGent. NeurIPS 2024.