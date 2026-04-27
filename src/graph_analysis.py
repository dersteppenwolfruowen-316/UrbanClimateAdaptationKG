"""
GDS graph algorithms: degree/betweenness/pagerank centrality,
Louvain community detection, and visualization.
"""
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from neo4j import GraphDatabase
from src.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE


# ── Neo4j connection ──────────────────────────────────────────────────────────
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


def project_graph(kg: Neo4jKG):
    """Project the KG into GDS memory for algorithm execution."""
    try:
        kg.run_write("CALL gds.graph.drop('climate_graph', false)")
    except Exception:
        pass

    kg.run_write("""
        CALL gds.graph.project(
            'climate_graph',
            ['City','ClimateHazard','AdaptationAction','Outcome',
             'Actor','Policy','Vulnerability','Mechanism'],
            {
                EXPERIENCES:     {orientation: 'UNDIRECTED'},
                ADDRESSES:       {orientation: 'UNDIRECTED'},
                PRODUCES:        {orientation: 'UNDIRECTED'},
                IMPLEMENTS:      {orientation: 'UNDIRECTED'},
                MANDATES:        {orientation: 'UNDIRECTED'},
                LOCATED_IN:      {orientation: 'UNDIRECTED'},
                WORSENS:         {orientation: 'UNDIRECTED'},
                ISSUED_BY:       {orientation: 'UNDIRECTED'},
                PARTICIPATES_IN: {orientation: 'UNDIRECTED'},
                COORDINATES_WITH:{orientation: 'UNDIRECTED'}
            }
        )
    """)
    print("Graph projected to GDS memory")


def run_degree_centrality(kg: Neo4jKG) -> pd.DataFrame:
    """Compute degree centrality for top 20 nodes."""
    result = kg.run("""
        CALL gds.degree.stream('climate_graph')
        YIELD nodeId, score
        WITH gds.util.asNode(nodeId) AS node, score
        WHERE score > 0
        RETURN labels(node)[0] AS type,
               coalesce(node.name, node.action_name, node.policy_name) AS name,
               score AS degree
        ORDER BY degree DESC LIMIT 20
    """)
    df = pd.DataFrame(result)
    print("\n=== Degree Centrality (Top 20) ===")
    print(df.to_string(index=False))
    return df


def run_betweenness_centrality(kg: Neo4jKG) -> pd.DataFrame:
    """Compute betweenness centrality for top 15 nodes."""
    result = kg.run("""
        CALL gds.betweenness.stream('climate_graph')
        YIELD nodeId, score
        WITH gds.util.asNode(nodeId) AS node, score
        WHERE score > 0
        RETURN labels(node)[0] AS type,
               coalesce(node.name, node.action_name, node.policy_name) AS name,
               round(score, 2) AS betweenness
        ORDER BY betweenness DESC LIMIT 15
    """)
    df = pd.DataFrame(result)
    print("\n=== Betweenness Centrality (Top 15) ===")
    print(df.to_string(index=False))
    return df


def run_pagerank(kg: Neo4jKG) -> pd.DataFrame:
    """Compute PageRank for top 15 nodes."""
    result = kg.run("""
        CALL gds.pageRank.stream('climate_graph', {dampingFactor: 0.85, maxIterations: 20})
        YIELD nodeId, score
        WITH gds.util.asNode(nodeId) AS node, score
        RETURN labels(node)[0] AS type,
               coalesce(node.name, node.action_name, node.policy_name) AS name,
               round(score, 4) AS pagerank
        ORDER BY pagerank DESC LIMIT 15
    """)
    df = pd.DataFrame(result)
    print("\n=== PageRank (Top 15) ===")
    print(df.to_string(index=False))
    return df


def run_louvain(kg: Neo4jKG) -> pd.DataFrame:
    """Run Louvain community detection and write community property to nodes."""
    kg.run_write("""
        CALL gds.louvain.write('climate_graph', {writeProperty: 'community'})
    """)
    result = kg.run("""
        MATCH (n) WHERE n.community IS NOT NULL
        RETURN n.community AS community, count(n) AS size,
               collect(distinct labels(n)[0])[..5] AS node_types
        ORDER BY size DESC LIMIT 15
    """)
    df = pd.DataFrame(result)
    print("\n=== Louvain Community Detection ===")
    print(df.to_string(index=False))

    # Print top nodes per community
    print("\nTop nodes per community:")
    result2 = kg.run("""
        MATCH (n) WHERE n.community IS NOT NULL
        WITH n.community AS comm,
             collect({name: coalesce(n.name, n.action_name, n.policy_name), type: labels(n)[0]})[..4] AS nodes,
             count(n) AS size
        ORDER BY size DESC LIMIT 8
        RETURN comm, size, nodes
    """)
    for row in result2:
        names = [f"{n['type']}:{n['name']}" for n in row['nodes']]
        print(f"  Community {row['comm']} (size={row['size']}): {', '.join(names)}")

    return df


def plot_analysis(df_degree: pd.DataFrame, df_between: pd.DataFrame, df_community: pd.DataFrame):
    """Generate and save the 3-panel graph analysis chart."""
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle('Knowledge Graph Analysis', fontsize=14, fontweight='bold')

    # Degree centrality bar chart
    colors_degree = [
        '#0D9488' if t == 'AdaptationAction' else
        '#4F46E5' if t == 'ClimateHazard' else
        '#F97316' if t == 'City' else '#94A3B8'
        for t in df_degree['type']
    ]
    axes[0].barh(range(len(df_degree)), df_degree['degree'], color=colors_degree)
    axes[0].set_yticks(range(len(df_degree)))
    axes[0].set_yticklabels([str(n)[:28] for n in df_degree['name']], fontsize=8)
    axes[0].set_xlabel('Degree')
    axes[0].set_title('Degree Centrality')
    axes[0].invert_yaxis()
    axes[0].legend(handles=[
        mpatches.Patch(color='#0D9488', label='AdaptationAction'),
        mpatches.Patch(color='#4F46E5', label='ClimateHazard'),
        mpatches.Patch(color='#F97316', label='City'),
        mpatches.Patch(color='#94A3B8', label='Other'),
    ], fontsize=7)

    # Betweenness centrality
    axes[1].barh(range(len(df_between)), df_between['betweenness'], color='#6366F1')
    axes[1].set_yticks(range(len(df_between)))
    axes[1].set_yticklabels([str(n)[:28] for n in df_between['name']], fontsize=8)
    axes[1].set_xlabel('Betweenness Score')
    axes[1].set_title('Betweenness Centrality')
    axes[1].invert_yaxis()

    # Community sizes
    axes[2].bar(range(len(df_community)), df_community['size'], color='#14B8A6')
    axes[2].set_xticks(range(len(df_community)))
    axes[2].set_xticklabels([f"C{r}" for r in df_community['community']], fontsize=8)
    axes[2].set_ylabel('Node Count')
    axes[2].set_title('Community Sizes (Louvain)')

    plt.tight_layout()
    plt.savefig('./data/graph_analysis.png', dpi=150, bbox_inches='tight')
    plt.show()
    print("Saved → ./data/graph_analysis.png")


def plot_community_detail(communities: list):
    """
    Generate bubble + bar chart for community thematic analysis.
    communities: list of dicts with keys: id, size, city, theme, hazards, color
    """
    light_colors = {
        "#F97316": "#FED7AA", "#0D9488": "#99F6E4", "#6366F1": "#C7D2FE",
        "#E11D48": "#FECDD3", "#8B5CF6": "#DDD6FE",
    }

    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    fig.patch.set_facecolor('white')
    for ax in axes: ax.set_facecolor('white')

    # Left: bubble chart
    ax = axes[0]
    ax.set_xlim(-1.5, 1.5); ax.set_ylim(-1.5, 1.5)
    ax.set_aspect('equal'); ax.axis('off')
    ax.set_title('Community Structure', color='#1E293B', fontsize=13, fontweight='bold', pad=12)

    angles    = np.linspace(0, 2 * np.pi, len(communities), endpoint=False)
    positions = [(0.85 * np.cos(a), 0.85 * np.sin(a)) for a in angles]

    for comm, pos in zip(communities, positions):
        radius = comm["size"] / 200
        ax.add_patch(plt.Circle(pos, radius,
            color=light_colors[comm["color"]], ec=comm["color"],
            linewidth=1.5, alpha=0.9, zorder=3))
        ax.text(pos[0], pos[1]+0.01, comm["id"], ha='center', va='center',
                fontsize=9, fontweight='bold', color=comm["color"], zorder=4)
        ax.text(pos[0], pos[1]-radius-0.08, comm["city"],
                ha='center', va='top', fontsize=8, color='#475569', zorder=4)
        ax.text(pos[0], pos[1]-radius-0.18, f"{comm['size']} nodes",
                ha='center', va='top', fontsize=7, color='#94A3B8', zorder=4)
        ax.plot([0, pos[0]], [0, pos[1]], color='#CBD5E1', linewidth=0.8, zorder=1, linestyle='--')

    ax.add_patch(plt.Circle((0,0), 0.12, color='#F1F5F9', ec='#94A3B8', linewidth=1.5, zorder=3))
    ax.text(0, 0, 'KG', ha='center', va='center', fontsize=9, color='#64748B', fontweight='bold', zorder=4)

    # Right: horizontal bar
    ax = axes[1]
    ax.set_title('Key Hazards by Community', color='#1E293B', fontsize=13, fontweight='bold', pad=12)
    for i, comm in enumerate(communities):
        y = len(communities) - 1 - i
        ax.barh(y, comm["size"], height=0.55,
                color=light_colors[comm["color"]], edgecolor=comm["color"], linewidth=1.2)
        ax.text(comm["size"]+1.5, y, f"{comm['size']}", va='center', color='#64748B', fontsize=8)
        tag_x = 2
        for hazard in comm["hazards"][:3]:
            short = hazard[:15] + "…" if len(hazard) > 15 else hazard
            ax.text(tag_x, y-0.27, short, fontsize=6.5, color=comm["color"],
                    bbox=dict(boxstyle='round,pad=0.2', facecolor=light_colors[comm["color"]],
                              edgecolor=comm["color"], linewidth=0.6))
            tag_x += len(short) * 0.88 + 7

    ax.set_yticks(range(len(communities)))
    ax.set_yticklabels([f"{c['id']}  {c['theme']}" for c in reversed(communities)],
                       color='#1E293B', fontsize=8.5)
    ax.set_xlabel('Node Count', color='#64748B', fontsize=9)
    for spine in ax.spines.values(): spine.set_edgecolor('#E2E8F0')
    ax.set_xlim(0, 145)
    ax.grid(axis='x', color='#F1F5F9', linewidth=0.8); ax.set_axisbelow(True)

    plt.suptitle('Louvain Community Detection', color='#1E293B', fontsize=14, fontweight='bold', y=1.01)
    plt.tight_layout()
    plt.savefig('./data/community_analysis.png', dpi=150, bbox_inches='tight', facecolor='white')
    plt.show()
    print("Saved → ./data/community_analysis.png")


def run_full_analysis():
    """Run all graph algorithms and generate visualizations."""
    kg = Neo4jKG(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE)
    project_graph(kg)
    df_degree  = run_degree_centrality(kg)
    df_between = run_betweenness_centrality(kg)
    run_pagerank(kg)
    df_community = run_louvain(kg)
    plot_analysis(df_degree, df_between, df_community)

    # Cross-city hazard sharing
    print("\n=== Hazards shared across multiple cities ===")
    result = kg.run("""
        MATCH (c:City)-[:EXPERIENCES]->(h:ClimateHazard)
        WITH h, collect(distinct c.name) AS cities, count(distinct c) AS city_count
        WHERE city_count > 1
        RETURN h.name AS hazard, city_count, cities
        ORDER BY city_count DESC LIMIT 15
    """)
    for row in result:
        print(f"  {row['hazard']:40s}: {row['city_count']} cities")

    kg.close()
    print("Graph analysis complete.")


if __name__ == "__main__":
    run_full_analysis()