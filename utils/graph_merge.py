def merge_graphs(kg_results):
    """
    kg_results: list of dict, 每个 dict 包含 'entities' 和 'relationships'
    
    return：
        merged_graph: dict, {"entities": [...], "relationships": [...]}
    """
    merged = {"entities": [], "relationships": []}

    entity_dict = {}
    for result in kg_results:
        for e in result.get("entities", []):
            entity_dict[e['id']] = e
    merged["entities"] = list(entity_dict.values())
    
    rel_set = {}
    for result in kg_results:
        for r in result.get("relationships", []):
            key = (r['source'], r['target'], r['type'])
            rel_set[key] = r
    merged["relationships"] = list(rel_set.values())

    return merged