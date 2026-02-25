def merge_graphs(kg_results):
    """
    kg_results: list of dict, contains 'entities' 和 'relationships'
    
    return：
        merged_graph: dict, {"entities": [...], "relationships": [...]}
    """
    merged = {"entities": [], "relationships": []}

    # ---- Merge entities ----
    entity_dict = {}
    for result in kg_results:
        for e in result.get("entities", []):
            entity_dict[e['id']] = e
    merged["entities"] = list(entity_dict.values())

    # ---- Merge relationships safely ----
    rel_set = {}
    for result in kg_results:
        for r in result.get("relationships", []):
            # source/target/type
            source = r.get('source') or r.get('start') or r.get('from')
            target = r.get('target') or r.get('end') or r.get('to')
            r_type = r.get('type')

            # if none
            if not all([source, target, r_type]):
                continue

            key = (source, target, r_type)
            if key not in rel_set:
                rel_set[key] = r

    merged["relationships"] = list(rel_set.values())

    return merged