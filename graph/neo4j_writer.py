import json
from neo4j import GraphDatabase

class Neo4jWriter:

    def __init__(self, uri, user, password, database):
        self.driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self.database = database

    def close(self):
        self.driver.close()

    @staticmethod
    def sanitize_properties(props: dict) -> dict:
        clean = {}
        for k, v in props.items():
            if isinstance(v, (str, int, float, bool)):
                clean[k] = v
            elif isinstance(v, list):
                clean[k] = [i for i in v if isinstance(i, (str, int, float, bool))]
            elif isinstance(v, dict):

                clean[k] = json.dumps(v)
        return clean

    def write_graph(self, graph: dict):
        with self.driver.session(database=self.database) as session:


            for entity in graph.get("entities", []):
                session.execute_write(self._write_node, entity)

            for rel in graph.get("relationships", []):
                session.execute_write(self._write_relationship, rel)

        print("Graph successfully written to Neo4j.")

    @staticmethod
    def _write_node(tx, entity: dict):
        label = entity.get("type") or "Entity"
        node_id = entity.get("id")
        props = entity.get("properties", {})

        props = Neo4jWriter.sanitize_properties(props)

        if not node_id:
            raise ValueError(f"Node missing 'id': {entity}")

        query = f"""
        MERGE (n:{label} {{id: $id}})
        SET n += $props
        """
        tx.run(query, id=node_id, props=props)


    @staticmethod
    def _write_relationship(tx, rel: dict):
        rel_type = rel.get("type")
        source = rel.get("source") or rel.get("start") or rel.get("from")
        target = rel.get("target") or rel.get("end") or rel.get("to")
        props = rel.get("properties", {})


        props = Neo4jWriter.sanitize_properties(props)

        if not all([source, target, rel_type]):
            return

        query = f"""
        MATCH (a {{id: $source}})
        MATCH (b {{id: $target}})
        MERGE (a)-[r:{rel_type}]->(b)
        SET r += $props
        """
        tx.run(query, source=source, target=target, props=props)