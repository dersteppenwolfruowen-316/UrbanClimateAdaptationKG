from neo4j import GraphDatabase

class Neo4jWriter:

    def __init__(self, uri, user, password, database):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    def close(self):
        self.driver.close()

    # --------------------------------------------------
    # Write Entire Graph
    # --------------------------------------------------
    def write_graph(self, graph):

        with self.driver.session(database=self.database) as session:

            # ---- write nodes ----
            for entity in graph["entities"]:
                session.execute_write(self._write_node, entity)

            # ---- write relationships ----
            for rel in graph["relationships"]:
                session.execute_write(self._write_relationship, rel)

        print("Graph successfully written to Neo4j.")

    # --------------------------------------------------
    # Write Node
    # --------------------------------------------------
    @staticmethod
    def _write_node(tx, entity):

        label = entity["type"]

        query = f"""
        MERGE (n:{label} {{id: $id}})
        SET n += $props
        """

        tx.run(
            query,
            id=entity["id"],
            props=entity.get("properties", {})
        )

    # --------------------------------------------------
    # Write Relationship
    # --------------------------------------------------
    @staticmethod
    def _write_relationship(tx, rel):

        rel_type = rel["type"]

        query = f"""
        MATCH (a {{id: $sid}})
        MATCH (b {{id: $tid}})
        MERGE (a)-[r:{rel_type}]->(b)
        SET r += $props
        """

        tx.run(
            query,
            sid=rel["source_id"],
            tid=rel["target_id"],
            props=rel.get("properties", {})
        )