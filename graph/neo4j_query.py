# graph/neo4j_query.py

from neo4j import GraphDatabase
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser


class Neo4jQA:

    def __init__(self, uri, user, password, database="BeijingKG"):
        self.driver = GraphDatabase.driver(
            uri,
            auth=(user, password),
            encrypted=False
        )
        self.database = database

        self.llm = ChatOpenAI(model_name="gpt-4o-mini", temperature=0)
        self.parser = StrOutputParser()

        self.cypher_prompt = PromptTemplate(
            input_variables=["question"],
            template="""
Generate an executable Cypher query.

Question: {question}

Labels:
Actor, City, AdaptationAction, Outcome,
ClimateHazard, Mechanism, Constraint

Relationships:
IMPLEMENTS
FACILITATED_BY
PRODUCES
ADDRESSES
LOCATED_IN
MANAGES
PARTICIPATES_IN
FACES
EXPERIENCES

Return only Cypher.
"""
        )

        self.reasoning_prompt = PromptTemplate(
            input_variables=["question", "results"],
            template="""
You are an expert in climate adaptation governance.

Question: {question}

Graph Results:
{results}

Explain the reasoning and provide the final answer.
"""
        )

    def execute(self, query):
        with self.driver.session(database=self.database) as session:
            result = session.run(query)
            return [r.data() for r in result]

    def ask(self, question):

        cypher = (self.cypher_prompt | self.llm | self.parser).invoke(
            {"question": question}
        ).strip("` \n")

        print("Generated Cypher:\n", cypher)

        results = self.execute(cypher)

        answer = (self.reasoning_prompt | self.llm | self.parser).invoke(
            {
                "question": question,
                "results": results
            }
        )

        return answer

    def close(self):
        self.driver.close()