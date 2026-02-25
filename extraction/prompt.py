EXTRACTION_PROMPT = """
You are an expert in Urban Climate Adaptation Governance,
Causal System Modeling, and Knowledge Graph Engineering.

Your task is to extract a MULTI-LAYER URBAN CLIMATE ADAPTATION SYSTEM
from the provided text using the ONTOLOGY schema.

This is NOT simple triplet extraction.
This is structured causal system modeling.

==================================================
CORE MODELING OBJECTIVE
==================================================

Model the following interacting layers:

1. Urban System Structure
2. Climate Risk Propagation
3. Governance & Institutional Mechanisms
4. Adaptation Interventions
5. Evaluation & Outcomes
6. Temporal Dynamics

You must preserve causal logic, enablement mechanisms,
and structural relationships.

==================================================
MANDATORY CAUSAL PRINCIPLE
==================================================

When supported by the text, construct full or partial causal chains:

Actor
  -[IMPLEMENTS]-> AdaptationAction
  -[FACILITATED_BY]-> Mechanism
  -[ADDRESSES]-> ClimateHazard
  -[REDUCES]-> Vulnerability
  -[PRODUCES]-> Outcome
  -[REFLECTS]-> ResilienceState

Mechanism is CRITICAL.

If financial, regulatory, institutional, technical,
or coordination enablers are mentioned,
you MUST explicitly extract a Mechanism entity
and connect it via FACILITATED_BY.

If policies mandate actions:
Policy -[MANDATES]-> AdaptationAction
Policy -[ISSUED_BY]-> Actor

If time is mentioned:
AdaptationAction -[STARTED_AT]-> TimePoint
Policy -[ISSUED_AT]-> TimePoint
Indicator -[RECORDED_AT]-> TimePoint

Prefer fewer but causally complete subgraphs
over many disconnected nodes.

==================================================
EXTRACTION RULES
==================================================

1. CITY SPECIFICITY
If multiple cities appear, distinguish them clearly.
Mark the primary case city as:
"is_case_study": true

2. NO ISOLATED NODES
Every entity must participate in at least one relationship.

3. NO HALLUCINATION
Only extract entities and relations explicitly stated
or strongly implied by the text.

Do NOT invent missing policies, actors, or outcomes.

4. ATTRIBUTE PRECISION
Extract quantitative values precisely:
- year
- cost_usd
- population
- gdp_per_capita
- frequency
- severity
- scale_usd
- baseline / target values
- indicator values
- percentages

If not explicitly stated → set property to null.

5. DETERMINISTIC ENTITY IDS
- lowercase only
- underscore format
- no spaces
- semantically stable
- city-specific if necessary

Example:
beijing
beijing_municipal_government
urban_heatwave
flood_resilience_program
climate_adaptation_fund_2015

6. RELATIONSHIP DIRECTION
Follow ontology domain → range strictly.

==================================================
STRUCTURAL LAYER GUIDANCE
==================================================

URBAN STRUCTURE
- City HAS_ZONE UrbanZone
- UrbanZone HAS_INFRASTRUCTURE Infrastructure
- Infrastructure SERVES ExposureUnit

CLIMATE RISK
- City EXPERIENCES ClimateHazard
- ClimateHazard AFFECTS_ZONE UrbanZone
- ClimateHazard EXPOSES ExposureUnit
- ClimateHazard WORSENS Vulnerability
- ExposureUnit EXPERIENCES_VULN Vulnerability

GOVERNANCE
- Policy ISSUED_BY Actor
- Policy MANDATES AdaptationAction
- Actor IMPLEMENTS AdaptationAction
- Actor MANAGES Mechanism
- Actor FACES Constraint

INTERVENTION
- AdaptationAction LOCATED_IN City
- AdaptationAction TARGETS_ZONE UrbanZone
- AdaptationAction ADDRESSES ClimateHazard
- AdaptationAction REDUCES Vulnerability
- AdaptationAction IMPROVES Infrastructure
- AdaptationAction FACILITATED_BY Mechanism
- AdaptationAction HINDERED_BY Constraint
- AdaptationAction PRODUCES Outcome

EVALUATION
- Indicator MEASURES Outcome
- Actor MONITORS Indicator
- Outcome REFLECTS ResilienceState

TEMPORAL
- AdaptationAction STARTED_AT TimePoint
- Policy ISSUED_AT TimePoint
- Indicator RECORDED_AT TimePoint

==================================================
OUTPUT FORMAT (STRICT JSON)
==================================================

Return a JSON object:

{
  "entities": [
      {
          "id": "string",
          "type": "EntityType",
          "properties": {}
      }
  ],
  "relationships": [
      {
          "type": "RELATION_TYPE",
          "source_id": "entity_id",
          "target_id": "entity_id",
          "properties": {}
      }
  ]
}

Return JSON only.
No markdown.
No explanation.
No comments.

TEXT:
"""