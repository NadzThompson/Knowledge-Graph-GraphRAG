-- NOVA Native Graph Store - canonical Delta tables
-- Run in Databricks SQL after setting the desired catalog/schema.

CREATE TABLE IF NOT EXISTS nova_entity (
  entity_id STRING NOT NULL,
  entity_type STRING NOT NULL,
  canonical_name STRING NOT NULL,
  properties MAP<STRING, STRING>,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  recorded_from TIMESTAMP,
  recorded_to TIMESTAMP,
  source_system STRING,
  created_at TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;

CREATE TABLE IF NOT EXISTS nova_relationship (
  relationship_id STRING NOT NULL,
  source_entity_id STRING NOT NULL,
  target_entity_id STRING NOT NULL,
  relationship_type STRING NOT NULL,
  properties MAP<STRING, STRING>,
  materiality DOUBLE,
  confidence DOUBLE,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  recorded_from TIMESTAMP,
  recorded_to TIMESTAMP,
  source_system STRING,
  created_at TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;

CREATE TABLE IF NOT EXISTS nova_evidence (
  evidence_id STRING NOT NULL,
  source_type STRING NOT NULL,
  source_uri STRING NOT NULL,
  source_section STRING,
  source_version STRING,
  excerpt_hash STRING,
  confidence DOUBLE,
  approved BOOLEAN,
  metadata MAP<STRING, STRING>,
  created_at TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;

CREATE TABLE IF NOT EXISTS nova_relationship_evidence (
  relationship_id STRING NOT NULL,
  evidence_id STRING NOT NULL,
  created_at TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;

CREATE TABLE IF NOT EXISTS nova_entity_alias (
  alias STRING NOT NULL,
  entity_id STRING NOT NULL,
  alias_type STRING,
  source STRING,
  confidence DOUBLE,
  created_at TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;

CREATE TABLE IF NOT EXISTS nova_ontology_type (
  type_name STRING NOT NULL,
  kind STRING NOT NULL,
  parent_type STRING,
  description STRING,
  properties MAP<STRING, STRING>,
  created_at TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;

CREATE TABLE IF NOT EXISTS nova_graph_projection (
  projection_name STRING NOT NULL,
  subject_entity_id STRING NOT NULL,
  projection_json STRING NOT NULL,
  as_of TIMESTAMP,
  materiality_threshold DOUBLE,
  generated_at TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;
