from __future__ import annotations
from datetime import datetime
from nova_graphrag.graph_store.base import GraphStore
from nova_graphrag.models import Entity, Relationship, Evidence


class DeltaGraphStore(GraphStore):
    """Databricks/Delta implementation contract.

    This adapter intentionally expects a SparkSession supplied by NOVA's runtime so that
    credentials, Unity Catalog, network policy and cluster configuration remain external.
    """

    def __init__(self, spark, catalog: str, schema: str):
        self.spark = spark
        self.prefix = f"{catalog}.{schema}"

    def upsert_entities(self, entities: list[Entity]) -> None:
        if not entities:
            return
        rows = [e.model_dump(mode="python") for e in entities]
        df = self.spark.createDataFrame(rows)
        df.createOrReplaceTempView("_nova_entity_updates")
        self.spark.sql(f"""
            MERGE INTO {self.prefix}.nova_entity t
            USING _nova_entity_updates s
            ON t.entity_id = s.entity_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    def upsert_relationships(self, relationships: list[Relationship]) -> None:
        if not relationships:
            return
        rows = [r.model_dump(mode="python", exclude={"evidence_ids"}) for r in relationships]
        df = self.spark.createDataFrame(rows)
        df.createOrReplaceTempView("_nova_rel_updates")
        self.spark.sql(f"""
            MERGE INTO {self.prefix}.nova_relationship t
            USING _nova_rel_updates s
            ON t.relationship_id = s.relationship_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        pairs = [
            {"relationship_id": r.relationship_id, "evidence_id": eid}
            for r in relationships for eid in r.evidence_ids
        ]
        if pairs:
            self.spark.createDataFrame(pairs).dropDuplicates().write.mode("append").saveAsTable(
                f"{self.prefix}.nova_relationship_evidence"
            )

    def upsert_evidence(self, evidence: list[Evidence]) -> None:
        if not evidence:
            return
        rows = [e.model_dump(mode="python") for e in evidence]
        df = self.spark.createDataFrame(rows)
        df.createOrReplaceTempView("_nova_evidence_updates")
        self.spark.sql(f"""
            MERGE INTO {self.prefix}.nova_evidence t
            USING _nova_evidence_updates s
            ON t.evidence_id = s.evidence_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    def get_entity(self, entity_id: str, as_of: datetime | None = None) -> Entity | None:
        cond = f"entity_id = '{entity_id.replace(chr(39), chr(39)*2)}'"
        if as_of:
            ts = as_of.isoformat()
            cond += f" AND (valid_from IS NULL OR valid_from <= TIMESTAMP '{ts}') AND (valid_to IS NULL OR valid_to > TIMESTAMP '{ts}')"
        rows = self.spark.sql(f"SELECT * FROM {self.prefix}.nova_entity WHERE {cond} LIMIT 1").collect()
        return Entity(**rows[0].asDict()) if rows else None

    def find_entities(self, name: str, entity_types: list[str] | None = None, limit: int = 20) -> list[Entity]:
        safe = name.replace("'", "''").lower()
        cond = f"lower(canonical_name) LIKE '%{safe}%'"
        if entity_types:
            vals = ",".join("'" + x.replace("'", "''") + "'" for x in entity_types)
            cond += f" AND entity_type IN ({vals})"
        rows = self.spark.sql(f"SELECT * FROM {self.prefix}.nova_entity WHERE {cond} LIMIT {int(limit)}").collect()
        return [Entity(**r.asDict()) for r in rows]

    def neighbors(self, entity_ids, relationship_types=None, as_of=None):
        if not entity_ids:
            return [], []
        ids = ",".join("'" + i.replace("'", "''") + "'" for i in entity_ids)
        cond = f"(source_entity_id IN ({ids}) OR target_entity_id IN ({ids}))"
        if relationship_types:
            rels = ",".join("'" + r.replace("'", "''") + "'" for r in relationship_types)
            cond += f" AND relationship_type IN ({rels})"
        if as_of:
            ts = as_of.isoformat()
            cond += f" AND (valid_from IS NULL OR valid_from <= TIMESTAMP '{ts}') AND (valid_to IS NULL OR valid_to > TIMESTAMP '{ts}')"
        rel_rows = self.spark.sql(f"SELECT * FROM {self.prefix}.nova_relationship WHERE {cond}").collect()
        relationships = [Relationship(**r.asDict(), evidence_ids=[]) for r in rel_rows]
        node_ids = sorted({x for r in relationships for x in [r.source_entity_id, r.target_entity_id]})
        if not node_ids:
            return [], relationships
        nids = ",".join("'" + i.replace("'", "''") + "'" for i in node_ids)
        entity_rows = self.spark.sql(f"SELECT * FROM {self.prefix}.nova_entity WHERE entity_id IN ({nids})").collect()
        return [Entity(**r.asDict()) for r in entity_rows], relationships

    def get_evidence(self, evidence_ids: list[str]) -> list[Evidence]:
        if not evidence_ids:
            return []
        ids = ",".join("'" + i.replace("'", "''") + "'" for i in evidence_ids)
        rows = self.spark.sql(f"SELECT * FROM {self.prefix}.nova_evidence WHERE evidence_id IN ({ids})").collect()
        return [Evidence(**r.asDict()) for r in rows]
