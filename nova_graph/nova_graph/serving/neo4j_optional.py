"""Optional Neo4j adapter. Enabled only when optional.neo4j_enabled=true; used for hops >= threshold."""
from __future__ import annotations

class Neo4jExpander:
    def __init__(self, uri: str, auth):
        from neo4j import GraphDatabase
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def expand(self, seeds, hops, as_of, max_tier):
        q = f"""
        MATCH (s) WHERE s.node_id IN $seeds
        MATCH p=(s)-[r*1..{int(hops)}]->(t)
        WHERE ALL(x IN r WHERE x.masking_tier <= $tier AND ($as_of IS NULL OR (x.valid_from <= date($as_of) AND (x.valid_to IS NULL OR x.valid_to > date($as_of)))))
          AND t.masking_tier <= $tier
        RETURN s.node_id AS src, t.node_id AS dst, length(p) AS hops, [n IN nodes(p) | n.node_id] AS path,
               [x IN r | type(x)] AS edge_types, reduce(w=1.0, x IN r | w * coalesce(x.weight,1.0)) AS weight
        LIMIT 500"""
        with self.driver.session() as s:
            return [(r["src"], r["dst"], r["hops"], r["path"], r["edge_types"], r["weight"])
                    for r in s.run(q, seeds=list(seeds), tier=max_tier, as_of=as_of.isoformat() if as_of else None)]
