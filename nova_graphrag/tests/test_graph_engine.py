from nova_graphrag.graph_store.memory import InMemoryGraphStore
from nova_graphrag.models import Entity, Relationship, GraphQuery
from nova_graphrag.graph_engine import GraphEngine


def test_expand_and_shortest_path():
    store = InMemoryGraphStore()
    store.upsert_entities([
        Entity(entity_id="a", entity_type="Policy", canonical_name="Policy"),
        Entity(entity_id="b", entity_type="Metric", canonical_name="NCF"),
        Entity(entity_id="c", entity_type="Calculation", canonical_name="Deposit Runoff"),
    ])
    store.upsert_relationships([
        Relationship(relationship_id="r1", source_entity_id="a", target_entity_id="b", relationship_type="DEFINES"),
        Relationship(relationship_id="r2", source_entity_id="b", target_entity_id="c", relationship_type="CALCULATED_FROM"),
    ])
    graph = GraphEngine(store)
    entities, rels = graph.expand(["a"], GraphQuery(text="x", max_hops=2))
    assert {e.entity_id for e in entities} == {"a", "b", "c"}
    assert {r.relationship_id for r in rels} == {"r1", "r2"}
    assert graph.shortest_path("a", "c") == ["a", "b", "c"]
