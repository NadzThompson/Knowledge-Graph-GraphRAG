import numpy as np
from nova_graph.pipeline.resolution import Candidate, resolve_block, normalise_name, blocking_key
from nova_graph.pipeline.communities import leiden_hierarchy
from nova_graph.pipeline.analytics import spectral_features
from nova_graph.pipeline.ids import node_id

def test_normalise_and_block():
    assert normalise_name("Northwind Bank of Canada Ltd.") == normalise_name("NORTHWIND BANK OF CANADA")
    assert blocking_key("LegalEntity", "Northwind Bank", {"country": "ca"}) == "LegalEntity|CA|nort"

def test_resolve_block_merges_and_picks_survivor():
    e = np.array([1.0, 0.0, 0.0]); e2 = np.array([0.99, 0.05, 0.0])
    cands = [Candidate("a", "Counterparty", "Acme Holdings Inc", "ACME1", {}, 0.9, 1, e),
             Candidate("b", "Counterparty", "ACME HOLDINGS", "ACME-1", {}, 0.8, 3, e2),
             Candidate("c", "Counterparty", "Zed Corp", "ZED", {}, 0.9, 1, np.array([0.0, 1.0, 0.0]))]
    out = resolve_block(cands)
    assert out and all(canon == "b" for _, canon, _, _ in out)
    assert {nid for nid, *_ in out} == {"a"}

def test_ids_deterministic():
    assert node_id("Metric", " lcr ") == node_id("Metric", "LCR")

def test_leiden_hierarchy_and_spectral():
    edges = [("a","b",1),("b","c",1),("a","c",1),("d","e",1),("e","f",1),("d","f",1),("c","d",0.1)]
    h = leiden_hierarchy(edges, levels=2)
    assert h[0]["a"] == h[0]["b"] and h[0]["a"] != h[0]["e"]
    f = spectral_features(edges)
    assert set(f) == {"a","b","c","d","e","f"} and f["c"]["betweenness"] > f["a"]["betweenness"]
