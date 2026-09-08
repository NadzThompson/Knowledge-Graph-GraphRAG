from nova_graph.serving.retriever import GraphRAG

class _G:  # minimal stubs
    pass

def test_mode_picker():
    r = GraphRAG.__new__(GraphRAG)
    assert r._pick_mode("Which upstream tables feed LCR line 12?", None) == "lineage"
    assert r._pick_mode("How is Northwind Europe connected to Counterparty X?", None) == "path"
    assert r._pick_mode("Summarise the themes across all subsidiaries", None) == "global"
    assert r._pick_mode("What is the NSFR treatment of covered bonds?", None) == "local"
