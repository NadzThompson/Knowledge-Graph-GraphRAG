import json
from datetime import date
from nova_graph.ontology import ONTOLOGY
from nova_graph.schemas import Chunk
from nova_graph.pipeline.extraction import parse_extraction
from nova_graph.pipeline.chunking import chunk_document

def test_ontology_rules():
    assert ONTOLOGY.allowed("GOVERNED_BY", "Metric", "RegulatoryGuideline")
    assert not ONTOLOGY.allowed("GOVERNED_BY", "Currency", "Person")
    assert not ONTOLOGY.llm_allowed("EXPOSED_TO")   # deterministic only

def test_parse_extraction_validates_and_adds_provenance():
    ch = Chunk(chunk_id="c1", doc_id="OSFI-LAR", text="...", ordinal=0, doc_title="OSFI LAR", doc_type="guideline")
    raw = json.dumps({
        "entities": [{"type": "Metric", "name": "Liquidity Coverage Ratio", "key": "LCR"},
                     {"type": "RegulatoryGuideline", "name": "OSFI LAR Chapter 2", "key": "OSFI LAR Ch.2"},
                     {"type": "Alien", "name": "bad", "key": "x"}],
        "relations": [{"type": "GOVERNED_BY", "from_key": "LCR", "to_key": "OSFI LAR Ch.2", "quote": "LCR shall be..."},
                      {"type": "EXPOSED_TO", "from_key": "LCR", "to_key": "OSFI LAR Ch.2"}]})
    res, rejected = parse_extraction(ch, raw, "gpt-5", valid_from=date(2026, 1, 1))
    assert rejected == 2
    types = {n.node_type for n in res.nodes}
    assert types == {"Metric", "RegulatoryGuideline", "Document"}
    edge_types = sorted(e.edge_type for e in res.edges)
    assert edge_types == ["GOVERNED_BY", "MENTIONS", "MENTIONS"]
    gov = next(e for e in res.edges if e.edge_type == "GOVERNED_BY")
    assert gov.evidence_chunk_ids == ["c1"] and "quote" in gov.properties

def test_chunking_keeps_headings():
    text = "2.1 Scope\n" + "word " * 500 + "\n2.2 Definitions\n" + "term " * 100
    chunks = chunk_document("d1", text, target_tokens=200)
    assert len(chunks) >= 3
    assert chunks[0].text.startswith("2.1 Scope")
    assert any(c.metadata["section"] == "2.2 Definitions" for c in chunks)
