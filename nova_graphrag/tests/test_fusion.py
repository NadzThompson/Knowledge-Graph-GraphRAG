from nova_graphrag.models import SearchHit
from nova_graphrag.retrieval.fusion import reciprocal_rank_fusion


def test_rrf():
    a = [SearchHit(source="elastic", item_id="1", score=10, text="a")]
    b = [SearchHit(source="pgvector", item_id="2", score=9, text="b")]
    out = reciprocal_rank_fusion([a, b], top_k=2)
    assert len(out) == 2
