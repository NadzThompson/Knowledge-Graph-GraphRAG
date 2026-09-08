from __future__ import annotations
from nova_graphrag.models import SearchHit


def reciprocal_rank_fusion(result_sets: list[list[SearchHit]], k: int = 60, top_k: int = 20) -> list[SearchHit]:
    scores: dict[tuple[str, str], float] = {}
    by_key: dict[tuple[str, str], SearchHit] = {}
    for results in result_sets:
        for rank, hit in enumerate(results, start=1):
            key = (hit.source, hit.item_id)
            scores[key] = scores.get(key, 0.0) + 1.0 / (k + rank)
            by_key[key] = hit
    ordered = sorted(scores, key=scores.get, reverse=True)[:top_k]
    return [by_key[key].model_copy(update={"score": scores[key]}) for key in ordered]
