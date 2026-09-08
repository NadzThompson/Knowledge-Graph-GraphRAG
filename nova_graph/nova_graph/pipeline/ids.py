import hashlib

def node_id(node_type: str, canonical_key: str) -> str:
    """Deterministic node id: same entity from any source lands on the same id."""
    return "n_" + hashlib.sha1(f"{node_type}|{canonical_key.strip().upper()}".encode()).hexdigest()[:20]

def edge_id(src: str, dst: str, edge_type: str, valid_from: str = "") -> str:
    return "e_" + hashlib.sha1(f"{src}|{edge_type}|{dst}|{valid_from}".encode()).hexdigest()[:20]

def chunk_id(doc_id: str, ordinal: int) -> str:
    return "c_" + hashlib.sha1(f"{doc_id}|{ordinal}".encode()).hexdigest()[:20]
