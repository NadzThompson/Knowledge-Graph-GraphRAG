"""Structure-aware chunking for regulatory and policy documents."""
from __future__ import annotations
import re
from .ids import chunk_id
from ..schemas import Chunk

HEADING = re.compile(r"^(\d+(\.\d+)*\s+.+|[A-Z][A-Z0-9 ,\-()]{6,})$")

def chunk_document(doc_id: str, text: str, doc_title: str = "", doc_type: str = "",
                   target_tokens: int = 350, overlap_tokens: int = 40, masking_tier: int = 0) -> list[Chunk]:
    """Split on headings/paragraphs, then pack to ~target_tokens with overlap.
    Section headings are prepended to each chunk so citations carry their context."""
    lines = text.splitlines()
    sections: list[tuple[str, list[str]]] = [("", [])]
    for ln in lines:
        s = ln.strip()
        if not s:
            continue
        if HEADING.match(s) and len(s) < 120:
            sections.append((s, []))
        else:
            sections[-1][1].append(s)
    chunks: list[Chunk] = []
    ordinal = 0
    for heading, paras in sections:
        buf: list[str] = []
        count = 0
        for p in _split_long(paras, target_tokens):
            n = len(p.split())
            if count + n > target_tokens and buf:
                chunks.append(_mk(doc_id, ordinal, heading, buf, doc_title, doc_type, masking_tier)); ordinal += 1
                tail = " ".join(" ".join(buf).split()[-overlap_tokens:])
                buf, count = [tail], len(tail.split())
            buf.append(p); count += n
        if buf:
            chunks.append(_mk(doc_id, ordinal, heading, buf, doc_title, doc_type, masking_tier)); ordinal += 1
    return chunks

def _split_long(paras: list[str], target: int) -> list[str]:
    """Break paragraphs longer than target into sentence-bounded pieces."""
    out: list[str] = []
    for p in paras:
        words = p.split()
        if len(words) <= target:
            out.append(p); continue
        sentences = re.split(r"(?<=[\.\!\?;])\s+", p)
        buf, cnt = [], 0
        for s_ in sentences:
            w = s_.split()
            while len(w) > target:                      # sentence itself too long: hard cut
                out.append(" ".join(w[:target])); w = w[target:]
            if cnt + len(w) > target and buf:
                out.append(" ".join(buf)); buf, cnt = [], 0
            buf.append(" ".join(w)); cnt += len(w)
        if buf: out.append(" ".join(buf))
    return out


def _mk(doc_id, ordinal, heading, buf, title, dtype, tier) -> Chunk:
    body = (heading + "\n" if heading else "") + "\n".join(buf)
    return Chunk(chunk_id=chunk_id(doc_id, ordinal), doc_id=doc_id, ordinal=ordinal, text=body,
                 doc_title=title, doc_type=dtype, masking_tier=tier, metadata={"section": heading})
