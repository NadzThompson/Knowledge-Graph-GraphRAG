"""Masking tier resolution: maps a caller's RBAC/ABAC claims to the maximum tier they may see.

Tiers (aligned with NOVA's four-tier data classification):
  0 internal   1 confidential   2 restricted (counterparty, positions)   3 highly restricted (people, PII)
"""
from __future__ import annotations
from dataclasses import dataclass, field

ROLE_TIERS = {"treasury_viewer": 1, "treasury_analyst": 2, "treasury_director": 3, "nova_admin": 3, "auditor": 3}

@dataclass
class Caller:
    user_id: str
    roles: list[str] = field(default_factory=list)
    attributes: dict = field(default_factory=dict)   # e.g. {"legal_entities": [...], "region": "CA"}

    @property
    def max_tier(self) -> int:
        return max([ROLE_TIERS.get(r, 0) for r in self.roles] + [0])

def redact_hits(hits: list, max_tier: int) -> list:
    return [h for h in hits if int(h.metadata.get("masking_tier", 0)) <= max_tier]
