"""Fixed treasury ontology (FIBO-aligned, bank-specific extensions).

Every node and edge written to the graph must validate against this module.
A fixed ontology is what makes LLM extraction auditable and keeps entity
resolution tractable at bank scale: the LLM chooses among known types, it
does not invent them.
"""
from __future__ import annotations
from enum import Enum
from dataclasses import dataclass, field


class NodeType(str, Enum):
    LegalEntity = "LegalEntity"
    Counterparty = "Counterparty"
    BusinessUnit = "BusinessUnit"
    Portfolio = "Portfolio"
    Product = "Product"
    Instrument = "Instrument"
    GLAccount = "GLAccount"
    Metric = "Metric"                 # LCR, NSFR, HQLA, NII, EVE, etc.
    RegulatoryReport = "RegulatoryReport"   # LCR return, NCCF, FR 2052a
    RegulatoryGuideline = "RegulatoryGuideline"  # OSFI LAR, Basel III, FRB Reg YY
    Regulator = "Regulator"
    Policy = "Policy"
    Committee = "Committee"
    Person = "Person"                 # role holders only, masked tier 3
    Currency = "Currency"
    Jurisdiction = "Jurisdiction"
    Scenario = "Scenario"
    DataAsset = "DataAsset"           # Delta table, Snowflake view, Kyvos cube, report field
    Document = "Document"
    Event = "Event"                   # rate decision, downgrade, limit breach


class EdgeType(str, Enum):
    PARENT_OF = "PARENT_OF"
    OWNS = "OWNS"
    BOOKS_IN = "BOOKS_IN"
    EXPOSED_TO = "EXPOSED_TO"         # weighted by exposure amount
    FUNDS = "FUNDS"
    HEDGES = "HEDGES"
    ROLLS_UP_TO = "ROLLS_UP_TO"       # GL / metric hierarchies
    MAPS_TO_LINE = "MAPS_TO_LINE"     # GL account -> report line
    REPORTED_IN = "REPORTED_IN"
    GOVERNED_BY = "GOVERNED_BY"
    ISSUED_BY = "ISSUED_BY"
    REFERENCES = "REFERENCES"
    SUPERSEDES = "SUPERSEDES"
    DERIVED_FROM = "DERIVED_FROM"     # Unity Catalog lineage
    APPLIES_TO = "APPLIES_TO"
    MEMBER_OF = "MEMBER_OF"
    DENOMINATED_IN = "DENOMINATED_IN"
    LOCATED_IN = "LOCATED_IN"
    TRIGGERS = "TRIGGERS"
    MENTIONS = "MENTIONS"             # document/chunk -> entity
    SAME_AS = "SAME_AS"               # entity resolution output
    BELONGS_TO_COMMUNITY = "BELONGS_TO_COMMUNITY"


@dataclass(frozen=True)
class EdgeRule:
    edge: EdgeType
    src: tuple[NodeType, ...]
    dst: tuple[NodeType, ...]
    weighted: bool = False
    from_llm: bool = True   # False => only deterministic pipelines may create it


@dataclass
class Ontology:
    node_types: tuple[NodeType, ...]
    rules: tuple[EdgeRule, ...]
    masking_tier_default: dict[NodeType, int] = field(default_factory=dict)

    def allowed(self, edge: str, src_type: str, dst_type: str) -> bool:
        for r in self.rules:
            if r.edge.value == edge and NodeType(src_type) in r.src and NodeType(dst_type) in r.dst:
                return True
        return False

    def llm_allowed(self, edge: str) -> bool:
        return any(r.edge.value == edge and r.from_llm for r in self.rules)

    def prompt_schema(self) -> dict:
        """Compact JSON schema handed to the extraction LLM."""
        return {
            "node_types": [n.value for n in self.node_types],
            "edge_types": [
                {"type": r.edge.value, "from": [s.value for s in r.src], "to": [d.value for d in r.dst]}
                for r in self.rules if r.from_llm
            ],
        }


N = NodeType
ONTOLOGY = Ontology(
    node_types=tuple(NodeType),
    rules=(
        EdgeRule(EdgeType.PARENT_OF, (N.LegalEntity,), (N.LegalEntity,), from_llm=False),
        EdgeRule(EdgeType.OWNS, (N.LegalEntity, N.BusinessUnit), (N.Portfolio, N.BusinessUnit), from_llm=False),
        EdgeRule(EdgeType.BOOKS_IN, (N.Portfolio, N.Instrument), (N.LegalEntity, N.GLAccount), from_llm=False),
        EdgeRule(EdgeType.EXPOSED_TO, (N.LegalEntity, N.Portfolio), (N.Counterparty, N.LegalEntity), weighted=True, from_llm=False),
        EdgeRule(EdgeType.FUNDS, (N.LegalEntity, N.Product), (N.LegalEntity, N.Portfolio), weighted=True),
        EdgeRule(EdgeType.HEDGES, (N.Instrument, N.Portfolio), (N.Portfolio, N.Metric)),
        EdgeRule(EdgeType.ROLLS_UP_TO, (N.GLAccount, N.Metric, N.BusinessUnit), (N.GLAccount, N.Metric, N.BusinessUnit), from_llm=False),
        EdgeRule(EdgeType.MAPS_TO_LINE, (N.GLAccount, N.Metric), (N.RegulatoryReport,), from_llm=False),
        EdgeRule(EdgeType.REPORTED_IN, (N.Metric, N.LegalEntity), (N.RegulatoryReport,)),
        EdgeRule(EdgeType.GOVERNED_BY, (N.Metric, N.Product, N.LegalEntity, N.Policy, N.RegulatoryReport), (N.RegulatoryGuideline, N.Policy)),
        EdgeRule(EdgeType.ISSUED_BY, (N.RegulatoryGuideline, N.RegulatoryReport), (N.Regulator,)),
        EdgeRule(EdgeType.REFERENCES, (N.RegulatoryGuideline, N.Policy, N.Document), (N.RegulatoryGuideline, N.Policy, N.Metric, N.RegulatoryReport)),
        EdgeRule(EdgeType.SUPERSEDES, (N.RegulatoryGuideline, N.Policy), (N.RegulatoryGuideline, N.Policy)),
        EdgeRule(EdgeType.DERIVED_FROM, (N.DataAsset, N.Metric, N.RegulatoryReport), (N.DataAsset,), from_llm=False),
        EdgeRule(EdgeType.APPLIES_TO, (N.RegulatoryGuideline, N.Policy, N.Scenario), (N.LegalEntity, N.Product, N.Metric, N.Jurisdiction)),
        EdgeRule(EdgeType.MEMBER_OF, (N.Person,), (N.Committee, N.BusinessUnit)),
        EdgeRule(EdgeType.DENOMINATED_IN, (N.Instrument, N.Portfolio, N.Metric), (N.Currency,), from_llm=False),
        EdgeRule(EdgeType.LOCATED_IN, (N.LegalEntity, N.Counterparty, N.Regulator), (N.Jurisdiction,)),
        EdgeRule(EdgeType.TRIGGERS, (N.Event, N.Scenario), (N.Metric, N.Policy, N.Event)),
        EdgeRule(EdgeType.MENTIONS, (N.Document,), tuple(NodeType), from_llm=False),
        EdgeRule(EdgeType.SAME_AS, tuple(NodeType), tuple(NodeType), from_llm=False),
        EdgeRule(EdgeType.BELONGS_TO_COMMUNITY, tuple(NodeType), tuple(NodeType), from_llm=False),
    ),
    masking_tier_default={N.Person: 3, N.Counterparty: 2, N.Instrument: 2, N.Portfolio: 1},
)
