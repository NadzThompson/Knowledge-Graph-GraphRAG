# 09 Roadmap

Phase 1 (build): structured edges for LE, GL, report lines, exposures, metrics; OSFI corpus extraction; local + lineage modes; Postgres sync; TreasuryNavigator wired.
Phase 2 (global): communities and summaries; global/drift modes; ScenarioSimulator uses closures; quality gates in CI.
Phase 3 (temporal + risk): daily exposure snapshots; as-of retrieval in all agents; spectral and magnitude features in RiskCalculator; GNN risk score trained on Databricks (PyTorch Geometric) and written to `node_features.gnn_risk_score`.
Phase 4 (scale): partition Postgres by node_type; `halfvec`; cuGraph for large components; optional Neo4j evaluation against measured hop distribution from `retrieval_log`.

GSRT hooks: `analytics.spectral_features` already emits Fiedler coordinates, spectral gap and magnitude contributions per node and date. Extend with persistent-homology features over exposure filtrations and feed them to the same table so agents can cite them.
