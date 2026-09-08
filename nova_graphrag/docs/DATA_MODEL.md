# Data Model

## Entity

An entity is a canonical object in the Treasury semantic model.

Minimum fields:
- entity_id
- entity_type
- canonical_name
- properties
- valid_from / valid_to
- recorded_from / recorded_to

## Relationship

A relationship is a typed edge connecting canonical entities.

Minimum fields:
- relationship_id
- source_entity_id
- target_entity_id
- relationship_type
- confidence
- materiality
- bitemporal validity
- evidence IDs

## Evidence

Evidence is first-class and not merely metadata. A relationship is not considered trusted simply because it exists.

Evidence should capture:
- source URI
- document section or table/row reference
- source version
- extraction method
- confidence
- approval state
- cryptographic excerpt/content hash where permitted

## Bitemporal model

NOVA should support:

- **Valid time**: when the fact was true in the business world
- **Recorded time**: when NOVA knew or recorded the fact

This allows historical reconstruction such as:

- What treatment was valid on 31 March 2025?
- What did NOVA know about that treatment on 5 April 2025?
- Which reports were produced before a policy correction was recorded?

## Materiality

Materiality should be optional on every relationship and usable as a retrieval filter. This prevents graph expansion from being dominated by immaterial technical links.

## Confidence and approval

LLM-extracted relationships should be stored with explicit confidence and review state. High-risk relationship classes can require deterministic extraction or human approval before being used in production answers.
