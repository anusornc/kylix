# Kylix

A permissioned blockchain for provenance tracking, built from scratch for a PhD project. It uses PROV-O as its ontology schema, a DAG for storage, Proof of Authority (PoA) with round-robin validation, and a knowledge graph for querying.

## Vision
Kylix aims to provide a scalable, semantic blockchain for tracking provenance in a permissioned network. Participants (validators) are trusted based on a "knows" relationship, ensuring a need-to-know basis.

## Features
- **Ontology**: PROV-O triples (e.g., `<entity> <wasGeneratedBy> <activity>`).
- **DAG**: Directed Acyclic Graph for parallel transaction processing.
- **PoA**: Validators sign transactions in a round-robin order.
- **Knowledge Graph**: Query provenance data with triple patterns.
- **Permissioned**: New validators must be known by an existing participant.

## Usage
1. Start: `iex -S mix`
2. Add transaction: `Kylix.add_transaction("entity1", "wasGeneratedBy", "activity1", "agent1", "valid_sig")`
3. Query: `Kylix.query({"entity1", "wasGeneratedBy", nil})`
4. Add validator: `Kylix.add_validator("agent4", "pubkey4", "agent1")`
5. Validators: `Kylix.get_validators()`

## Tests
```bash
mix test