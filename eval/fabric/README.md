# Fabric opaque-payload baseline

This is the Fabric half of the evaluation package. A replicator puts the
same fifteen Fig1 facts on a **one-channel test network** as opaque
payloads so the correctness table’s Fabric column is fair: on-chain
lineage questions are **not answerable**.

Kylix records Fig1 as signed PROV-O Transactions
(`eval/fig1-transactions.json` via `Kylix.add_transaction`) and asks the
lineage suite at `Kylix.Query.SparqlEngine.execute`. Fabric stores those
same facts as uninterpreted bytes. Nothing in this procedure treats
Fabric as a SPARQL store.

Do not implement production chaincode, stand up a multi-org network, or
add CouchDB selectors / graph walks. A stub that only calls `PutState`
and `GetState` is enough. Single-org, one channel.

## PutState

Write **fifteen** world-state keys. Values are the corresponding `{s,p,o}`
fact from `eval/fig1-transactions.json`, encoded as **opaque** bytes
(JSON, protobuf, or raw UTF-8 — the encoding does not matter because
nothing on-chain interprets it). One `PutState` blob per Fig1 fact,
keyed by transaction id.

| Key | Opaque value (`s` `p` `o`) |
| --- | --- |
| `fig1-tx-01` | `entity:cleaned-data` `prov:wasGeneratedBy` `activity:clean` |
| `fig1-tx-02` | `activity:clean` `prov:used` `entity:raw-measurements` |
| `fig1-tx-03` | `entity:cleaned-data` `prov:wasDerivedFrom` `entity:raw-measurements` |
| `fig1-tx-04` | `entity:model` `prov:wasGeneratedBy` `activity:analyze` |
| `fig1-tx-05` | `activity:analyze` `prov:used` `entity:cleaned-data` |
| `fig1-tx-06` | `entity:model` `prov:wasDerivedFrom` `entity:cleaned-data` |
| `fig1-tx-07` | `entity:fig1` `prov:wasGeneratedBy` `activity:plot` |
| `fig1-tx-08` | `activity:plot` `prov:used` `entity:model` |
| `fig1-tx-09` | `entity:fig1` `prov:wasDerivedFrom` `entity:model` |
| `fig1-tx-10` | `entity:cleaned-data` `prov:wasAttributedTo` `agent:alice` |
| `fig1-tx-11` | `entity:model` `prov:wasAttributedTo` `agent:alice` |
| `fig1-tx-12` | `entity:fig1` `prov:wasAttributedTo` `agent:alice` |
| `fig1-tx-13` | `activity:clean` `prov:wasAssociatedWith` `agent:alice` |
| `fig1-tx-14` | `activity:analyze` `prov:wasAssociatedWith` `agent:alice` |
| `fig1-tx-15` | `activity:plot` `prov:wasAssociatedWith` `agent:alice` |

These are the only fifteen facts. Do not add a sixteenth fact.

## GetState

`GetState` by id returns the opaque bytes for that key.
`GetState("fig1-tx-07")` yields the fig1 / `prov:wasGeneratedBy` /
plot bytes. It does not bind `?activity` for “what generated fig1”
unless the caller already knows the key.

The lineage suite (`eval/queries/`) is seven SPARQL questions —
generated-by, used, one-hop derived-from, the three-triple derived-from
chain, attributed-to, activity outputs, and COUNT — that join and bind
variables. **GetState-by-id cannot answer the lineage suite.** On-chain
each row is **not answerable**.

The baseline uses no CouchDB selectors or graph chaincode. Those would
interpret the bytes as a graph and cease to be an opaque-payload
baseline.

## Optional external index

An off-chain scan — `GetState` of all fifteen keys, parse the bytes
*outside* Fabric, then ask the lineage questions in a local store — can
recover the same bindings as `eval/expected/`. That scan is an
**external index**, not a Fabric query capability. It is the work Kylix
does natively on Transactions.

## Correctness table

Re-run the Kylix half with `eval/run_kylix.sh`. Fill the Fabric
(on-chain) column from this procedure:

| Query | Fabric (on-chain) |
| --- | --- |
| generated-by | not answerable |
| used | not answerable |
| derived-from | not answerable |
| derived-from-chain | not answerable |
| attributed-to | not answerable |
| activity-outputs | not answerable |
| count | not answerable |
