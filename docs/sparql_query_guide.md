# Kylix SPARQL Query Guide

This document provides a comprehensive guide to using SPARQL queries with the Kylix blockchain.

## Introduction to SPARQL

SPARQL (SPARQL Protocol and RDF Query Language) is a powerful query language for RDF data. In Kylix, each blockchain transaction is stored as an RDF triple with subject, predicate, and object, making SPARQL an ideal way to query the data.

## Basic Concepts

### Triple Patterns

The core of a SPARQL query is the triple pattern. Each pattern consists of:
- Subject: The entity the statement is about
- Predicate: The relationship or property
- Object: The value or related entity

Variables in SPARQL start with a question mark (e.g., `?s`, `?p`, `?o`).

## Basic Query Structure

A simple SPARQL query looks like this:

```sparql
SELECT ?s ?p ?o
WHERE {
  ?s ?p ?o .
}
```

This will return all triples in the database.

## Common Query Patterns

### Query for Specific Subject

```sparql
SELECT ?predicate ?object
WHERE {
  "Alice" ?predicate ?object .
}
```

### Query for Specific Relationship

```sparql
SELECT ?subject ?object
WHERE {
  ?subject "knows" ?object .
}
```

### Query with Multiple Patterns

```sparql
SELECT ?person ?interest
WHERE {
  ?person "knows" "Bob" .
  ?person "likes" ?interest .
}
```
This returns all interests of people who know Bob.

## Advanced Features

### FILTER

Filters limit results based on specified conditions:

```sparql
SELECT ?person ?interest
WHERE {
  ?person "likes" ?interest .
  FILTER(?interest = "Coffee")
}
```

### OPTIONAL

The OPTIONAL keyword makes a pattern optional, similar to a LEFT JOIN in SQL:

```sparql
SELECT ?person ?friend ?email
WHERE {
  ?person "knows" ?friend .
  OPTIONAL { ?friend "email" ?email }
}
```
This returns all person-friend relationships, with email addresses where available.

### UNION

UNION combines results from alternative patterns:

```sparql
SELECT ?person ?relation ?target
WHERE {
  { ?person "knows" ?target } UNION { ?person "likes" ?target }
}
```
This returns both "knows" and "likes" relationships.

### Aggregation Functions

SPARQL supports various aggregation functions:

```sparql
SELECT ?person (COUNT(?friend) AS ?friendCount)
WHERE {
  ?person "knows" ?friend
} GROUP BY ?person
```

Available aggregation functions:
- COUNT: Count values
- SUM: Sum numeric values
- AVG: Calculate average
- MIN: Find minimum value
- MAX: Find maximum value
- GROUP_CONCAT: Concatenate values into a string

### ORDER BY

Order results by one or more variables:

```sparql
SELECT ?person ?friend
WHERE {
  ?person "knows" ?friend
} ORDER BY ?person DESC(?friend)
```

### LIMIT and OFFSET

Limit the number of results returned:

```sparql
SELECT ?s ?p ?o
WHERE {
  ?s ?p ?o
} LIMIT 10 OFFSET 20
```
This returns results 21-30.

## Advanced Query Examples

### Finding Paths in the Graph

```sparql
SELECT ?person ?friendOfFriend
WHERE {
  ?person "knows" ?friend .
  ?friend "knows" ?friendOfFriend .
  FILTER(?person != ?friendOfFriend)
}
```
This finds all friend-of-friend relationships.

### Complex Analysis with Aggregation and Filtering

```sparql
SELECT ?person (COUNT(?friend) AS ?friendCount) (GROUP_CONCAT(?interest) AS ?interests)
WHERE {
  ?person "knows" ?friend .
  OPTIONAL { ?person "likes" ?interest }
  FILTER(?person != "Dave")
} GROUP BY ?person
  HAVING (COUNT(?friend) > 1)
  ORDER BY DESC(?friendCount)
  LIMIT 5
```
This finds the top 5 most connected people (except Dave), counting their friends and listing their interests.

## Using SPARQL in Kylix

### API Endpoint

SPARQL queries can be executed against the Kylix blockchain via:

```
POST /api/sparql
Content-Type: application/sparql-query

SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10
```

### Response Format

Responses are returned in JSON format:

```json
{
  "results": {
    "bindings": [
      {
        "s": { "value": "Alice" },
        "p": { "value": "knows" },
        "o": { "value": "Bob" }
      },
      ...
    ]
  }
}
```

## Performance Considerations

- Use specific patterns rather than general ones when possible
- Place the most selective patterns (those returning fewer results) first
- Use FILTER clauses to reduce intermediate results
- Limit the number of OPTIONAL patterns, as they can be expensive
- For large result sets, use LIMIT and OFFSET for pagination

## SPARQL and Blockchain Validation

When querying transaction data, be aware of these blockchain-specific aspects:

- Each transaction has validator, timestamp, and signature metadata
- The query engine can access transaction chains via edge information
- You can query transaction provenance and lineage

Example query to find transaction chains:

```sparql
SELECT ?tx1 ?tx2 ?tx3
WHERE {
  ?tx1 "confirms" ?tx2 .
  ?tx2 "confirms" ?tx3
}
```

## Additional Resources

- [SPARQL 1.1 Query Language Specification](https://www.w3.org/TR/sparql11-query/)
- [RDF Primer](https://www.w3.org/TR/rdf11-primer/)
- [Kylix API Documentation](https://example.com/kylix/api) (internal link)