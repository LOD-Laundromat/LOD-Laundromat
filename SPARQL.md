SPARQL endpoint test
--------------------

```sparql
SELECT * WHERE { ?s ?p ?o }
```

```bash
curl -H "Accept: application/sparql-results+json" "http://localhost:3020/sparql/?query=SELECT%20*%20WHERE%20%7B%20%3Fs%20%3Fp%20%3Fo%20%7D%20LIMIT%201"
```



# More than #threads cleaning files:

```sparql
PREFIX llo: <http://lodlaundromat.org/ontology/>
SELECT ?datadoc ?startClean ?triples
WHERE {
  ?datadoc llo:startClean ?startClean .
  ?datadoc llo:triples ?triples .
  FILTER NOT EXISTS { ?datadoc llo:endClean ?endClean . }
}
```



# More than #threads cleaning files:

```sparql
PREFIX llo: <http://lodlaundromat.org/ontology/>
SELECT ?datadoc ?startClean
WHERE {
  ?datadoc llo:startClean ?startClean .
  FILTER NOT EXISTS { ?datadoc llo:endClean ?endClean . }
}
ORDER BY ASC(?startClean)
```



# Dataset with permission error

```sparql
PREFIX httpo: <http://lodlaundromat.org/http/ontology/>
PREFIX llo: <http://lodlaundromat.org/ontology/>
SELECT ?dataset ?url
WHERE {
  ?dataset llo:exception httpo:409 .
  ?dataset llo:url ?url .
}
LIMIT 1
```



# Sorted by number of duplicates.

```sparql
PREFIX llo: <http://lodlaundromat.org/ontology/>
SELECT DISTINCT ?dups ?triples ?url
WHERE {
  ?dataset llo:duplicates ?dups .
  ?dataset llo:triples ?triples .
  ?dataset llo:url ?url .
}
ORDER BY DESC(?dups)
```

```sparql
PREFIX error: <http://lodlaundromat.org/error/ontology/>
PREFIX llo: <http://lodlaundromat.org/ontology/>
PREFIX ll: <http://lodlaundromat/org/resource/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT distinct (COUNT(?datadoc) AS ?count) {
  ?datadoc rdf:type llo:ArchiveEntry .
  FILTER NOT EXISTS {
    ?parent llo:containsEntry ?datadoc .
  }
}
```

```sparql
PREFIX error: <http://lodlaundromat.org/error/ontology/>
PREFIX llo: <http://lodlaundromat.org/ontology/>
PREFIX ll: <http://lodlaundromat/org/resource/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT distinct ?datadoc {
  ?datadoc rdf:type llo:ArchiveEntry .
  FILTER NOT EXISTS {
    ?parent llo:containsEntry ?datadoc .
  }
}
LIMIT 25
```
