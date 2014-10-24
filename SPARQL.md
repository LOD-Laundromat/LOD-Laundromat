# More than #threads cleaning files:
PREFIX llo: <http://lodlaundromat.org/ontology/>
SELECT ?datadoc ?startClean ?triples
WHERE {
  ?datadoc llo:startClean ?startClean .
  ?datadoc llo:triples ?triples .
  FILTER NOT EXISTS { ?datadoc llo:endClean ?endClean . }
}

# More than #threads cleaning files:
PREFIX llo: <http://lodlaundromat.org/ontology/>
SELECT ?datadoc ?startClean
WHERE {
  ?datadoc llo:startClean ?startClean .
  FILTER NOT EXISTS { ?datadoc llo:endClean ?endClean . }
}
ORDER BY ASC(?startClean)

# Dataset with permission error
PREFIX httpo: <http://lodlaundromat.org/http/ontology/>
PREFIX llo: <http://lodlaundromat.org/ontology/>
SELECT ?dataset ?url
WHERE {
  ?dataset llo:exception httpo:409 .
  ?dataset llo:url ?url .
}
LIMIT 1

# Sorted by number of duplicates.
PREFIX llo: <http://lodlaundromat.org/ontology/>
SELECT DISTINCT ?dups ?triples ?url
WHERE {
  ?dataset llo:duplicates ?dups .
  ?dataset llo:triples ?triples .
  ?dataset llo:url ?url .
}
ORDER BY DESC(?dups)
