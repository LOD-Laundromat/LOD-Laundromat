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
