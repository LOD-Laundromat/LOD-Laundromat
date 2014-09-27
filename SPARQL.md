# More than #threads cleaning files:
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?datadoc ?startClean ?triples
WHERE {
  ?datadoc llo:startClean ?startClean .
  ?datadoc llo:triples ?triples .
  FILTER NOT EXISTS { ?datadoc llo:endClean ?endClean . }
}
