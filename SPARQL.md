PREFIX ll:  <http://lodlaundromat.org/resource/>
PREFIX llo: <http://lodlaundromat.org/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT DISTINCT ?size
WHERE {
  ?md5 llo:endUnpack ?end .
  FILTER NOT EXISTS {
    ?md5 llo:startClean ?clean .
  }
  ?md5 llo:size ?size .
}
ORDER BY DESC(?size)



PREFIX ll:  <http://lodlaundromat.org/resource/>
PREFIX llo: <http://lodlaundromat.org/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT (COUNT(?md5) AS ?n)
WHERE {
  ?md5 llo:url ?url .
  FILTER NOT EXISTS {
    ?md5 llo:startUnpack ?clean .
  }
}
