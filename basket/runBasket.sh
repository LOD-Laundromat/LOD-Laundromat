#!/bin/bash

# Non-HTTP(S) URLs that are currently not used.
# file:///var/www/vhosts/wildcard.rkbexplorer.com/repositories/void.rkbexplorer.com/models/5/d/2/0/5d20381b766258b7e05ea0560f826b8d.ttl#DS1
# file:///var/www/vhosts/wildcard.rkbexplorer.com/repositories/void.rkbexplorer.com/models/5/d/2/0/5d20381b766258b7e05ea0560f826b8d.ttl#DS2
# file:///var/www/vhosts/wildcard.rkbexplorer.com/repositories/void.rkbexplorer.com/models/a/6/e/b/a6ebae7f00baebe54c51cde89df0b2b3.ttl#DS1
# ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/rdf/taxonomy.rdf.gz
# ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/rdf/uniprot.rdf.gz
# ttp://rod.eionet.europa.eu/issues

scheme="http"

# Choose one authority.
####authority="localhost:3040"
authority="lodlaundry.wbeek.ops.few.vu.nl"
####authority="virtuoso.lodlaundromat.ops.few.vu.nl"

path="/basket"

# The input file with one URL per line.
input=url.data

while read line; do
  command="curl --data \"url=${line}\" ${scheme}://${authority}${path}";
  eval $command;
  # For debugging purposes, the command can be written to standard output.
  ####echo $command;
done < "$input"

