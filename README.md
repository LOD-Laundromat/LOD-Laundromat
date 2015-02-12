LOD Laundromat: Washing Machine
===============================

This is where we put dirty Linked Data in a washing machine,
add some detergent... let the thing run for a while...
and get out clean data.

Services
--------

LOD Laundromat can be run as a Linux service by using the following command:

~~~{.sh}
sudo service SERVICE ACTION
~~~

`SERVICE` is one of:
  - `backend`
  - `lodlaundromat-wm`
  - `/etc/init.d/virtuoso-opensource`

`ACTION` is one of:
  - `start`
  - `stop`
  - `restart`



Machines
--------

  - **D2S**: `ssh -X d2s.labs.vu.nl` and `ssh -X lodlaundromat.lxc`
  - `lodlaundromat.d2s.labs.vu.nl`: ???
  - **LOD Laundromat LXC**: virtual machine on `lodlaundromat.d2s.labs.vu.nl`
  - **Production server**: Hosts Web Services.
    *clouvVps*, IP: `213.187.244.59`.
    Updated by pushing to *Github*.



Endpoints
---------

Ednpoints that require authentication and/or API keys
should be registered as Web services.
This is done by include a line in a file called `service.db`.
For example:

```prolog
assert(service(service,user,password,_)).
```

| **Location** | **HTTP Method** | **Arguments** | ** Standards-compliance** | **What it does** |
|:-------------|:----------------|:--------------|:--------------------------|:-----------------|
| http://cliopatria.lodlaundromat.d2s.labs.vu.nl | `GET` | HTTP authentication | Does not support RDF Datasets. | This is used to debug the LOD Washing Machine during development. |
| http://localhost:8686 | `GET` | `url` | | The NodeJS backend of the LOD Laundromat site, responsible for serving files for users, and adding items to the seed list. |
| http://localhost:8890/sparql | `GET` | `query` | SPARQL 1.1 Query | The SPARQL endpoint that is used by the LOD Laundromat Web Services. |
| http://localhost:8890/sparql-auth | `POST` | `query` | SPARQL 1.1 Protocol, SPARQL 1.1 Query, SPARQL 1.1 Update | The first SPARQL Endpoint that is used by the LOD Washing Machine. |
| http://localhost:8890/sparql-graph-crud | | | SPARQL 1.1 Graph Store HTTP Protocol | The second SPARQL Endpoint that is used by the LOD Washing Machine. |



Endpoint aliases
----------------

| **Alias** | **Forwards to** | **Description** |
|:----------|:----------------|:----------------|
| http://backend.lodlaundromat.org | http://localhost:8686 | Web Service for adding items to the LOD Basket (i.e., the LOD Laundromat seed list). Query terms: <ol><li>`url` seed point</li><li>`type` either `archive` or `url`</li><li>`from` The URI where we found this dump (`url`=`from` iff `type`=`url`)</li><li>`lazy` Either `1` or `0`, denoting different processing modes for the node.js backend.</li></ol> |
| http://download.lodlaundromat.org | http://localhost:8686 | Web Service for downloading clean data files. The URL path must be set to `/MD5`. |
| http://lodlaundromat.org/sparql | http://sparql.backend.lodlaundromat.org | Use to preserve consistency in the Web interface. |
| http://sparql.backend.lodlaundromat.org | http://localhost:8890/sparql | Web Service for querying the LOD Laundromat metadata. |



Virtuoso RDF dataset
--------------------

```uri
http://lodlaundromat.org/ontology#error
http://lodlaundromat.org/ontology#http
http://lodlaundromat.org/ontology#llo
http://lodlaundromat.org/ontology#llm
http://lodlaundromat.org#metrics-11
http://lodlaundromat.org#11
```



SSH config file
---------------

File path: `~/.ssh/config`

~~~
Host *
  ServerAliveInterval 60
~~~



Subdomains
----------

| *Port* | *Subdomain* |
|:------:|:------------|
| 4001   | cliopatria  |
| 4002   | bertrand    |
| 4003   | webqr       |
| 4004   | datahives   |
| 4005   | iotw        |
| 4006   | su          |
| 4007   | lodobs      |
| 4008   | tools       |
| 4009   | dans        |
