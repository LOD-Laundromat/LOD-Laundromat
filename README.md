LOD Laundromat: Washing Machine
===============================

This is where we put dirty Linked Data in a washing machine,
add some detergent... let the thing run for a while...
and get out clean data.

Services
--------

LOD Laundromat can be run as a Linux service by using the following command:

~~~{.sh}
sudo SERVICE ACTION
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

  - **D2S**: `d2s.labs.vu.nl`
  - `lodlaundromat.d2s.labs.vu.nl`: ???
  - **LOD Laundromat LXC**: virtual machine on `lodlaundromat.d2s.labs.vu.nl`
  - **Production server**: Hosts Web Services.
    *clouvVps*, IP: `213.187.244.59`.
    Updated by pushing to *Github*.


Endpoints
---------

<table>
  <tr>
    <th>Location</url>
    <th>HTTP Method</url>
    <th>Arguments</url>
    <th>Standards-compliance</td>
    <th>What it does</th>
  </td>
  <tr>
    <td>(http://cliopatria.lodlaundromat.d2s.labs.vu.nl)<td>
    <td>`GET`</td>
    <td>HTTP authentication</td>
    <td>Does not support RDF Datasets.</td>
    <td>This is used to debug the LOD Washing Machine during development.</td>
  </tr>
  <tr>
    <td>`http://localhost:8686`</td>
    <td>`GET`</td>
    <td>`url`</td>
    <td></td>
    <td>
      The NodeJS backend of the LOD Laundromat site,
      responsible for serving files for users,
      and adding items to the seed list.
    </td>
  </tr>
  <tr>
    <td>`http://localhost:8890/sparql`<td>
    <td>`GET`</td>
    <td>`query`</td>
    <td>SPARQL 1.1 Query</td>
    <td>
      The SPARQL endpoint that is used by the LOD Laundromat Web Services.
    </td>
  </tr>
  <tr>
    <td>`http://localhost:8890/sparql-auth`</td>
    <td>`POST`</td>
    <td>Search=[query]</td>
    <td>
      SPARQL 1.1 Protocol, SPARQL 1.1 Query, SPARQL 1.1 Update
    </td>
    <td>
      The first SPARQL Endpoint that is used by the LOD Washing Machine.
    </td>
  </tr>
  <tr>
    <td>`http://localhost:8890/sparql-graph-crud`</td>
    <td></td>
    <td></td>
    <td>SPARQL 1.1 Graph Store HTTP Protocol</td>
    <td>
      The second SPARQL Endpoint that is used by the LOD Washing Machine.
    </td>
</table>



Endpoint aliases
----------------

<table>
  <tr>
    <th>Alias</th>
    <th>Forwards to</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>`http://backend.lodlaundromat.org`</td>
    <td>`http://localhost:8686`</td>
    <td>
      Web Service for adding items to the LOD Basket
      (i.e., the LOD Laundromat seed list).
    </td>
  </tr>
  <tr>
    <td>`http://download.lodlaundromat.org`<td>
    <td>`http://localhost:8686`</td>
    <td>
      Web Service for downloading clean data files.
      The URL path must be set to `/MD5`.
    </td>
  </tr>
  <tr>
    <td>`http://lodlaundromat.org/sparql`</td>
    <td>`http://sparql.backend.lodlaundromat.org`</td>
    <td>Use to preserve consistency in the Web interface.</td>
  </tr>
  <tr>
    <td>`http://sparql.backend.lodlaundromat.org`</td>
    <td>`http://localhost:8890/sparql`</td>
    <td>Web Service for querying the LOD Laundromat metadata.</td>
  </tr>
</table>



Virtuoso RDF dataset
--------------------
