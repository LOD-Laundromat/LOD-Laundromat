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
    <td><a href="http://cliopatria.lodlaundromat.d2s.labs.vu.nl">http://cliopatria.lodlaundromat.d2s.labs.vu.nl</a></td>
    <td><code>GET</code></td>
    <td>HTTP authentication</td>
    <td>Does not support RDF Datasets.</td>
    <td>This is used to debug the LOD Washing Machine during development.</td>
  </tr>
  <tr>
    <td><code>http://localhost:8686</code></td>
    <td><code>GET</code></td>
    <td><code>url</code></td>
    <td></td>
    <td>
      The NodeJS backend of the LOD Laundromat site,
      responsible for serving files for users,
      and adding items to the seed list.
    </td>
  </tr>
  <tr>
    <td><code>http://localhost:8890/sparql</code></td>
    <td><code>GET</code></td>
    <td><code>query</code></td>
    <td>SPARQL 1.1 Query</td>
    <td>
      The SPARQL endpoint that is used by the LOD Laundromat Web Services.
    </td>
  </tr>
  <tr>
    <td><code>http://localhost:8890/sparql-auth</code></td>
    <td><code>POST</code></td>
    <td><code>query</code></td>
    <td>SPARQL 1.1 Protocol, SPARQL 1.1 Query, SPARQL 1.1 Update</td>
    <td>
      The first SPARQL Endpoint that is used by the LOD Washing Machine.
    </td>
  </tr>
  <tr>
    <td><code>http://localhost:8890/sparql-graph-crud</code></td>
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
    <td><a href="http://backend.lodlaundromat.org">http://backend.lodlaundromat.org</a></td>
    <td><code>http://localhost:8686</code></td>
    <td>
      Web Service for adding items to the LOD Basket
      (i.e., the LOD Laundromat seed list).
    </td>
  </tr>
  <tr>
    <td><a href="http://download.lodlaundromat.org">http://download.lodlaundromat.org</a></td>
    <td><code>http://localhost:8686</code></td>
    <td>
      Web Service for downloading clean data files.
      The URL path must be set to <code>/MD5</code>.
    </td>
  </tr>
  <tr>
    <td><a href="http://lodlaundromat.org/sparql">http://lodlaundromat.org/sparql</a></td>
    <td><a href="http://sparql.backend.lodlaundromat.org">http://sparql.backend.lodlaundromat.org</a></td>
    <td>Use to preserve consistency in the Web interface.</td>
  </tr>
  <tr>
    <td><a href="http://sparql.backend.lodlaundromat.org">http://sparql.backend.lodlaundromat.org</a></td>
    <td><code>http://localhost:8890/sparql</code></td>
    <td>Web Service for querying the LOD Laundromat metadata.</td>
  </tr>
</table>



Virtuoso RDF dataset
--------------------

SSH config file
---------------

File path: `~/.ssh/config`

~~~
Host *
  ServerAliveInterval 60
~~~
