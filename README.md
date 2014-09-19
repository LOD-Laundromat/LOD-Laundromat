LOD Laundromat: Washing Machine
===============================

This is where we put dirty Linked Data in a washing machine,
add some detergent... let the thing run for a while...
and get out clean data.

Services
--------

LOD Laundromat can be run as a Linux service by using the following command:

~~~{.sh}
sudo ACTION SERVICE
~~~

`ACTION` is one of:
  - `start`
  - `stop`
  - `restart`

`SERVICE` is one of:
  - `backend`
  - `lodlaundromat-wm`



<table>
  <tr>
    <th>Location</url>
    <th>HTTP Method</url>
    <th>Arguments</url>
    <th>Extras</td>
    <th>What it does</th>
  </td>
  <tr>
    <td>http://backend.lodlaundromat.org</td>
    <td>GET</td>
    <td>Path=/;Search=[url]</td>
    <td>This is where URLs are added to Virtuoso graph ???.</td>
  <tr>
    <td>http://cliopatria.lodlaundromat.d2s.labs.vu.nl<td>
    <td>GET</td>
    <td>HTTP authentication</td>
    <td>This is used to debug the LOD Washing Machine during development.</td>
  </tr>
  <tr>
    <td>http://download.lodlaundromat.org<td>
    <td>GET</td>
    <td>Path=/MD5</td>
    <td>This is where you can download clean data from.</td>
  </tr>
  <tr>
    <td>http://sparql.backend.lodlaundromat.org<td>
    <td>?</td>
    <td>?</td>
    <td>?</td>
  </tr>
  <tr>
    <td>http://localhost:8890/sparql-auth</td>
    <td>POST</td>
    <td>Search=[query]</td>
    <td>?</td>
  </tr>
</table>

