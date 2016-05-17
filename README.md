LOD Laundromat
==============

Cleaning Other People's Dirty Data
----------------------------------

This is where we put dirty Linked Data in a washing machine, add some detergent... let the thing run for a while... and get out clean data.


Seedlist HTTP API
-----------------

| *Path*             | *Method* | *Media type*        | *Status codes* |
|:-------------------|:---------|:--------------------|:---------------|
| `/seedlist`        | `GET`    | `appplication/json` | 200            |
| `/seedlist`        | `GET`    | `text/html`         | 200            |
| `/seedlist`        | `POST`   | `application/json`  | 201, 409       |
| `/seedlist/$HASH$` | `DELETE` |                     | 404            |
| `/seedlist/$HASH$` | `GET`    | `application/json`  | 200, 404       |
| `/seedlist/$HASH$` | `GET`    | `text/html`         | 200, 404       |


Washing Machine HTTP API
-------------------------

| *Path*         | *Method* | *Media type*         | *Status codes* |
|:---------------|:---------|:---------------------|:---------------|
| `/laundry`     | `POST`   | `application/json`   | 201            |
| `/laundry/MD5` | `DELETE` |                      |                |
| `/laundry/MD5` | `GET`    | `application/nquads` | 200            |
| `/laundry/MD5` | `GET`    | `text/html`          | 200            |


TODO
----

  - error(io_error(read,<STREAM>),context(rdf_ntriples:read_ntriple/2,'Invalid argument'))
  - Minimally encoded IRIs (LOD Laundromat IRIs, LIRIs).
  - Calculate data MD5 in the read stream.
  - RocksDB API generator.
  - Build new Web site on HDT and RocksDB.
