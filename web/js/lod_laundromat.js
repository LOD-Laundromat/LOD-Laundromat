/*
@author Wouter Beek
@version 2016/02
*/

function startCleaning(endpoint, seed) {
  $.ajax({
    "contentType": "application/json",
    "data": JSON.stringify({"seed": seed}),
    "dataType": "json",
    "type": "POST",
    "url": endpoint
  });
