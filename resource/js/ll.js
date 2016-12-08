// This way JS wont break on Internet Explorer when log statements
// are still in the code.
if (!console.log) {
  console = {log:function(){}};
};

function startCleaning(endpoint, seed) {
  $.ajax({
    "contentType": "application/json",
    "data": JSON.stringify({"seed": seed}),
    "dataType": "json",
    "type": "POST",
    "url": endpoint
  });
}

var analyticsId = 'UA-51130014-1';
var prefixes = "PREFIX llo: <http://lodlaundromat.org/ontology/>\n\
PREFIX ll: <http://lodlaundromat.org/resource/>\n";
var llVersion = 13;
var sparql = {
  url : "http://sparql.backend.lodlaundromat.org",
  graphs: {
    main: "http://lodlaundromat.org#" + llVersion,
    seedlist: "http://lodlaundromat.org#seedlist",
    metrics: "http://lodlaundromat.org#metrics-" + llVersion,
    error: "http://lodlaundromat.org/ontology#error",
    http: "http://lodlaundromat.org/ontology#http"
  },
  prefixes: prefixes,
  queries : {
    totalTripleCount :
    prefixes + "SELECT (SUM(?triples) AS ?totalTriples) {\n\
    ?dataset llo:triples ?triples . \n\
}\n",
    serializationsPerDoc:
    prefixes + "SELECT ?contentType (COUNT(?doc) AS ?count) WHERE {\n\
  ?doc llo:serializationFormat ?format \n\
    BIND(replace(str(?format),\".*/\",\"\") AS ?contentType)\n\
} GROUP BY ?contentType",
    serializationsPerTriple :
    prefixes + "PREFIX llo: <http://lodlaundromat.org/ontology/>\n\
SELECT (replace(str(?formatUri),\".*/\",\"\") as ?format) (SUM(?triples) AS ?count)\n\
WHERE {\n\
  ?datadoc llo:serializationFormat ?formatUri .\n\
  ?datadoc llo:triples ?triples .\n\
}\n\
GROUP BY ?formatUri\n",
    contentTypesPerDoc :
    prefixes + "SELECT (replace(str(?formatUri),\".*/\",\"\") as ?format) (COUNT(?datadoc) AS ?count)\n\
WHERE {\n\
  ?datadoc llo:serializationFormat ?formatUri .\n\
}\n\
GROUP BY ?formatUri\n",
    contentTypesVsSerializationFormats:
    prefixes + "SELECT ?matchType (COUNT(?datadoc) AS ?count)\n\
WHERE {\n\
  ?datadoc llo:contentType ?contentType .\n\
  ?datadoc llo:serializationFormat ?formatUri .\n\
  MINUS {?datadoc llo:serializationFormat <http://www.w3.org/ns/formats/RDFa>}#unfair to take this one into account, as http content type of rdfa will be the html page\n\
  FILTER(!contains(str(?contentType), \"zip\"))\n\
  BIND(if(contains(str(?contentType), \"n3\"), \"turtle\", LCASE(?contentType)) AS ?contentType)#make http content types consistent with our serialization formats\n\
  BIND(LCASE(replace(str(?formatUri),\".*/\",\"\")) AS ?format)#only take local name of uri, and to lower case for easy comparison\n\
  BIND(if(contains(str(?formatUri), \"RDF_XML\"), \"rdf+xml\", ?formatUri) AS ?formatUri)\n\
  BIND(if (contains(str(?contentType), ?format), \"matches\", \"does not match\") AS ?matchType)\n\
}\n\
GROUP BY ?matchType",
    contentLengths :
    prefixes + "SELECT ?clength ?bcount WHERE {\n\
  ?doc llo:contentLength ?clength ;\n\
    llo:byteCount ?bcount.\n\
  MINUS {\n\
    ?doc llo:archiveContains []\n\
  }\n\
  MINUS {[] llo:archiveContains ?doc}\n\
  FILTER(!STRENDS(str(?doc), \".bz2\"))\n\
  FILTER(!STRENDS(str(?doc), \".gz\"))\n\
}",
    datasetsWithCounts :
    prefixes + "SELECT ?md5 ?doc ?triples ?duplicates {\n\
  [] llo:triples ?triples;\n\
    llo:duplicates ?duplicates ;\n\
    llo:url ?doc ;\n\
    llo:md5 ?md5 .\n\
  FILTER(?triples > 0)\n\
} ORDER BY DESC(?triples + ?duplicates) LIMIT 1000",
    exceptionCounts:
    prefixes + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
PREFIX error: <http://lodlaundromat.org/error/ontology/>\n\
SELECT DISTINCT ?exception (COUNT(?doc) AS ?count)\n\
WHERE {\n\
  ?doc llo:exception/rdf:type/rdfs:label ?exception\n\
} GROUP BY ?exception LIMIT 100",
    totalWardrobeContents:
    prefixes + "SELECT (COUNT(?datadoc) AS ?total)\n\
WHERE {\n\
  ?datadoc llo:md5 [] ;\n\
	 llo:triples [] .\n\
}\n",
    wardrobeListing: function(drawId, orderBy, offset, limit, filter) {
      var colsToVar = {
	0: "?url",
	3: "?triples"
      };


      filter = filter.trim().replace("\"", "");//very simple method to avoid injection
      var triplesFilter = null;
      var urlFilter = null;
      if (filter.length > 0) {
        if (filter.indexOf('triples:') == 0) {
          filter = filter.substring('triples:'.length);
          triplesFilter = filter.match(/([\d\.]*)/)[0];
          filter = filter.substring(triplesFilter.length).trim();
        }
        if (filter.length) {
          urlFilter = filter;
        }
      }




      var filterExpressions = [];
      if (urlFilter && urlFilter.length > 0) {

	filterExpressions.push("CONTAINS(lcase(str(?url)), \"" + urlFilter.toLowerCase() + "\")");
      }
      if (triplesFilter && triplesFilter.length) {
	filterExpressions.push("CONTAINS(str(?triples), \"" + triplesFilter + "\")");
      }

      var filterClause = "";
      if (filterExpressions.length > 0) filterClause = "      FILTER(" + filterExpressions.join(" || ") + ")\n";
      var triplePatterns =
	  "      {\n\
         ?datadoc llo:url ?url ;\n\
		   llo:triples ?triples ;\n\
           llo:md5 ?md5 .\n\
        } UNION {\n\
        ?datadoc llo:path ?url ;\n\
           llo:triples ?triples ;\n\
           llo:md5 ?md5 .\n\
        ?parent llo:containsEntry ?datadoc .\n\
        } UNION {\n\
        ?datadoc a llo:Archive ;\n\
           llo:md5 ?md5;\n\
           llo:containsEntry [].\n\
        {?datadoc llo:url ?url}\n\
        UNION\n\
        {?datadoc llo:path ?url}\n\
        }\n" + filterClause;

      var query = prefixes + "SELECT DISTINCT ?totalFilterCount ?drawId ?md5 ?url ?triples ?parent \n\
WHERE {\n\
  BIND(\"" + drawId + "\" AS ?drawId) \n\
  {\n\
    SELECT ?md5 ?url ?triples ?parent WHERE { \n" + triplePatterns + "\
    }";
      //	var orderBys = [];
      //	if (orderBy && orderBy.length > 0) {
      //		for (var i = 0; i < orderBy.length; i++) {
      //			if (orderBy[i].column in colsToVar) {
      //				orderBys.push(orderBy[i].dir.toUpperCase() + "(" + colsToVar[orderBy[i].column] + ")");
      //			}
      //		}
      //	}
      //	if (orderBys.length > 0) {
      //		query += " ORDER BY " + orderBys.join(" ");
      //	}

      if (limit && limit > 0) {
	query += " LIMIT " + limit;
      }
      if (offset && offset > 0) {
	query += " OFFSET " + offset;
      }
      query += "\n\
  }\n\
  {\n\
    SELECT (COUNT(DISTINCT ?datadoc) AS ?totalFilterCount) WHERE {\n" + triplePatterns + "\
    }\n\
  }\n\
} \n";
      return query;

    },
    totalBasketContents:
    prefixes + "SELECT (COUNT(?datadoc) AS ?total)\n\
WHERE {\n\
  ?datadoc llo:url [] ;\n\
     llo:md5 [] .\n\
}\n",
    basketListing: function(basketGraph, mainGraph, drawId, orderBy, offset, limit, filter) {
      var requiredClause = "";
      var minusClause = "";
      var getStatusBlock = function() {
	var optionalTPatterns = [];
	var minusTPatterns = [];
	var requiredTPatterns = [];
	var cleanTPattern = "?datadoc llo:endClean ?endClean"

	if (statusFilter == null) {
	  optionalTPatterns.push(cleanTPattern);
	} else if (statusFilter == "cleaned"){
	  requiredTPatterns.push(cleanTPattern);
	} else {
	  //we want the queued ones
	  minusTPatterns.push(cleanTPattern);
	}


	var clauses = "";

	if (requiredTPatterns.length > 0 || minusTPatterns.length > 0) {
	  if (requiredTPatterns.length > 0) {
	    for (var i = 0; i < requiredTPatterns.length; i++) {
	      requiredClause += "       " + requiredTPatterns[i] + " .\n";
	    }
	    clauses+= requiredClause;
	  }
	  for (var i = 0; i < minusTPatterns.length; i++) {
	    minusClause += "       MINUS{" + minusTPatterns[i] + "}\n";

	  }
	  clauses += minusClause;



	}
	if (optionalTPatterns.length > 0) {
	  clauses = "OPTIONAL {\n\
		          GRAPH <" + mainGraph + "> {\n";
	  for (var i = 0; i < optionalTPatterns.length; i++) {
	    clauses += "       " + optionalTPatterns[i] + " .\n";
	  }
	  clauses += "     }\n   }\n";
	}

	return clauses;
      };
      filter = filter.trim();
      var statusFilter = null;
      var urlFilter = null;
      if (filter.length > 0) {
	if (filter.indexOf('status:') == 0) {
	  filter = filter.substring('status:'.length);
	  statusFilter = filter.match(/([a-z]*)/)[0];
	  filter = filter.substring(statusFilter.length).trim();
	}
	if (filter.length) {
	  urlFilter = filter;
	}
      }

      var filterClause = "";

      if (urlFilter && urlFilter.length > 0) {
	filterExpressions = [("CONTAINS(lcase(str(?url)), \"" + urlFilter.toLowerCase() + "\")")];
	filterClause = "      FILTER(" + filterExpressions.join(" || ") + ")\n";
      }
      var triplePatterns =
	  "         ?datadoc llo:url ?url ;\n\
         llo:added ?dateAdded .\n" + filterClause;

      var query = prefixes + "SELECT ?totalFilterCount ?drawId ?datadoc ?url ?dateAdded ?startUnpack ?endUnpack ?startClean ?endClean\n\
WHERE {\n\
  BIND(\"" + drawId + "\" AS ?drawId) \n\
  {\n\
    SELECT ?datadoc ?url ?dateAdded ?startUnpack ?endUnpack ?startClean ?endClean WHERE {\n\
	  "+  triplePatterns + getStatusBlock() + "\
        " + filterClause + "\
    }";
      /**
       * Do not add order By! This makes the query too complex. Virtuoso rejects this query in such a case. We could change this limit,
       * but I'm worried that when the dataset grows, we need to keep upping this value (making the endpoint venerable to complex queries from third parties as well)
       */
      if (limit && limit > 0) {
	query += " LIMIT " + limit;
      }
      if (offset && offset > 0) {
	query += " OFFSET " + offset;
      }
      query += "\n\
  }\n\
  {\n\
    SELECT (COUNT(?datadoc) AS ?totalFilterCount) WHERE {\n\
	    "+  triplePatterns + "\n\
	  " + filterClause + "\n" + requiredClause + minusClause + "\
	}\n\
  }\n\
}";
      return query;
    },
    getDegreeStats:
    prefixes +
      "PREFIX llm: <http://lodlaundromat.org/metrics/ontology/>\n\
SELECT DISTINCT * {\n\
  ?doc llm:outDegree/llm:mean ?outDegreeMean ;\n\
    llm:inDegree/llm:mean ?inDegreeMean ;\n\
    llm:degree/llm:mean ?degreeMean ;\n\
    llm:outDegree/llm:std ?outDegreeStd ;\n\
    llm:inDegree/llm:std ?inDegreeStd;\n\
    llm:degree/llm:std ?degreeStd ;\n\
    llm:outDegree/llm:median ?outDegreeMedian ;\n\
    llm:inDegree/llm:median ?inDegreeMedian ;\n\
    llm:degree/llm:median ?degreeMedian ;\n\
    llm:outDegree/llm:min ?outDegreeMin ;\n\
    llm:inDegree/llm:min ?inDegreeMin ;\n\
    llm:degree/llm:min ?degreeMin ;\n\
    llm:outDegree/llm:max ?outDegreeMax ;\n\
    llm:inDegree/llm:max ?inDegreeMax ;\n\
    llm:degree/llm:max ?degreeMax .\n\
} "
  }
};

var api = {
  "laundryBasket": {
    "seedUpdateApi": "http://backend.lodlaundromat.org"
  },
  "notifications": {
    "api": "http://notify.lodlaundromat.d2s.labs.vu.nl"
  },
  "ldf": {
    browser: "http://ldf.lodlaundromat.org/",
    query: function(md5) {
      var ldfUrl = api.ldf.browser + md5;
      return "http://client.linkeddatafragments.org/#startFragment=" + encodeURIComponent(ldfUrl);
    },
  },

  "namespace": "http://lodlaundromat.org/vocab#",
  "wardrobe": {
    "download": function(md5, type) {
      var url = "http://download.lodlaundromat.org/" + md5;
      if (type) url += "?type=" + type;
      return url;
    }
  }
};


var getSparqlLink = function(query) {
  return "/sparql?query=" + encodeURIComponent(query);
};


// Init loader.
$.ajaxSetup({
  beforeSend: function() {
    $('#loader').show();
  },
  complete: function(){
    $('#loader').hide();
    if (goToHash) goToHash();
  },
  success: function() {},
  url : sparql.url,
});

$("<div id='loader'><img src='/imgs/loader.gif'></div>").appendTo($("body"));



/**
 * helpers
 */
if (typeof d3 != 'undefined') {
  var formatPercentage = d3.format("%");
  var formatThousands = d3.format(",g");
  var formatLargeShortForm = d3.format(".2s");
  var formatNumber = d3.format(",n");
}
var goToHash = function(){
  if(window.location.hash) {
    $.scrollTo($(window.location.hash), { duration: 500 });
  }
};

$(document).ready(function(){});

var modalDiv = $("<div class='modal  fade'  tabindex='-1' role='dialog' aria-hidden='true'></div>")
    .html('<div class="modal-dialog modal-lg ">' +
	  '  <div class="modal-content">' +
	  '    <div class="modal-header">' +
	  '    </div>' +
	  '    <div class="modal-body">' +
	  '      <p>One fine body&hellip;</p>' +
	  '    </div>' +
	  '    <div class="modal-footer"></div>' +
	  '  </div><!-- /.modal-content -->' +
	  '</div><!-- /.modal-dialog -->')
    .appendTo($("body"));
var modal = modalDiv.modal({show: false});


/**
 * config {
 *  header: null, string, or jquery el
 *  content: string (text) or jquery el
 *  footer: null, string(html), or jquery el
 * }
 */
var drawModal = function(config) {
  var header = modalDiv.find(".modal-header");
  if (!config.header) {
    if (header.length > 0) modalDiv.find(".modal-header").remove();
  } else {
    if (header.length == 0) header = $("<div class='modal-header'></div>").prependTo(modalDiv.find(".modal-content"));
    if (typeof config.header == "string") {
      modalDiv.find(".modal-header").html('<button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>' +
					  '<h4 class="modal-title">' + config.header + '</h4>');
    } else {
      modalDiv.find(".modal-header").empty().append(config.header);
    }
  }
  modalDiv.find(".modal-body").empty().append(config.content);
  modal.modal("show");
};



/**
 * draw header
 */
var drawHeader = function() {
  var addItem = function(config) {
    var item = $("<li></li>").appendTo(topNavBar);
    if (config.active) item.addClass("active");
    var anchor = $("<a></a>").attr("href", config.href).appendTo(item);
    if (config.newWindow) anchor.attr("target", "_blank");
    //    $("<img/>").attr("src", config.img).appendTo(anchor);
    $("<span></span>").text(config.title).appendTo(anchor);
  };
  var items = [
    {href: "/", img: "/imgs/laundry.png", title: "Main"},
    {href: "/basket", img: "/imgs/basket.png", title: "Basket"},
    {href: "/wardrobe", img: "/imgs/wardrobe.png", title: "Wardrobe"},
    {href: "/lodlab", title: "LOD Lab"},
    {href: "/visualizations", img: "/imgs/analysis.png", title: "Widgets"},
    {href: "/sparql", img: "/imgs/labels.png", title: "SPARQL"},
    {href: "/services", img: "/imgs/labels.png", title: "Services"},
    {href: "/about", img: "/imgs/laundryLine.png", title: "About"},
  ];
  var lastIndexOf = document.URL.lastIndexOf("/");
  var basename = "";
  if (lastIndexOf < document.URL.length) {
    basename = document.URL.substring(lastIndexOf + 1);
  }
  var hashTagIndex = basename.indexOf("#");
  if (hashTagIndex == 0) {
    basename == "";
  } else if (hashTagIndex > 0) {
    basename = basename.substring(0, hashTagIndex-1);
  }

  if (basename.length == 0) basename = "index.html";



  for (var i = 0; i < items.length; i++) {
    if (basename == items[i].href) items[i].active = true;
    addItem(items[i]);
  }
};
drawHeader();

var getAndDrawCounter = function() {
  var draw = function(count) {
    var holder = $('.counter');
    var countString = count.toString();
    var charsLeft = countString.length;
    for (var i = 0; i < countString.length; i++) {
      //    <span class="position"><span class="digit static" style="top: 0px; opacity: 1;">0</span></span>
      holder.append($('<span>' + countString.charAt(i) + '</span>'));
      charsLeft = charsLeft - 1;
      if (charsLeft % 3 == 0 && charsLeft > 0) {
        holder.append("<span>.</span>");
      }
    }
  };
  if ($('.counter').length > 0) {
    $.ajax({
      url: sparql.url,
      data: [
        {name: "default-graph-uri", value: sparql.graphs.main},
        {name: "query", value: sparql.queries.totalTripleCount}
      ],
      success: function(data) {
        if (data.results && data.results.bindings && data.results.bindings.length > 0 && data.results.bindings[0].totalTriples && data.results.bindings[0].totalTriples.value > 0) {
          draw(data.results.bindings[0].totalTriples.value);
        } else {
          $("#counterWrapper").hide();
        }
      },
      headers: {
        "Accept": "application/sparql-results+json,*/*;q=0.9"
      }
    });
  }
};
getAndDrawCounter();


//this function is useful for printing charts to pdf.
var deleteEveryDivExcept = function(divId) {
  $("div").hide();
  $("h1").hide();
  $("h2").hide();
  $("h3").hide();
  var targetDiv = $("#" + divId);
  targetDiv.parents().show();
  targetDiv.show();
};


var showNotification = function(msg) {
  if (msg) {
    $('<div>', {class: 'alert alert-info', role: 'alert'}).text(msg).prependTo($('body'));
  }
}
//showNotification('We have upgraded the Washing Machine crawling mechanism. For consistency reasons, we have re-initiated the crawl from scratch. (taking the manually added seed-items from the previous crawl into account) (20 Feb. 2015)');

var setCookie = function(name, value, days) {
  var expires;
  if (days) {
    var date = new Date();
    date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
    expires = "; expires=" + date.toGMTString();
  }
  else {
    expires = "";
  }
  document.cookie = name + "=" + value + expires + "; path=/";
}

function getCookie(c_name) {
  if (document.cookie.length > 0) {
    c_start = document.cookie.indexOf(c_name + "=");
    if (c_start != -1) {
      c_start = c_start + c_name.length + 1;
      c_end = document.cookie.indexOf(";", c_start);
      if (c_end == -1) {
        c_end = document.cookie.length;
      }
      return unescape(document.cookie.substring(c_start, c_end));
    }
  }
  return "";
}
