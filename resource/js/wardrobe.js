if (!window.location.hash || window.location.hash.indexOf('select') < 0) {
  $('body').removeClass('showCustomSelect');
}

var recordsTotal;
var dt;
var doSearch;
var addFilterWidgets = function() {
  var tripleFilter = $('<input id="tripleFilter" style="margin-left: 20px;" type="search" placeholder="filter">')
      .keyup(function(event){
	if(event.keyCode == 13){
	  doSearch();
	}
      }).appendTo("#tripleHeader");
  
  /**
   * URL filter
   */
  var urlFilter = $('<input id="urlFilter" style="margin-left: 20px;" type="search" placeholder="filter">')
      .keyup(function(event){
	if(event.keyCode == 13){
	  doSearch();
	}
      }).appendTo("#urlHeader");
  
  doSearch = function() {
    var urlFilterVal = urlFilter.val().trim();
    var tripleFilterVal = tripleFilter.val().trim().replace('.', '');
    var filter = "";
    if (tripleFilterVal.length > 0 ) {
      filter += "triples:" + tripleFilterVal;
    }
    if (urlFilterVal.length > 0) {
      filter += (filter.length == 0? "": " ") + urlFilterVal;
    }
    dt.api().search(filter).draw();
  };
};

$.ajax({
  data: [
    {name: "default-graph-uri", value: sparql.graphs.main},
    {name: "default-graph-uri", value: sparql.graphs.seedlist},
    {name: "query", value: sparql.queries.totalWardrobeContents},
  ],
  headers: {
    "Accept": "application/json"
  },
  success: function(data) {
    if (data.results.bindings.length > 0) {
      recordsTotal = data.results.bindings[0].total.value;
      $(document).ready(function() {
  	dt = $('#wardrobeTable').dataTable( {
	  "processing": true,
	  "serverSide": true,
	  "ajax": $.fn.dataTable.pipeline(),
	  "drawCallback": function ( oSettings ) {
	    $('.longtitle').tooltip();
	  },
	  createdRow: function(row, data, dataIndex) {
	    $(row).find("a").click(
	      function(event){
	        event.stopPropagation();
	      }
	    );
	    $(row).find("button").click(
	      function(event){
	        event.stopPropagation();
	      }
	    );
	  },
	  "columns": [
            {//0 URL
              "class": "urlCol",
              orderable: false,
	    },
	    {//1 services buttons
	      render: function( data, type, full, meta ) {
		var getLdfBrowserBtn = function(href, title, enabled) {
		  return "<a style='padding: 6px;' class='btn btn-default " + (enabled? "" : "disabled") + "' title='" + title + "' href='" + href + "' target='_blank'><img class='pull-left' style='height: 20px;width: 20px;' src='../imgs/logo_ldf.svg'>&nbsp;Browse</a>";
		}
		var getLdfQueryBtn = function(href, title, enabled) {
		  return "<a style='padding: 6px;' class='btn btn-default " + (enabled? "" : "disabled") + "' title='" + title + "' href='" + href + "' target='_blank'><img class='pull-left' style='height: 20px;width: 20px;' src='../imgs/logo_ldf.svg'>&nbsp;Query</a>";
		}
		var metaDataBtn = "<a style='padding: 6px;' class='btn btn-default' title='Show more info' href='http://lodlaundromat.org/resource/" + full[4] + "' target='_blank'><span class='glyphicon glyphicon-info-sign'></span> Metadata</a>";
		
		var ldfBrowserButton,ldfQueryButton;
		if (full[3] === null) {
		  //an archive file. no triples set (not '0' as well)
		  ldfBrowserButton = getLdfBrowserBtn('javascript:void(0)', 'Unable to browse archive as LDF. Access the unpacked members instead', false);
		  ldfQueryButton = getLdfQueryBtn('javascript:void(0)', 'Unable to query this archive as LDF. Access the unpacked members instead', false);
		} else {
		  ldfBrowserButton = getLdfBrowserBtn( api.ldf.browser + full[4], 'Browse data via LDF', true);
		  ldfQueryButton = getLdfQueryBtn(api.ldf.query(full[4]), 'Query as LDF', true);
		}
	    	return ldfBrowserButton + '&nbsp;' + ldfQueryButton + '&nbsp;' + metaDataBtn;
              },
              "class": "buttonCol defaultButtons",
              orderable: false,
              width: 280
            },
            {//2 downloads buttons
	      render: function( data, type, full, meta ) {
		var getDownloadLink = function(className, href, title, enabled, buttonName) {
		  return "<a class='" + className + " btn btn-default " + (enabled? "" : "disabled") + "' title='" + title + "' href='" + href + "' target='_blank'><span class='glyphicon glyphicon-download'></span> " + buttonName + "</a>";
		}
		var getHdtBtn = function(href, title, enabled) {
		  return "<a class='downloadHdt btn btn-default " + (enabled? "" : "disabled") + "' title='" + title + "' href='" + href + "' target='_blank'><img class='pull-left' style='height:20px;width: 20px;' src='../imgs/logo_hdt.png'>&nbsp;&nbsp;HDT</a>";
		}
		
		var dirtyBtn, cleanBtn, hdtBtn;
		if (full[5].length) {
		  //this one is unpacked from an archive. we don't have a single cleaned file...
		  dirtyBtn = getDownloadLink('downloadDirty', 'javascript:void(0)', 'File is unpacked from an archive. Download the original archive in order to fetch this file', false, 'Original Dirty File');
		} else {
		  dirtyBtn = getDownloadLink('downloadDirty', full[0].split(" ")[0], 'Download original dirty dataset', true, 'Original Dirty File');
		}
		
		if (full[3] === null) {
                  //an archive file. no triples set (not '0' as well)
		  cleanBtn = getDownloadLink('downloadClean', 'javascript:void(0)', 'Unable to download archive as cleaned file. Access the unpacked members as cleaned file instead', false, 'GZIP');
		  hdtBtn = getHdtBtn('javascript:void(0)', 'Unable to download archive as cleaned file. Access the unpacked members as cleaned file instead', false);
		} else {
		  cleanBtn = getDownloadLink('downloadClean', api.wardrobe.download(full[4]), 'Download the washed and cleaned data', true, 'GZIP');
		  hdtBtn = getHdtBtn(api.wardrobe.download(full[4], "hdt"), 'Download the washed and cleaned data as HDT file', true);
		}
		var space = "&nbsp;";//yes UGLY. Just want this done fast
		return cleanBtn + space + hdtBtn + space + dirtyBtn;
	      },
	      width: 315,
	      "class": "buttonCol defaultButtons",
	      orderable: false
            },
	    {//3 triples
              "render": function ( count ) {
          	if (count && count.length) {
          	  return ("" + count).replace(/(\d)(?=(\d\d\d)+(?!\d))/g, function($1) { return $1 + "." ;});//add thousands separator
          	} else {
          	  return "N/A";
          	}
		//			    		return "<span class='longtitle' data-toggle='tooltip' data-placement='top' title='" + oObj + "'>" + shortenUrl(oObj) + "</span>";
          	
	      },
	      "class": "tripleCol",
	      width: 90,
	      orderable: false,
	    },
	    {//4 md5
	      visible: false,
	    },

	    {//5 parent doc
	      visible:false
	    },
	    {//6 custom select button for iframe use
	      orderable: false,
              width: 100,
              class: 'buttonCol customSelect',
              render: function(data, type, full, meta) {
		return "<button class='btn btn-primary' title='Select document' onClick='customSelect(\"" + full[4] + "\")'>Select</button>";
              }
            }
	  ]
	});
  	dt.fnFilterOnReturn().css("display", "table");
      } );
      addFilterWidgets();
    }
  },
  url: sparql.url
});

var customSelect = function(md5) {
  if (parent && parent.postMessage) parent.postMessage(md5,'*');
};
//
// Pipelining function for DataTables. To be used to the `ajax` option of DataTables
//
$.fn.dataTable.pipeline = function ( opts ) {
  // Configuration options
  var conf = $.extend( {
    pages: 5,   // number of pages to cache
    url: sparql.url,    // script url
    data: function(request) {
      return [
    	{name: "query", value:  sparql.queries.wardrobeListing(request.draw, request.order, request.start, request.length, request.search.value)},
    	{name: "default-graph-uri", value: sparql.graphs.main},
    	{name: "default-graph-uri", value: sparql.graphs.seedlist},
      ];
    },
    method: 'GET' // Ajax HTTP method
  }, opts );
  //  console.log(opts);
  
  // Private variables for storing the cache
  var cacheLower = -1;
  var cacheUpper = null;
  var cacheLastRequest = null;
  var cacheLastJson = null;
  
  return function ( request, drawCallback, settings ) {
    //  	console.log(request);
    var ajax      = false;
    var requestStart  = request.start;
    var drawStart   = request.start;
    var requestLength = request.length;
    var requestEnd  = requestStart + requestLength;
    
    if ( settings.clearCache ) {
      // API requested that the cache be cleared
      ajax = true;
      settings.clearCache = false;
    }
    else if ( cacheLower < 0 || requestStart < cacheLower || requestEnd > cacheUpper ) {
      // outside cached data - need to make a request
      ajax = true;
    }
    else if ( JSON.stringify( request.order )   !== JSON.stringify( cacheLastRequest.order ) ||
              JSON.stringify( request.columns ) !== JSON.stringify( cacheLastRequest.columns ) ||
              JSON.stringify( request.search )  !== JSON.stringify( cacheLastRequest.search )
	    ) {
      // properties changed (ordering, columns, searching)
      ajax = true;
    }
    
    // Store the request for checking next time around
    cacheLastRequest = $.extend( true, {}, request );
    
    if ( ajax ) {
      // Need data from the server
      if ( requestStart < cacheLower ) {
        requestStart = requestStart - (requestLength*(conf.pages-1));
        if ( requestStart < 0 ) {
          requestStart = 0;
        }
      }
      
      cacheLower = requestStart;
      cacheUpper = requestStart + (requestLength * conf.pages);
      
      request.start = requestStart;
      request.length = requestLength*conf.pages;
      
      // Provide the same `data` options as DataTables.
      if ( $.isFunction ( conf.data ) ) {
        // As a function it is executed with the data object as an arg
        // for manipulation. If an object is returned, it is used as the
        // data object to submit
        var d = conf.data( request );
        if ( d ) {
          request = d;
        }
      }
      else if ( $.isPlainObject( conf.data ) ) {
        // As an object, the data given extends the default
        $.extend( request, conf.data );
      }
      settings.jqXHR = $.ajax( {
        "type":   conf.method,
        "url":    conf.url,
        "data":   request,
        "dataType": "json",
        "cache":  false,
        "success":  function ( sparqlResult ) {
          var json = sparqlResultToDataTable(sparqlResult);
          cacheLastJson = $.extend(true, {}, json);
          if ( cacheLower != drawStart ) {
            json.data.splice( 0, drawStart-cacheLower );
          }
          json.data.splice( requestLength, json.data.length );
          drawCallback( json );
        }
      } );
    }
    else {
      json = $.extend( true, {}, cacheLastJson );
      json.draw = request.draw; // Update the echo for each response
      json.data.splice( 0, requestStart-cacheLower );
      json.data.splice( requestLength, json.data.length );
      
      drawCallback(json);
    }
  };
};

// Register an API method that will empty the pipelined data, forcing an Ajax
// fetch on the next draw (i.e. `table.clearPipeline().draw()`)
$.fn.dataTable.Api.register( 'clearPipeline()', function () {
  return this.iterator( 'table', function ( settings ) {
    settings.clearCache = true;
  } );
} );

var sparqlResultToDataTable = function(sparqlResult) {
  var datatable = {
    recordsTotal: recordsTotal,
    data: [],
    
  };
  for (var i = 0; i < sparqlResult.results.bindings.length; i++) {
    var binding = sparqlResult.results.bindings[i];
    var md5 = binding.md5.value;
    var row = [];
    datatable.draw = sparqlResult.results.bindings[i].drawId.value;//same for all results though
    datatable.recordsFiltered = sparqlResult.results.bindings[i].totalFilterCount.value;//same for all results though
    
    //while we are at it, do some post processing as well
    //0 url
    var url = sparqlResult.results.bindings[i].url.value;
    if (binding.parent && binding.parent.value) {
      url += ' <span class="label label-default">Unpacked from archive. <a style="color:#272727" href="' + binding.parent.value + '" target="_blank">show parent</a></span>';
    }
    if (!binding.triples) {
      url += ' <span class="label label-default">Archive file. Access the unpacked files <a style="color:#272727" href="http://lodlaundromat.org/resource/' + md5 + '" target="_blank">here</a></span>';
    }
    row.push(url);
    
    //1 services buttons
    //    row.push("<a style='padding: 6px;' class='btn btn-default glyphicon glyphicon-info-sign' title='Show more info' href='http://lodlaundromat.org/resource/" + md5 + "' target='_blank'></a>");
    row.push('');
    
    //2 download buttons
    row.push(null);
    var triples = null;
    if (sparqlResult.results.bindings[i].triples && sparqlResult.results.bindings[i].triples.value) {
      triples = sparqlResult.results.bindings[i].triples.value;
    }
    //3 triples
    row.push(triples);
    
    //4 md5
    row.push(md5);
    
    //5 parent doc
    if (binding.parent && binding.parent.value) {
      row.push( binding.parent.value);
    } else {
      row.push('');
    }
    
    //6 special select button for use in iframe
    row.push('');
    datatable.data.push(row);
    
  }
  return datatable;
};

var getHostname = function(url) {
  var m = url.match(/^http:\/\/[^/]+/);
  return m ? m[0] : null;
};
