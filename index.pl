% plServer.
user:file_search_path(plServer_wui, plServer(web_ui)).

% plDev.
user:file_search_path(plDev_console, plDev(plConsole)).
user:file_search_path(plDev_doc,     plDev(plDoc)).
user:file_search_path(plDev_handler, plDev(plHandler)).
user:file_search_path(plDev_module,  plDev(plModule)).
user:file_search_path(plDev_thread,  plDev(plThread)).

% plRdf.
user:file_search_path(plRdf_conv, plRdf(conversion)).
user:file_search_path(plRdf_ent, plRdf(entailment)).
user:file_search_path(plRdf_mt, plRdf(model_theory)).
user:file_search_path(plRdf_ser, plRdf(serialization)).
user:file_search_path(plRdf_term, plRdf(term)).

% plRdf-Dev
user:file_search_path(plRdfDev_man, plRdfDev(management)).
user:file_search_path(plRdfDev_wui, plRdfDev(web_ui)).

