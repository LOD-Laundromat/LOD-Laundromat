:- module(
  seedlist,
  [
    add_seed/1,  % +Uri
    end_seed/1,  % +Hash
    seed/5,      % ?Hash, ?Uri, ?Added, ?Started, ?Ended
    seed0/1,     % -Uri
    start_seed/2 % -Hash, -Uri
  ]
).

/** <module> Seedlist

@author Wouter Beek
@version 2017/04
*/

:- use_module(library(debug)).
:- use_module(library(md5)).
:- use_module(library(persistency)).
:- use_module(library(uri)).

:- debug(seedlist).

:- initialization(db_attach('seedlist.data', [])).

:- persistent
   seed(hash:atom, uri:atom, added:float, started:float, ended:float).





%! add_seed(+Uri) is det.

add_seed(Uri0) :-
  uri_normalized(Uri0, Uri),
  md5_hash(Uri, Hash, []),
  with_mutex(seedlist, (
    (   seed(Hash, Uri, _, _, _)
    ->  true
    ;   get_time(Now),
        assert_seed(Hash, Uri, Now, 0.0, 0.0),
        debug(seedlist, "Seedpoint added: ~a", [Uri])
    )
  )).



%! end_seed(+Hash) is semidet.

end_seed(Hash) :-
  with_mutex(seedlist, (
    retract_seed(Hash, Uri, Added, Started, 0.0),
    get_time(Ended),
    assert_seed(Hash, Uri, Added, Started, Ended)
  )).



%! start_seed(-Hash, -Uri) is semidet.

start_seed(Hash, Uri) :-
  with_mutex(seedlist, (
    retract_seed(Hash, Uri, Added, 0.0, 0.0),
    get_time(Started),
    assert_seed(Hash, Uri, Added, Started, 0.0)
  )).



seed0(Uri) :-
  seed0(_, Uri, _, _, _).

seed0(dbe8e9e8dc412990d5a13289a3613bd6,'http://resource.geolba.ac.at/PoolParty/sparql/GeologicUnit',1492891753.6481893,0.0,0.0).
seed0('93ad55ebbc9b8aa1ac183d5fe75189f9','http://resource.geolba.ac.at/PoolParty/sparql/GeologicTimeScale',1492891753.6487272,0.0,0.0).
seed0('8fbaa3d5a6e7b4ac33c175e178913444','http://resource.geolba.ac.at/PoolParty/sparql/lithology',1492891753.6488128,0.0,0.0).
seed0('8cc8c6cf2bd0a23309bef2d37d92cd0e','http://resource.geolba.ac.at/PoolParty/sparql/tectonicunit',1492891753.648892,0.0,0.0).
seed0('8bcb930ba85bce12a2c4e192bf710d07','http://resource.geolba.ac.at/PoolParty/sparql/structure',1492891753.6489682,0.0,0.0).
seed0('2cd92a95edbad263a2dd1499e8719b82','http://resource.geolba.ac.at/GeologicUnit/export/GeologicUnit.rdf',1492891753.649044,0.0,0.0).
seed0('207e2443163fab98ccfd9b6199c03413','http://resource.geolba.ac.at/structure/export/structure.rdf',1492891753.6491187,0.0,0.0).
seed0('78363f6152ca19b43f0f0300ff855e27','http://resource.geolba.ac.at/GeologicTimeScale/export/GeologicTimeScale.rdf',1492891753.6491945,0.0,0.0).
seed0(e2df2f041f0b8315ea4ac773b13e6886,'http://resource.geolba.ac.at/lithology/export/lithology.rdf',1492891753.6492715,0.0,0.0).
seed0('45722757854edf71928bccb3ab508afc','http://resource.geolba.ac.at/tectonicunit/export/tectonicunit.rdf',1492891753.6493473,0.0,0.0).
seed0('796b4a79ab82c2350b2da19f17427aff','http://opendata.opennorth.se/dataset/environment',1492902318.7570653,0.0,0.0).
seed0(ae8a24088daeaa4857fdb7936e61feb7,'http://opendata.caceres.es/dataset/913329f3-6173-4fef-b465-b65513499352/resource/06bf182b-8ffe-41fc-af13-ff0b4fac3a4f/download/ejecucion20143ingreso.ttl',1492902318.8266222,0.0,0.0).
seed0('5d5cde3d611c7af9db7edc871f10e71c','http://opendata.caceres.es/dataset/5ec28a4e-b7e6-4346-9149-677869bb1c2d/resource/09fa1d99-9451-45aa-b6bc-c81266b14180/download/parcelasferial.ttl',1492902318.8277397,0.0,0.0).
seed0('8f05b8d6efce80700a7f1d1abb2da6df','http://opendata.caceres.es/dataset/be99268f-cc38-47d4-b70f-27ab78ff09a9/resource/0d30c4a9-2fe8-40a4-8c4a-015daaae9c5b/download/presupuestos2013.ttl',1492902318.8288026,0.0,0.0).
seed0(b0d1e925c78004c2a19f1c05bd22f18b,'http://opendata.caceres.es/sparql?default-graph-uri=&query=select%2B?URI%2B?empadronadaEnVia%2B?fechaEmpadronamiento%2B?fechaNacimiento%2B?genero%2B?estudios%2B?nacionalidad%2Bwhere%2B%7B%2B%0D%0A?URI%2Ba%2Bom%3APersona.%0D%0A?URI%2Bom%3AempadronadaEnVia%2B?empadronadaEnVia.%0D%0A?URI%2Bom%3AfechaEmpadronamiento%2B?fechaEmpadronamiento.%0D%0A?URI%2Bschema%3AbirthDate%2B?fechaNacimiento.%0D%0A?URI%2Bschema%3Agender%2B?genero.%0D%0A?URI%2Bom%3Aestudios%2B?estudios.%0D%0A?URI%2Bschema%3Anationality%2B?nacionalidad.%2B%0D%0A%7D&format=turtle',1492902318.829838,0.0,0.0).
seed0(ec2514ffae0862629a21bc414e5b0a89,'http://opendata.caceres.es/dataset/f735a10a-16f0-4408-9c8a-873020d02f0c/resource/10f8a474-64a8-4a63-9833-ecbe22ad5d8e/download/cines.ttl',1492902318.830885,0.0,0.0).
seed0(f01e08f35cb975566a892c07f64bb3a0,'http://opendata.caceres.es/dataset/925d2dcb-9ada-48b0-8385-718fac009174/resource/20d350cf-a76e-4922-bea8-c733c5296268/download/barrios.ttl',1492902318.8319657,0.0,0.0).
seed0('2f0834ebd17d359c8b0bdd92e8507386','http://opendata.caceres.es/dataset/89106ea6-3514-4856-8b52-bddfe538c653/resource/232ef7e0-6c86-480a-9011-7d2130aca20b/download/centroseducacion.ttl',1492902318.8330235,0.0,0.0).
seed0(e4eea2b564a6cb0a81762feee534ef4c,'http://opendata.caceres.es/dataset/cad00ed9-4fa3-4d04-8524-4b7b91ab0813/resource/2600ced6-a676-470f-ba12-23d43a22ca7e/download/ejecucion20142gasto.ttl',1492902318.8340616,0.0,0.0).
seed0(aab1d68d9689ced6e242fbba8d1ba01b,'http://opendata.caceres.es/dataset/d80b3641-41a2-4467-83c8-8ecd6052f7d0/resource/26bf9a4b-cdfb-43f1-9696-44932c40d6fc/download/presupuestos2014upcc.ttl',1492902318.8351302,0.0,0.0).
seed0('4563cbca6bc2e0e2b2d50fbf72600500','http://opendata.caceres.es/dataset/76ee61b3-fd33-4f13-b138-2337ddadfb17/resource/29de8b48-ddd6-4556-9e35-440e956ed125/download/ejecuci-n-2016-trimestre-3.ttl',1492902318.8361702,0.0,0.0).
seed0('8c663c3ff9c0585e37d1366eaa407a3c','http://opendata.caceres.es/dataset/846ceb31-511c-4faa-b77e-c39b1a1d5293/resource/2a7b3f86-fd93-4efc-b777-28d38c7b079f/download/centrosexposiciones.ttl',1492902318.837244,0.0,0.0).
seed0('2701e9928f867fc4af4a2e324a382a1c','http://opendata.caceres.es/dataset/44554f74-5307-4037-8033-8b83b2b5d929/resource/2b3d92ef-216d-4ea3-8299-10980a756757/download/ejecucion20144gasto.ttl',1492902318.8385124,0.0,0.0).
seed0('5d0a82e82a9607108d1e4c355c8ae538','http://opendata.caceres.es/dataset/d80b3641-41a2-4467-83c8-8ecd6052f7d0/resource/2c874c6a-4a42-4ba2-861e-1cb84712fcd2/download/presupuestos2014aytocc.ttl',1492902318.8395572,0.0,0.0).
seed0(a8fa65a48feec92f9f02586f4ab62eaf,'http://opendata.caceres.es/dataset/b3a8dc61-f91a-4585-9630-5b7c8e92748d/resource/2ca03ab7-f8c1-4f2b-9799-27175c77f1ef/download/imagenes-2017.ttl',1492902318.8413973,0.0,0.0).
seed0('66aad0f15672bc8176137292dcdaa4b8','http://opendata.caceres.es/dataset/62b7e14c-7926-441a-afd3-aef66c51d42c/resource/2d500d0b-46fa-4149-84cc-c30b9b5048cb/download/ejecuci-n2-trimestre2015ingresos-xlsx.ttl',1492902318.8431156,0.0,0.0).
seed0('93439d8972a48ddf06704927344725e9','http://opendata.caceres.es/dataset/aca3ed93-d4e4-4186-a434-557c69b6c21c/resource/2fa8970b-46c1-4d8b-8218-c7738978e98f/download/arboles.ttl',1492902318.8442683,0.0,0.0).
seed0('7206d4a193ae1b8d64cbc7f328037fa4','http://opendata.caceres.es/dataset/b44452bb-0325-4a05-9ddf-d6d50fd80c6b/resource/34c99298-2848-4a5c-a39b-e97d6b80333a/download/distritos.ttl',1492902318.8454506,0.0,0.0).
seed0(a1cbda86a3e5551d776679504b40509d,'http://opendata.caceres.es/dataset/44554f74-5307-4037-8033-8b83b2b5d929/resource/3698fc98-84cf-4e82-9748-d00b6d461df4/download/ejecucion20144ingreso.ttl',1492902318.8464894,0.0,0.0).
seed0('874b205b635107bc61da95f5001ce1ea','http://opendata.caceres.es/dataset/a0b271cb-b94e-422e-8b1b-7958fe5c3522/resource/39780169-14ce-46e4-bc69-a9b6a05e173c/download/ejecuci-n-2016-trimestre-4.ttl',1492902318.847514,0.0,0.0).
seed0('47bd9c2377d7cff0a23451f8dd56945a','http://opendata.caceres.es/dataset/86977101-7512-4313-b18d-a8fdcaeb15ec/resource/3f7f3dea-180b-4111-a545-e8201ccff75a/download/contenedores.ttl',1492902318.8485906,0.0,0.0).
seed0('3a74efc19dd0d9beb82e189b0e736e4b','http://opendata.caceres.es/dataset/e5a601a1-70bb-46cc-8986-cb54eae0615e/resource/40cec535-a253-40fc-8422-42cf29dd40ea/download/cofradias-2017.ttl',1492902318.8496144,0.0,0.0).
seed0('67e9509a81f4d8dc81917c2c38336c9a','http://opendata.caceres.es/dataset/c0347f89-ce8e-4e3b-9a9d-73af884fc19f/resource/42f4cb2a-5fdc-477e-b307-ffd135786824/download/ruidos.ttl',1492902318.850635,0.0,0.0).
seed0('946a296034741e2f2f6da7340234d667','http://opendata.caceres.es/dataset/2490efc4-3887-4904-b15a-e2f57d89c135/resource/44955719-ded8-497b-b385-eb7d1d39e2e7/download/ejecucion-2015-trimestre-4-gastos.ttl',1492902318.8516693,0.0,0.0).
seed0('45071f1b7926b9ff87454af5bc5145b3','http://opendata.caceres.es/dataset/2cc2ece0-4118-4864-b2e6-f2baccf9d70c/resource/45962ac2-c6ab-42d6-a8f4-629bfa07d8e3/download/farmacias.ttl',1492902318.8532658,0.0,0.0).
seed0('8789adf663cb6df6a4b3457ec809d48f','http://opendata.caceres.es/dataset/b8fd3418-5b08-4904-b528-43eb6974ec78/resource/4736b440-08cd-4b2b-9536-9f349e73a9a3/download/centrosreligiosos.ttl',1492902318.8551552,0.0,0.0).
seed0(b7a84be2ee9f57e90e9d3569fa7289ef,'http://opendata.caceres.es/dataset/392d6926-ff89-42da-93e6-f0a37c107416/resource/4d1bc661-7215-46a6-8629-a5240e6e4e8a/download/empresasartisticas.ttl',1492902318.8571434,0.0,0.0).
seed0(b9db61a829df4ce7d1b61553fde1858e,'http://opendata.caceres.es/dataset/bb7bd23b-dd0d-4d2e-97f2-5d4f8c8d0e70/resource/4ed656b9-35a8-4bfd-bf59-ec161a0b3a38/download/puntosrecogida.ttl',1492902318.8590934,0.0,0.0).
seed0(a30c131e67e156d32a7d49834028e394,'http://opendata.caceres.es/dataset/fe1fa88b-9aa3-4588-b1bb-f600fd581016/resource/51494aad-ff94-4e17-9f63-5109f4234a5e/download/gasolineras.ttl',1492902318.8610241,0.0,0.0).
seed0('1d87a39969f1bd906de91f32499867cc','http://opendata.caceres.es/dataset/a355b23a-d882-4f28-837e-2b0b8ba0df2e/resource/52f99c8b-5adb-4e6e-92ca-c0376766dea1/download/residenciamayores.ttl',1492902318.8623946,0.0,0.0).
seed0(feb2ebef43ef2cb4414874a2ce665f8c,'http://opendata.caceres.es/dataset/6160280a-bf38-4440-a7cd-6ba1671097a6/resource/5349b672-23b0-45cb-b9bf-3ae7afd947ef/download/vias.ttl',1492902318.863596,0.0,0.0).
seed0(d9c160f55cb5f4b8b6e859e9b71655d5,'http://opendata.caceres.es/dataset/dcf156b3-1f00-4a16-8c48-d07ab4fe1331/resource/5a67f9e2-9f00-4a80-a5bf-3ce0a619e962/download/hogaresmayores.ttl',1492902318.8646944,0.0,0.0).
seed0(f752fb0650e8d1ea06809281186fa027,'http://opendata.caceres.es/dataset/5336bed9-60bf-45d6-a2aa-009d8dc04f48/resource/5bf554df-5f34-4395-862b-d36a1f9454a6/download/asociacionesvecinos.ttl',1492902318.865728,0.0,0.0).
seed0(c105b6e094a42e436a9531bdd93bede1,'http://opendata.caceres.es/dataset/14b60340-49d1-4d4d-a03d-dad3d7ce5a62/resource/5cdeea50-76c7-4f84-97f0-13a20e2ca8bb/download/ejecucion20151.ttl',1492902318.8667586,0.0,0.0).
seed0('9271d348ce71628630d7e731928d8f29','http://opendata.caceres.es/dataset/7cb98073-4f6a-42ae-90e2-6726aec848f1/resource/5dbb5a0f-9532-4f80-8a7d-4480984ae53d/download/perfilcontratante.ttl',1492902318.8686543,0.0,0.0).
seed0('2ecbcaed334457162fae7a634da486c9','http://opendata.caceres.es/dataset/bca747bc-72e1-4fe3-ba63-4a53f5cb9a3d/resource/639ced43-a4bd-4530-b378-07e7b45df322/download/restaurantes.ttl',1492902318.870324,0.0,0.0).
seed0('9569b72ec9afad5b4db037a7d6a13fad','http://opendata.caceres.es/dataset/6ccc1dfb-b1cc-4316-86f7-5e058d735425/resource/6536df52-0065-404d-b8c8-acfef4f8a430/download/instalacionesdeportivas.ttl',1492902318.8715034,0.0,0.0).
seed0(fcfff3d91a1c290ca89e676d61915f41,'http://opendata.caceres.es/dataset/aef42d83-215f-4b20-885e-be49b0262ae0/resource/65ab14f4-058a-4784-95ef-74fc61ee5caf/download/teatros.ttl',1492902318.8727067,0.0,0.0).
seed0(bdc3e60862b80b1263cb857df42f1a05,'http://opendata.caceres.es/dataset/ee690da4-c03e-4ce8-8f2b-14df84a591f3/resource/664c1b67-3608-41d5-ab16-c2625c28dc94/download/presupuestos2016aytocc.ttl',1492902318.8737338,0.0,0.0).
seed0('19bf69a9941d3c4bdd90de89150cdd39','http://opendata.caceres.es/dataset/36f6db2e-5c77-4773-8b30-fd6b8c5350d9/resource/6de040e2-1380-4ce2-ae1c-0cb24732e433/download/ludotecas.ttl',1492902318.8748202,0.0,0.0).
seed0(dc039829789acffc07d92a3c786ecc2c,'http://opendata.caceres.es/dataset/83f93dbf-ba4b-4ee9-9c73-7eeb47bb2bab/resource/6f529e6b-c85c-4fa7-8534-81d0f889a646/download/cafebar.ttl',1492902318.876575,0.0,0.0).
seed0(f554a6340fc09a54d88d349821f577b3,'http://opendata.caceres.es/dataset/31b980b2-7d77-4637-a439-ea6c92f1a026/resource/6fd004a4-bc94-4111-a2f1-c8cd46e99f5b/download/plazaszonaazul.ttl',1492902318.8777442,0.0,0.0).
seed0('83ca87916c1b162ec543e8b289cf866f','http://opendata.caceres.es/dataset/0815670a-b30c-4e77-9562-6f5bd2c70197/resource/721702d8-0a05-466d-b0ec-7d489b7cd6b5/download/centrossanitarios.ttl',1492902318.8790228,0.0,0.0).
seed0('01149925e69cd68479942616ca51364e','http://opendata.caceres.es/dataset/2362d8d7-9a18-478c-aa80-26061765ac27/resource/73ae1bef-f360-4a86-b09b-46899730e526/download/parques.ttl',1492902318.8809261,0.0,0.0).
seed0('4e941a72540b3537c5a49795f2b2efea','http://opendata.caceres.es/dataset/4c81047c-b4d4-47e5-9e1a-d6b642db3780/resource/74f3e844-afe7-4605-bf51-d0d7f2a024cc/download/ejecuci-n3ertrimestre2015gastos-xlsx.ttl',1492902318.8828442,0.0,0.0).
seed0('8fffa281884240fb361972848da4378d','http://opendata.caceres.es/dataset/2490efc4-3887-4904-b15a-e2f57d89c135/resource/7b729eab-4c83-4aa9-ab83-bdd889370661/download/ejecuci-n-2015-trimestre-4-ingresos.ttl',1492902318.8848982,0.0,0.0).
seed0('7cc4d9a6b0eee421bb8b49b808100120','http://opendata.caceres.es/dataset/402d34e0-f214-4810-b437-c89c6ba2c3db/resource/7d483a50-c1d9-4b7f-8d8f-630902402edb/download/pasos-2017.ttl',1492902318.88678,0.0,0.0).
seed0('466feb9cd0ba9bf4e65ecf0ddfaf5ca5','http://opendata.caceres.es/dataset/d4935edc-01bc-4fd8-b262-d070f80242a8/resource/7eb3e988-d29a-4247-a5af-8bf93c76eab8/download/casascultura.ttl',1492902318.888693,0.0,0.0).
seed0('6a9f59d95cda36b63278eec818308deb','http://opendata.caceres.es/dataset/6c8acd26-ed03-466c-8f14-f2bb654b87dc/resource/7ff29933-f0b2-4668-91d8-c93452d1e638/download/bibliotecas.ttl',1492902318.8906302,0.0,0.0).
seed0(ac91b12c94bdbc5a9fba91f90d6be8ee,'http://opendata.caceres.es/dataset/844798db-4e93-4595-ac75-0f689600b236/resource/850f6db4-34bf-41e9-b0e6-328f6f5d1edc/download/ejecuciones20141gasto.ttl',1492902318.8918667,0.0,0.0).
seed0('6b886d52140c7ae12e4908a730f833c3','http://opendata.caceres.es/dataset/e0ca2d03-4886-4b4b-a06c-b65293c32337/resource/8542ad18-75c2-4ff2-93d3-0afb806c25de/download/desfibriladores.ttl',1492902318.893011,0.0,0.0).
seed0('59d4310411e0f0fa1ad1d281abbccb65','http://opendata.caceres.es/dataset/d80b3641-41a2-4467-83c8-8ecd6052f7d0/resource/8d47c63f-d88d-44e8-8a87-8802b23e2041/download/presupuestos2014imascc.ttl',1492902318.8949015,0.0,0.0).
seed0(d653ce0790d60ac2f733c8ef9b9c89b2,'http://opendata.caceres.es/dataset/98a8da8a-280d-4aa1-a60b-6839a61cdaa2/resource/8ff47b4e-cdf0-4be3-9c95-130c07d901cd/download/paradastaxi.ttl',1492902318.896576,0.0,0.0).
seed0('25455ff2f5ff3572a6c3f1995013832e','http://opendata.caceres.es/dataset/e847a693-c8f6-44ae-b22f-98ae0f48b5e8/resource/91d136f1-2c25-4322-acbe-45734d6321fe/download/procesiones-2017.ttl',1492902318.897618,0.0,0.0).
seed0('13525ace4af83dc764eca9f4ba79620b','http://opendata.caceres.es/dataset/4eec1e5a-9833-4148-929e-7f7440408422/resource/9f12f003-947d-4856-add8-e5ea45b01adf/download/monumentos.ttl',1492902318.8987374,0.0,0.0).
seed0(e1baacf6232f31079521b4af2f404909,'http://opendata.caceres.es/dataset/af475139-0908-4d0d-9b6e-d7cc98fc1758/resource/a4facabb-b6a5-4d3f-94c2-beabb5faac09/download/presupuestos2012.ttl',1492902318.8998163,0.0,0.0).
seed0(d204dfc4f303ff800cdfcb44e8646550,'http://opendata.caceres.es/dataset/ba86e12b-9657-4f64-8276-ed3af5430b88/resource/a626eff6-0429-444f-be3f-aa9529cd976d/download/agenda.ttl',1492902318.9012854,0.0,0.0).
seed0('6e4304d0386a0b0a0df13103dd043322','http://opendata.caceres.es/dataset/3e71cdf9-a0c9-4e53-9d5f-de0ffc04e9ae/resource/a8d36a0c-b757-4492-a1aa-ddd36dd0d139/download/zonasdeportivas.ttl',1492902318.90232,0.0,0.0).
seed0(c58180cde07572ae3224ba13d0822da7,'http://opendata.caceres.es/dataset/286b7f8b-d55e-440d-83fe-4aba4764aa50/resource/add88fad-cb75-4fd5-a179-b0a8b0f0e697/download/autobuses.ttl',1492902318.9033701,0.0,0.0).
seed0(f753948e42000ff6bcd6e49c177f9507,'http://opendata.caceres.es/dataset/495372a2-8aef-40bc-b019-eeb6e3417f42/resource/aea9cb5f-1a1f-4925-96cd-54d69548f107/download/presupuestos2015.ttl',1492902318.9044137,0.0,0.0).
seed0('98e446dee0bd1b3ccd3eba27065e1476','http://opendata.caceres.es/dataset/c604b405-37af-4afd-95f6-77cd3eb6e0b9/resource/b7622c1e-115f-4aab-b7b1-e39c241a911f/download/barcopas.ttl',1492902318.9055128,0.0,0.0).
seed0('5d39366b11e49033e2bba281272ce55f','http://opendata.caceres.es/dataset/62b7e14c-7926-441a-afd3-aef66c51d42c/resource/be92dde9-4956-44b5-927d-2bfc36f39aef/download/ejecuci-n2-trimestre2015gastos-xlsx.ttl',1492902318.9065878,0.0,0.0).
seed0(af59cc94800383419c777eb80b465163,'http://opendata.caceres.es/dataset/50ced1b4-40d8-44b4-bda5-b0351e2fd626/resource/c27b42f4-01f2-43de-beee-a78745872b26/download/piscinas2016.ttl',1492902318.9076264,0.0,0.0).
seed0('6796ac8ae562c9fdd7ed25eabc9b219c','http://opendata.caceres.es/dataset/d80b3641-41a2-4467-83c8-8ecd6052f7d0/resource/c3175a5b-f33d-4bfa-a7ff-905bf2136060/download/presupuestos2014imjcc.ttl',1492902318.9086676,0.0,0.0).
seed0('0dd7f5f2171363dd5d722fd1aaec9e57','http://opendata.caceres.es/dataset/58fb18ac-0501-4c1b-b003-1906636e9a4d/resource/c621e3e9-fa26-46e9-8da0-e1883ff06999/download/museos.ttl',1492902318.909723,0.0,0.0).
seed0(f852dc86765abd59cedf5a65a9d085ae,'http://opendata.caceres.es/dataset/4b93142a-ddde-41af-ad8f-0396932624c7/resource/ca80c2c1-a642-4488-82f6-0aeaa08ee5ee/download/farolas.ttl',1492902318.9107585,0.0,0.0).
seed0('2cfea4ef654e1db845d2a95e043a6233','http://opendata.caceres.es/dataset/ae4fc7bd-ac91-4d48-bde7-ba18b2dfac3c/resource/d8eff674-1bfa-4bba-b841-60c896a34195/download/actividades.ttl',1492902318.911874,0.0,0.0).
seed0(d6f09972b498752da977df88641a013d,'http://opendata.caceres.es/dataset/f6479d5c-bba6-4146-befd-59efd409df27/resource/dc2b3f8f-193e-47d8-ae29-a1df96348cac/download/ejecuci-n-2016-trimestre-1.ttl',1492902318.9129558,0.0,0.0).
seed0('1298ec63b73967efe7ae1bd4a30483b0','http://opendata.caceres.es/dataset/844798db-4e93-4595-ac75-0f689600b236/resource/df01d7bd-260f-4e28-b632-dc6640541323/download/ejecuciones20141ingreso.ttl',1492902318.9139984,0.0,0.0).
seed0('35816f54dd680f49f9cd4e94bf64dbc3','http://opendata.caceres.es/dataset/cad00ed9-4fa3-4d04-8524-4b7b91ab0813/resource/e1cfc673-a401-49f6-9181-e2a5557a8f7e/download/ejecucion20142ingreso.ttl',1492902318.915031,0.0,0.0).
seed0('0a9261bd0c89d07ba4aecc3b0d236b8f','http://opendata.caceres.es/dataset/611e8197-edb5-4303-9f55-95cd897b8a47/resource/e25dc759-cdf4-42b4-964b-428d2dbf4e08/download/cajeros.ttl',1492902318.9160464,0.0,0.0).
seed0('9caf4b279e0047d93b31e93ca14fc352','http://opendata.caceres.es/dataset/d80b3641-41a2-4467-83c8-8ecd6052f7d0/resource/e3a773be-38ab-4d7a-938d-357ef0a25cf2/download/presupuestos2014imdcc.ttl',1492902318.9170775,0.0,0.0).
seed0('67faa00502b194e067700deef04200ba','http://opendata.caceres.es/dataset/7be6f696-2c25-4b4d-8ce9-e3a7520177f8/resource/e56a568c-1436-4aab-85d8-4fee1ef2fdec/download/alojamientos.ttl',1492902318.9180965,0.0,0.0).
seed0('6ca59802ff211355f31190ff110a0c6e','http://opendata.caceres.es/dataset/4c81047c-b4d4-47e5-9e1a-d6b642db3780/resource/e6c82d9c-0400-4c9a-aa88-11898e16e398/download/ejecuci-n3ertrimestre2015ingresos-xlsx.ttl',1492902318.9191241,0.0,0.0).
seed0('841514170be94a4788ef2c3431d073fd','http://opendata.caceres.es/dataset/e0efcfd6-7583-4a8a-af26-0190ca14dd24/resource/e7eb2d6f-cc59-4d11-967c-9a01e312d80a/download/plazasmovilidadreducida.ttl',1492902318.920163,0.0,0.0).
seed0('95a4333f3dc8b3566dedb830bd9ac983','http://opendata.caceres.es/dataset/cc5e9ed3-2ffe-4da9-8919-33c48c889075/resource/e902556a-a5c8-4fa2-a9e6-75ce6a41b128/download/ejecuci-n-2016-trimestre-2.ttl',1492902318.9211967,0.0,0.0).
seed0(bbffee422f57de624809d4da8e3dcf55,'http://opendata.caceres.es/dataset/913329f3-6173-4fef-b465-b65513499352/resource/e927a4cc-d9bb-4e62-b34c-dad625702740/download/ejecucion20143gasto.ttl',1492902318.9222083,0.0,0.0).
seed0('6c04d1f0c894d8c61fced1656e9a26e4','http://www.comune.pisa.it/it/lod/rdfDataModel-hotspot.rdf',1492903320.3521955,0.0,0.0).
seed0(ba6666f93ec95ca22bd3c87f0dc82d09,'http://www.comune.pisa.it/it/lod/rdfDataModel-bandi-lavori-pubblici.rdf',1492903320.3593786,0.0,0.0).
seed0('2ff970ee8b1c6df24c26b0cdab5dc2a3','http://www.comune.pisa.it/it/lod/rdfSchema.rdf',1492903320.3645403,0.0,0.0).
seed0(d0834d935349bd00c4432a8e919eb949,'http://www.comune.pisa.it/it/lod/rdfDataModel-notizia.rdf',1492903320.3683457,0.0,0.0).
seed0('170f66600028538a99cea729ca7b2429','http://www.comune.pisa.it/it/lod/rdfDataModel-altri-bandi.rdf',1492903320.3735967,0.0,0.0).
seed0(ea98145fce604c30c67eb926fa01840d,'http://data.fusepool.info:8181/ldp/tuscany-museums-1/y-csv-transformed',1492903320.3771043,0.0,0.0).
seed0(d8b71e4275bca13fff7647035911d564,'http://www.comune.pisa.it/it/lod/rdfDataModel-immobile.rdf',1492903320.381857,0.0,0.0).
seed0(b094c8634926c02ab2a526eff8046674,'http://www.comune.pisa.it/it/lod/rdfDataModel-incarico.rdf',1492903320.3865933,0.0,0.0).
seed0(ee6f3eb0ca093a35562ee99905a577f2,'http://data.fusepool.info:8181/ldp/tuscany-accommodations-1/v-csv-transformed',1492903320.4129286,0.0,0.0).
seed0('5bd0244ba6b0025339a21d404e825a32','http://www.comune.pisa.it/it/lod/rdfDataModel-bandi-forniture-servizi.rdf',1492903320.4177415,0.0,0.0).
seed0(cbb14708e6f01ffd3f517203f100707c,'http://www1.siop.planejamento.gov.br/downloads/rdf/loa2004.zip',1492931322.8312109,0.0,0.0).
seed0('8c687bfacd956d005b70f31148a2323f','http://www.portaldocidadao.tce.sp.gov.br/api_rdf_orgaos',1492931322.8369458,0.0,0.0).
seed0(bc22f80fd2fd496b6b60f50d56854649,'http://www1.siop.planejamento.gov.br/downloads/rdf/loa2007.zip',1492931322.8437366,0.0,0.0).
seed0('9a883b5351d388b70177e9bb52c53e01','http://dadosabertos.dataprev.gov.br/storage/f/2015-09-25T20%3A27%3A54.336Z/cr-cred-grupo-especies.rdf',1492931322.8469589,0.0,0.0).
seed0(eaa73809c9839c03f781bae3c199a3a1,'http://dadosabertos.dataprev.gov.br/storage/f/2015-09-10T21%3A39%3A07.854Z/sp-examesnormaisgrupoesp.ttl',1492931322.8495164,0.0,0.0).
seed0(c38cc07b004063fb5f4aa2163f6174b6,'http://www1.siop.planejamento.gov.br/downloads/rdf/loa2001.zip',1492931322.8533723,0.0,0.0).
seed0('5487445192fdae63124edaf256139c37','http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A49%3A13.936Z/at-cnae-95.ttl',1492931322.8641577,0.0,0.0).
seed0(ab45705c3a5078f9e1ca48801b376840,'http://www1.siop.planejamento.gov.br/downloads/rdf/loa2015.zip',1492931322.8676722,0.0,0.0).
seed0('7a58ca68ca06f7ef7c8e006726ad8bea','http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A40%3A48.622Z/at-por-cid.ttl',1492931322.8727295,0.0,0.0).
seed0(bdc0de5ce9d5c8526c9c42bcfeefc79b,'http://www1.siop.planejamento.gov.br/downloads/rdf/loa2008.zip',1492931322.878897,0.0,0.0).
seed0('4523209aa26f47d8b8b64de79747a851','http://dadosabertos.dataprev.gov.br/storage/f/2015-09-30T18%3A11%3A41.182Z/cr-emissao-especies-sexo.ttl',1492931322.8819828,0.0,0.0).
seed0(c705f9022ef4a00c099a68614812e871,'http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A42%3A15.568Z/at-por-mes.ttl',1492931322.8865614,0.0,0.0).
seed0('21ebec12ef6f53754649acdb053e2ecf','http://www1.siop.planejamento.gov.br/downloads/rdf/loa2014.zip',1492931322.8885436,0.0,0.0).
seed0('623a4f18cb6d1573263db295104579c4','http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A43%3A30.177Z/at-por-cbo.ttl',1492931322.8970833,0.0,0.0).
seed0('91570397f89c01162937cb74b5f30734','http://www1.siop.planejamento.gov.br/downloads/rdf/loa2009.zip',1492931322.8992906,0.0,0.0).
seed0(d589e7f629f91f774cc7c3105d7d3c4c,'http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A22%3A34.434Z/at-liquidados-por-uf.ttl',1492931322.9003813,0.0,0.0).
seed0('98cf0ec7c3c500cf14dc5e4d716a6fdb','http://dadosabertos.dataprev.gov.br/storage/f/2015-08-28T20%3A32%3A59.413Z/sp-nao-conclusivo-uf.ttl',1492931322.903053,0.0,0.0).
seed0('6a4572244631e21d47a330a9fbd42c3e','http://www1.siop.planejamento.gov.br/downloads/rdf/loa2000.zip',1492931322.9063334,0.0,0.0).
seed0(ae64a6db212c87c19cae96fde2e27bdb,'http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A25%3A26.316Z/at-por-uf.ttl',1492931322.9112773,0.0,0.0).
seed0('5e1ab83213dd1c7cced1367f262beaa3','http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A44%3A47.494Z/at-liquidados-por-mes.ttl',1492931322.9183564,0.0,0.0).
seed0('13a798e6fac9da4242469a07e5dcc3ab','http://api.comprasnet.gov.br/sicaf/v1/consulta/fornecedores.rdf?uf=RN',1492931322.9195788,0.0,0.0).
seed0(ec8c4dcaa529272c5247995c8f4ae56f,'http://dadosabertos.dataprev.gov.br/storage/f/2015-09-30T21%3A23%3A47.099Z/cr-emissao-faixa-valor-e-especies.ttl',1492931322.923581,0.0,0.0).
seed0(f2afb50b1ed94045ac07b7d3517f95c9,'http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A29%3A39.082Z/at-por-idade.ttl',1492931322.9279027,0.0,0.0).
seed0('9b10c6fcb6388ccaaf795e0ffc22b938','http://www1.siop.planejamento.gov.br/downloads/rdf/loa2010.zip',1492931322.9311965,0.0,0.0).
seed0('6d21a83e97fae572ea75c0ae6bea9953','http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A12%3A14.117Z/at-por-parte-do-corpo-atingida.ttl',1492931322.9468377,0.0,0.0).
seed0('76231c258c6de63986ef790c99dbf97d','http://www1.siop.planejamento.gov.br/downloads/rdf/loa2003.zip',1492931322.952034,0.0,0.0).
seed0('7d5e61d935ccb81bff0844ae01725800','http://www1.siop.planejamento.gov.br/downloads/rdf/loa2005.zip',1492931322.9565163,0.0,0.0).
seed0(f15ec130f97a3337adc7cd290c124796,'http://dadosabertos.dataprev.gov.br/storage/f/2015-10-01T20%3A50%3A25.227Z/cr-emissao-meio-pagamento.ttl',1492931322.9593837,0.0,0.0).
seed0('4d2c2054ace99bfdd33b0403e941b522','http://dadosabertos.dataprev.gov.br/storage/f/2015-09-11T18%3A09%3A51.119Z/sp-reabilitacaoporuf.ttl',1492931322.961525,0.0,0.0).
seed0(b845d55c2b891d710de3adb157dc70c1,'http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A46%3A28.132Z/at-cnae-20.ttl',1492931322.9676805,0.0,0.0).
seed0(a5d3325d5cb777254feb59bf61ae958d,'http://www1.siop.planejamento.gov.br/downloads/rdf/loa2016.zip',1492931322.9686217,0.0,0.0).
seed0('6abd666e7b205df9e7dfccea49f537a8','http://www1.siop.planejamento.gov.br/downloads/rdf/loa2006.zip',1492931322.9707267,0.0,0.0).
seed0('9d3541e0c6c7dc3356066a5fd470ef64','http://www1.siop.planejamento.gov.br/downloads/rdf/loa2013.zip',1492931322.9748595,0.0,0.0).
seed0(bf714146a84ded5890494faa20db2ff9,'http://dadosabertos.dataprev.gov.br/storage/f/2015-09-10T17%3A50%3A30.109Z/sp-servicosocialuf.ttl',1492931322.978996,0.0,0.0).
seed0('893a7ffa64cb7df8fa009934f7f0f8ef','http://www.portaldocidadao.tce.sp.gov.br/api_rdf_municipios',1492931322.9799356,0.0,0.0).
seed0(b4d0e050cc7216134a2fa040e9a467ee,'http://www1.siop.planejamento.gov.br/downloads/rdf/loa2012.zip',1492931322.9813306,0.0,0.0).
seed0('6e28730b5521d938f853c03e45a4d380','http://dadosabertos.dataprev.gov.br/storage/f/2015-08-28T20%3A30%3A15.019Z/sp-exames-especialidade.ttl',1492931322.983476,0.0,0.0).
seed0(dc1078ac88ec4460dffd1ea66fd01340,'http://dadosabertos.dataprev.gov.br/storage/f/2015-09-25T20%3A47%3A01.531Z/cr-cred-conc-uf.rdf',1492931322.9844031,0.0,0.0).
seed0('0d25e59fd104ee5e8c44380d4eec8871','http://dadosabertos.dataprev.gov.br/storage/f/2015-09-10T21%3A11%3A08.419Z/sp-examestipouf.ttl',1492931322.986417,0.0,0.0).
seed0('59d6670444821d99be253b7e72f4afb9','http://www1.siop.planejamento.gov.br/downloads/rdf/loa2002.zip',1492931322.9897492,0.0,0.0).
seed0(d4b9999bd4b1577c30a207a0c7fd535e,'http://dadosabertos.dataprev.gov.br/storage/f/2015-09-28T20%3A10%3A17.798Z/cr-cred-mensal-pais.ttl',1492931322.9927099,0.0,0.0).
seed0(e96cc70df846a647d42d0fd02c4c6693,'http://dadosabertos.dataprev.gov.br/storage/f/2015-09-10T14%3A16%3A50.035Z/sp-conclusivo-uf.ttl',1492931322.9940996,0.0,0.0).
