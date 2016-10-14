:- module(llw_rdf11, []).

/** <module> LOD Laundromat Web: RDF 1.1 page

@author Wouter Beek
@version 2016/03, 2016/08-2016/09
*/

:- use_module(library(html/html_ext)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_ext)).

:- use_module(llw(html/llw_html)).

:- http_handler(llw(rdf11), rdf11_handler, []).





rdf11_handler(_) :-
  reply_html_page(
    llw(false),
    \title(["RDF 1.1","SWI-Prolog's RDF library"]),
    article([
      \rdf11_header,
      \rdf11_term,
      \rdf11_triple,
      \rdf11_graph,
      \rdf11_graph_theory,
      \rdf11_semantics
    ])
  ).

rdf11_header -->
  html(
    header(
      \llw_image_content(
        rdf_w3c,
        [
          h1("RDF 1.1 library"),
          p("Assert, retract and query RDF data.  Advanced support for datatypes.  Constrained-based literal matching and comparison.")
        ]
      )
    )
  ).

rdf11_term -->
  html(
    section([
      h2(\anchor(rdf_term, "RDF Terms")),
      \rdf11_term_iri,
      \rdf11_term_literal,
      \rdf11_term_bnode
    ])
  ).

rdf11_term_iri -->
  html(
    section([
      h3("IRIs"),
      p([
        "IRIs are represented by Prolog atoms.  Since IRIs contain characters like slash an colon that have special meaning in Prolog IRIs need to be enclosed in single quotes.  ",
        html_code("'http://example.org'"),
        " is an example of an IRI (notice the surrounding single quotes!).  If you know that something is an RDF term, you can check whether it is an IRI with predicate ",
        html_code("rdf_is_iri/1"),
        ".  However, this predicate also succeeds for many atoms that are not RDF terms, e.g., ",
        html_code("rdf_is_iri(not_an_iri)"),
        " succeeds even though ",
        html_code("not_an_iri"),
        " does not follow IRI syntax.  If you want to check whether an arbitrary Prolog term is an IRI or not you can use ",
        html_code("uri_is_global/1"),
        " from module ",
        html_code("library(uri)"),
        ".  The latter does require some parsing and is therefore slower than ",
        html_code("rdf_is_iri/1"),
        ", which is prefered in situations where you know that the term you are checking is an RDF term."
      ]),
      p([
        "IRIs can become quite long.  Take for instance the following IRI which is used very often in RDF data: ",
        html_code("'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'"),
        ".  Clearly it would be ugly to include this lengthy IRI in your Prolog source code and it would be a nuisance to have to type it repeatedly at the Prolog top-level when performing an RDF query.  Therefore IRIs can be compactly represented by compound terms of the form ",
        html_code("Alias:Local"),
        ", where ",
        html_code("Alias"),
        " stands for a, typically lengthy, IRI prefix and ",
        html_code("Local"),
        " is the, generally much briefer, IRI suffix that remains.  For our example we can write ",
        html_code("rdf:type"),
        " instead of ",
        html_code("'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'"),
        ", since ",
        html_code("rdf"),
        " is the common alias for the IRI prefix ",
        html_code("'http://www.w3.org/1999/02/22-rdf-syntax-ns#'"),
        ".  A list of common aliases is maintained by the Web site ",
        \external_link('http://prefix.cc'),
        ".  Notice that in the abbreviated notation ",
        html_code("rdf:type"),
        " we do not need to enclose any part between single quotes, since ",
        html_code("rdf"),
        " and ",
        html_code("type"),
        " are both legal Prolog atoms."
      ]),
      p([
        "The RDF 1.1 library predefines several common prefixes (see Table 1).  Other prefixes can be defined with the predicate ",
        html_code("rdf_register_prefix/2"),
        " which takes an alias and the IRI prefix it abbreviates (in that order).  For instance, suppose we use the IRIs ",
        html_code("'http://example.org/a'"),
        " and ",
        html_code("'http://example.org/b'"),
        " a lot in our code.  It makes sense to add the directive ",
        html_code(":- rdf_register_prefix(ex, 'http://example.org/')."),
        " at the beginning of our program.  We can now write the briefer ",
        html_code("ex:a"),
        " and ",
        html_code("ex:b"),
        " instead."
      ]),
      \table(
        tr([th("Alias"),th("IRI prefix")]),
        [
          tr(["dc",\external_link('http://purl.org/dc/elements/1.1/')]),
          tr(["dcterms",\external_link('http://purl.org/dc/terms/')]),
          tr(["eor",\external_link('http://dublincore.org/2000/03/13/eor#')]),
          tr(["foaf",\external_link('http://xmlns.com/foaf/0.1/')]),
          tr(["owl",\external_link('http://www.w3.org/2002/07/owl#')]),
          tr(["rdf",\external_link('http://www.w3.org/1999/02/22-rdf-syntax-ns#')]),
          tr(["rdfs",\external_link('http://www.w3.org/2000/01/rdf-schema#')]),
          tr(["serql",\external_link('http://www.openrdf.org/schema/serql#')]),
          tr(["skos",\external_link('http://www.w3.org/2004/02/skos/core#')]),
          tr(["void",\external_link('http://rdfs.org/ns/void#')]),
          tr(["xsd",\external_link('http://www.w3.org/2001/XMLSchema#')])
        ])
      ),
      p([
        html_code("rdf_global_id(-, +)"),
        " uses the alias that is associated with the longest IRI prefix.  For instance, in the following example alias ",
        html_code("b"),
        " is used since it results in a shorter local name than when alias ",
        html_code("a"),
        " would have been used."
      ]),
      \html_code("?- rdf_register_prefix(a, 'http://a/').
?- rdf_register_prefix(b, 'http://a/b/').
?- rdf_global_id(Alias:Local, 'http://a/b/c').
Alias = b,
Local = c"),
      p([
        "Compact IRI notation is expanded by concatenating the local name to the namespace, resulting in an absolute IRI.  Since compact IRI expansion is implemented with ",
        html_code("expand_goal/2"),
        " the use of these constructs in compiled code does not incur a performance penalty."
      ])
    ])
  ).

rdf11_term_literal -->
  html(
    section([
      h3("RDF Literals"),
      p("")
    ])
  ).

rdf11_term_bnode -->
  html(
    section([
      h3("Blank nodes"),
      p([
        "A new blank node is created with the predicate ",
        html_code("rdf_create_bnode/1"),
        "."
      ])
    ])
  ).

rdf11_triple -->
  html(
    section([
      h2("RDF Triples"),
      p([
        "The core structure of RDF data is an RDF ",
        \concept("triple"),
        ", i.e., a tuple consisting of three terms.  As we learned in ",
        \ref(rdf_term, "a previous section"),
        ", terms are divided into IRIs, literals and blank nodes."
      ]),
      \rdf11_triple_positional
    ])
  ).

rdf11_triple_positional -->
  html(
    section([
      h3(
        \anchor(positional_types, "Positional types of RDF terms")
      ),
      p([
        "RDF terms can also be subdivided according to the position they take in a triple.  The first term in a striple is called its ",
        \concept("subject"),
        " the second its ",
        \concept("predicate"),
        " and the thrid its ",
        \concept("object"),
        "."
      ]),
      p([
        "When variables are used to denote arbitrary RDF terms in the context of a triple, then the variable names ",
        html_code("s"),
        ", ",
        html_code("p"),
        " and ",
        html_code("o"),
        " are often used to indicate an arbitrary subject, predicate and object term: ",
        html_code("〈s,p,o〉"),
        "."
      ]),
      p([
        "Notice that the positional types of a term are determined ",
        em("relative to a triple"),
        ".  Specifically, the positional types of an RDF term may be differ for different triples.  For example, ",
        html_code("x"),
        " is a subject term in triple ",
        html_code("〈x,p,o〉"),
        " but a predicate term in triple ",
        html_code("〈s,x,o〉"),
        "."
      ])
    ])
  ).

rdf11_triple_kinds -->
  html(
    section([
      h3(
        \anchor(
          positional_types,
          "Relation between positional types and kind types of RDF terms"
        )
      ),
      p([
        "There is a relation between the previously introduced ",
        \ref(kind_types, "kind types"),
        " and ",
        \ref(positional_types, "positional types"),
        " for RDF terms: the latter formulate restrictions on the former.  The following two restrictions hold:"
      ]),
      ul([
        li("A term that is a subject term in some triple cannot be a literal."),
        li("A term that is a predicate term in some triple must be a predicate.")
      ]),
      \todo("What is the motivation for these positional restrictions on kind types?")
    ])
  ).

rdf11_graph -->
  {http_absolute_location(img('rdf_graph.svg'), Img)},
  html(
    section([
      h2("RDF Graphs"),
      p([
        "A set of triples is called a ",
        \concept("graph"),
        ".  The image on the right shows a graph that consists of a single triple.  The names of the nodes and edge are the ",
        \ref(positional_types, "positional types of the terms"),
        "."
      ]),
      figure(Img, "Simple RDF graph."),
      p("The positional type for RDF terms can also be scoped to work at the graph level.  We can talk about the subjects, predocate, objects and nodes of a graph.  Since positional term types are relative to a triple, these sets may overlap.")
    ])
  ).

rdf11_graph_theory -->
  html(
    section([
      h2("RDF and graph theory"),
      p("You have to be a little careful when applying graph theory to RDF data.  RDF data structures and graphs are closely related, but they also differ in subtle ways."),
      p("The first different is that nodes and edges are not distinct.")
    ])
  ).

rdf11_semantics -->
  html(
    section([
      h2("Semantics"),
      \rdf11_semantics_term,
      \rdf11_semantics_diff,
      \rdf11_semantics_specific,
      \rdf11_semantics_triple,
      \rdf11_semantics_speech
    ])
  ).

rdf11_semantics_term -->
  html(
    section([
      h3("The semantics of terms"),
      p("Everything until now has been relatively simple.  We discussed the abstract syntax of RDF data as well as some of the more common conrete syntaxes.  If you master one or two concrete RDF syntaxes then you are probably good to go to work with Linked Data in practice.  Most Linked Data pratitioners never progress beyond the level of syntax anyway.  However, if you want to progress beyond the level of sytax then the next sections may be of interest to you."),
      p("The reader must be aware that the semantics of RDF data is still largely an open topic.  At the moment, only the notion of entailment is well-defined.  This means that we will sometimes have to come up with novel theoretical contributions ourselves in order to be able to progress."),
      p([
        "An RDF term denotes a ",
        \concept("resource"),
        ", sometimes called a ",
        \concept("thing"),
        " or ",
        \concept("entity"),
        ".  Since nobody knows what resources, things or entities are, we say that anything is a resource.  Since the word ‘anything’ is merely a grammarical variant of the word ‘thing’, we have still no idea what we are talking about.  We sometimes try to mask the fact that we do not know what resources are by enumerating categories of resources, e.g., physical objects, online documents, numbers, abstract ideas are all resources.  All this does not teach us anything about semantics.  It only tells us that we will use words such as ‘resource’ to denote the denotations of RDF terms.  Sometimes people completely mess things up by saying that a resource “denotes something in the world”.  If the world is what we commonly consider it to be, then this statement is inconsistent with saying that RDF terms denote anything.  Specifically, there is a problem with denoting things that are not in the world.  The only way in which this statement can be anything but ridiculous is when it is considered to define what the world is.  It is then similar to the first proposition of the Tractatus Logico Philosophicus, which defines the world as everything that is the case.  On the Semantic Web people could be considered to define the world as the collecting of things that RDF terms refer to, although it is not clear whether they have this definition in mind when they make the aforementioned satement."
      ]),
      p("Whatever the exact wording may be, Semantic Web practitioners do not know what the denotations of RDF terms are and they use different words to denote these mysterious denotations.  I'm curious whether RDF terms can denote contingently non-existing things, e.g., can there be a term “the king of France”?  I'm also curious whether RDF terms can denote necessarily non-existing things, e.g., can there be a term “a suqare circle”?"),
      p([
        "Denotations are sometimes split into different kinds, depending on the terms that denote them.  Resources denoted by IRIs are called ",
        \concept("referents"),
        " and resources denoted by literals are called ",
        \concept("literal values"),
        ".  This is a rather nutty practice because everything that can be denote by an IRI can also be denote by a literal and vice versa.  For instance the resource denoted by the IRI ",
        html_code("ex:amsterdam"),
        " can also be denoted by the literal ",
        html_code("\"Amsterdam\"@en"),
        ", and the literal ",
        html_code("\"-1\"^^xsd:integer"),
        " can also be denoted by the IRI ",
        html_code("ex:minus_one"),
        ".  We conclude that ‘referent’ and ‘literal value’ are further synonyms of ‘resource’."
      ]),
      p([
        "The set of resources is sometimes called the ",
        \concept("universe of discouse"),
        ".  The universe of discouse is traditionally the narrower set of terms and expressions that are currently under discussion.  In the case of the Semantic Web we want to be able to talk about anything so the universe of discourse is unrestricted."
      ])
    ])
  ).

rdf11_semantics_diff -->
  html(
    section([
      h3("The difference between IRI and literal denotations"),
      p("Now we come to an interesting mistake: it is sometimes assumed that the denotations of IRIs are very different from the denotations of litetals.  This is not the case."),
      \todo("What is the difference between kind types semantically?  IRIs and literals are universally interchangeable.  IRIs and literals are akin to nouns and names in natural language.  E.g., the name “Amsterdam” can be interchanged between users of the English language to denote a city in the Netherlands.  Similarly, the word “car” can be interchanged between users of the English language to denote a specific concept.  Blank nodes also denote resources, but they cannot be universally interchanged.  Blank nodes are akin to indefinite pronouns in natural language.  E.g., the word “something” can be used in two utterances with a completely different denotation.  As we learned from the abstract syntax, RDF terms come in three kind types: IRIs, literals and blank nodes.")
    ])
  ).

rdf11_semantics_specific -->
  html(
    section([
      h3("Specific resources"),
      p("Some people want to distinguish the resources denoted by IRIs and literals from the resources denote by blank nodes.  They do so by using two undefined notions: (1) naming and (2) identification.  It is not entirely clear what they mean by this.  It is not the identification that is practically implemented by the dereferenceability of HTTP(S) IRIs, because it also extends to literals that cannot be dereferenced.  It is said that IRIs and literals identify specific resources while blank nodes do not.")
    ])
  ).

rdf11_semantics_triple -->
  html(
    section([
      h3("Meaning of a triple"),
      p([
        "Traditionally, a ",
        \concept("relation"),
        " is a set of tuples.  A set of tuples of length one is a ",
        \concept("property"),
        ".  In RDF semantics a property is a resource that has a binary relation as its extension."
      ])
    ])
  ).

rdf11_semantics_speech -->
  html(
    section([
      h3("Speech act"),
      p("When talking about RDF pragmatics, people sometimes conflate abstract syntax with semantics.  For instance, they say that an RDF triple (abstract syntax) can be asserted (pragmatics).  It is better to say that an RDF triple (abstract syntax) denotes an RDF statement (model-theoretic semantics) and that statement can be asserted."),
      p("It is claimed that the assertion of an RDF triple states that the relation denoted by the predicate term holds between the resource denoted by the subject term and the resource denoted by the object term.")
    ])
  ).
  




% HELPERS %

concept(Content_2) -->
  html(span(class=concept, Content_2)).

todo(Content_2) -->
  html(p(class=todo, ["[TODO] ",html(Content_2)])).
