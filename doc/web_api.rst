Web API
*******

Relational Queries
==================

We export a SQL API to the client, via honeysql (EDN data representing SQL AST). ::
 
    {:select [:*] :from [:dataset] :limit 10}

Brief honeysql documentation is at https://github.com/jkk/honeysql. Xena
supports the hashmap (curly brace) format described on the page.

This approach exposes the client to details of the sql engine, which is unfortunate. For
example, h2 will not find indexes for joins that are grouped with
parenthesis. Users will have
to avoid using parenthesis for certain joins. Additionally, h2 will not use indexes
for large ``in`` queries. To make use of indexes in that scenario we have
to perform an inner join on a table literal with the values to be matched.

Details like this make it desirable to abstract the query engine by writing a
domain--specific
query language (DSL). This is a potentially large effort, and requires more
insight into what set of query operations would sufficiently cover current
and future use cases. Picking a DSL restricts future queries. One possible
way to mitigate that limitation is by automatically pushing xena updates
(much like automatic web browser updates). With automatic updates, we could
push new DSL features as needed.

Analytic Queries
================
Simple analytic queries are supported by exposing a scheme interpreter, with
support for matrix math operations. ::

    (+ 1 2) ; returns 3

The advantage of using scheme in this role is simplicity of implementation. A
simple scheme interpreter is about a page of code. This prevents the language
from becoming a large project of its own. For this reason scheme has been used
in similar roles, in many contexts, for decades.

The drawback of this approach is that it creates an unfamiliar, idiosyncratic
language. An alternative is to expose an existing language.

Exposing an existing language, such as clojure (the implementation language of
Xena Server), or python (available via jython) is tempting, but also has drawbacks.
In particular, it is difficult to securely sandbox full--featured languages. This
is especially concerning since Xena is intended to be a customer--deployed server:
the security risks are assumed by our customers.

Scheme Overview
===============

The scheme interpreter only supports expressions. There are no side--effecting
functions, and so no statement positions are supported (i.e. no ``do``, or
implicit ``do``).

No macros, or reader macros are supported.

Special Forms
-------------

fn
__
Define a lambda expression (anonymous function). ::

    (fn [x] (+ 1 x))

quote
_____
Read the next form as a literal. ::

    (quote (1 2 3))

let
___
Bind values to local varibles. This ``let`` is non-destructuring, but does
allow references to preceding variables in the same ``let`` expression. ::

    (let [x 2
          y (+ x 2)]
      (* x y)) ; returns 8

if
__
Conditional. ::

    (if x (+ x 1) 0) ; return x + 1 if x is not nil, otherwise 0

Data Types
----------
Data types are inherited from the underlying clojure implementation.

http://clojure.org/data_structures

Of general interest are numbers, strings, keywords, symbols,
hash maps (dictionaries), and vectors (arrays). ::

    :a            ; keyword, primarily useful as keys for hash maps
    {:a 5 :b 12}  ; hash map with entires :a, :b
    ["foo" "bar"] ; vector of two strings

Note that keywords operate as functions of hash maps, looking themselves up
in the hash map. ::

    (map :x [{:x 2} {:x 3}]) ; returns [2 3]


Functions
---------

Math
____
Matrix-aware math operators include ``+ - / * > < >= <= =``.

``mean`` computes an average over the given dimension of a  matrix.

Functional methods
__________________
Typical functional methods include the following. This list is expected to
grow substantially.

* ``map`` --- Map a function over a collection.
* ``cons`` --- Prepend an element on a collection.
* ``car`` --- Return the first element of a collection.
* ``cdr`` --- Return the elements after the first in a collection.
* ``group-by`` --- Group elements of a collection by the given key function.
* ``assoc`` -- Set a value in a nested structure. Uses the clojure implementation.
* ``get`` -- Get a value in a nested structure. Uses the clojure implementation.
* ``apply`` -- Invoke a function with a vector of parameters.

Data Access
___________
These functions provide access to the Xena database.

``fetch`` returns a matrix of rows from the given dataset fields that match the given
sampleIDs. ::

    (fetch {:samples ["sampleA" "sampleB"]
            :columns ["probeA" "probeB"]
            :table "public/TCGA/BRCA/exon"})

.. note :: The ``columns`` parameter should really be ``fields``.

.. note :: This method is only useful for datasets with a ``sampleID`` field.
   It could be generalized to work for fields other than ``sampleID``. Alternatively,
   this method may be deprecated in favor of the ``unpack*`` sql functions that
   provide a more general solution.

``query`` executes a honeysql query. ::

    (query {:select [:*] :from [:dataset] :limit 10})

SQL Functions
_____________
H2 database functions are available via honeysql with the ``#sql/call`` form. ::

    (query {:select [#sql/call [:exp 1]]})

Documentation of h2 functions is here: http://h2database.com/html/functions.html.

Custom functions are provided for unpacking Xena field blobs.


.. c:function:: unpack(field_id, row)

Retrieve the specified virtual row for the field
field_id, as a floating--point number.

.. c:function:: unpackCode(field_id, row) 

Retrieve the specified virtual row for the
field field_id, as
an integer number. This is primarily of interest for categorical fields.

.. c:function:: unpackValue(field_id, row)

Retrieves the specified virtual row for the
field field_id,
and looks up the corresponding string in the ``code`` table.
This is only of interest for categorical fields.

All of these methods cache retrieved segments, and internally use prepared
statements to fetch on cache-miss, so they are reasonably performant when
doing a table scan.
