UCSC Xena Server Design
***********************

`Dependencies drawn with graphviz <dependencies.png>`_

Modules For Data Ingestion
==========================

The primarly modules involved in data ingestion are shown below, with
module dependencies drawn beneath the module.

+----------------------------+
|        cavm.core           |
+----------------------------+
|       cavm.loader          |
+-------------+--------------+
|    cavm.db  | cavm.readers |
+-------------+--------------+
|   cavm.h2   | cavm.cgdata  |
+-------------+--------------+
| H2 database | cgdata.core  |
+-------------+--------------+
|             | Filesystem   |
+-------------+--------------+

Briefly, ``cavm.core`` is the *main* method, providing command line processing,
configuration, and launch of the application. The ``cavm.loader`` module
provides a file loading interface bound to a particular file reader and database.
``cavm.db`` provides an abstract interface to xena operations on a database
(e.g. ``run-query``, ``write-matrix``), which is implemented by the ``cavm.h2``
module, using the H2 database as a backend. The ``cavm.readers`` module provides
interfaces for detecting the type of a file, and parsing a file. These interfaces
are implemented by ``cavm.cgdata``, which uses ``cgdata.core`` to read cgdata
files.

Modules For Data Serving
========================

+---------------------------------------------------------------------------------------+
|        cavm.core                                                                      |
+---------------------------------------------------------------------+---------+-------+
| cavm.views.datasets                                                 | cavm.h2 | jetty |
+-----------------------+----------------------+-----------+----------+---------+-------+
| cavm.query.expression | cavm.query.functions | liberator | cavm.db  |    H2   |       |
+-----------------------+----------------------+-----------+----------+---------+-------+
|                       |    core.matrix       |                      |         |       |
+-----------------------+----------------------+----------------------+---------+-------+

When serving data, ``cavm.core`` retrieves an H2 implementation of the ``cavm.db``
interface, and brings up jetty over the views in ``cavm.views.datasets``, passing
in the database. 

``cavm.views.datasets`` uses the Liberator library for AJAX
handling,
and for generating responses evaluates scheme expressions by passing them to
``cavm.query.expression``. A root context for symbol resolution is passed,
which includes standard scheme functions that are defined in ``cavm.query.functions``.
Matrix math operators are provided by ``core.matrix``.


Database backend: h2.clj
========================

The h2 backend provides data persistence and query. H2 is an embedded, pure-java, sql database.
It was chosen primarily for turn-key deployment to user machines having small to medium data
sizes. We are experimentally using it for our larger, curated data, as well, with plans to fall back
to a conventional sql database if it does not perform well enough. H2 is not
designed for highly-concurrent usage, but on a high-memory server with SSD
it may be good enough for our anticipated loads. Further testing is needed.


H2 is a row store. Our usage patterns are a better fit for a column store.
In database terms, a *column* is a set of values all having the same data type, e.g.
a chromosome name. A *row* is a set of values each having potentially different data
types, e.g. a three-tuple of chromosome name, start position, and end position. In our data,
sample name, chromosome, score for probe X, and age are different *columns*.

However we can't store the data in this orientation in a row store.  We store
very large numbers of columns (hundreds of thousands); we access small numbers of columns
at a time, rather than whole rows; and we often must do table scans, because it is impractical to
index hundreds of thousands of columns. Rows stores are poor at all of these things.

Consequently we build a virtual column store over h2, by packing columns into binary blobs, and
providing functions for extracting values from the blobs. We use the term "field" to describe a
column, and build a table of fields. To avoid further confusion, in the rest of
this document the term "field" will mean a column in our virtual column store.
"Virtual row" will refer to a row in our virtual column store. The terms row
and column will refer to the implementation, i.e. the literal rows and columns
in our h2 tables.

The gene and position types are handled separately from the other field types
because of the requirement that they be
indexed. H2 can't build an index on an opaque blob, so we can't store
chromosome name as a category field,
which is stored as a blob.

Additionally, the gene type is separate from the other types because it is structured.


File readers, readers.clj
=========================

The application file reader is created by calling cavm.readers/reader with a list
of file format readers. This function will try each file format reader until one
succeeds, or all of them fail. Each reader should return the type of the file,
and a list of cavm data elements that can be read from the file.  ::

    {:file-type "tsv"
     :data [{:type "genomicMatrix"
             :file "path/to/file"
             :meta {:probeMap "path/to/probemap"}
             :features nil}]}


CGData
------

CGData files are strict tsv files having metadata in associated json files with
matching base names, e.g. ``cnv.tsv`` and ``cnv.tsv.json``.

The json files may have references to other files which are necessary for
interpretation of the data. File references are either relative or absolute
paths. Relative paths are interpreted relative to the refering file. Absolute
paths are interpreted relative to a document root, which must be passed to APIs
that read CGData files.

CGData probemap references
--------------------------

The CGData specification connects related files through abstract names. For example,
if a genomic matrix metadata refers to probemap "probemapA", and a probemap
metadata includes name "probemapA", those files are connected through the probemap
relation.

This has a number of disadvantages, including being difficult for users to understand,
and being expensive for the loader to resolve: there is no algorithm for finding
the probemap for a genomic matrix, apart from scanning every probemap until a
matching name is found.

Instead, Xena expects such references to refer directly to files. A genomic
matrix metadata should have probemap equal to the file name of the associated probemap.

It's worth noting that this is similar to the json-ld format. We should consider
just using json-ld.

Xena has a utility command, ```--json```, which will rewrite all cgdata json metadata in
a directory tree to be file references instead of references to abstract names.

