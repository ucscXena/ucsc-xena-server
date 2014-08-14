Xena Database Schema
********************

Conceptually, Xena is a column store. However, we currently
use h2, a row store, for persistence and query. We expose the
underlying h2 query engine to the user, meaning the user must
be aware of the row-based schema.

We refer to the conceptual columns as *fields*, and the conceptual
rows as *virtual rows*. We refer to h2 rows and columns as rows and
columns, respectively.

Tables
======

dataset
-------
The ``dataset`` table is a list of all datasets. At minimum, this is an
id, and a name, which is the path of the input file relative to the document
root. Additional metadata (e.g. from a cgdata ``.json`` file) may be associated with
the dataset. Of note is the ``cohort`` field, which defines the namespace of
any sampleIDs in the dataset.

field
-----
The list of fields in the datasets.

field-score
-----------
A join table, associating fields with blocks of values. This is the
basic mechanism of the column store abstraction. All the virtual rows of a single field
are
split into segments of equal length and stored in a table of binary blobs (``scores``).
The ``field-score`` table is a one-to-many association between a field and the
segments holding the virtual rows of that field. The table includes a column ``i`` which
specifies the order of the segments in the field. E.g. if the rows are split into
two segments, the early rows are in segment 0, the later rows in segment 1.

.. note:: This table could probably be eliminated by using a composite [``field``,
   ``i``] key on the scores table.

scores
------
The table of blobs, holding the segments of the fields. This is essentially
a key/value table, and a key/value store such as jdbm might be better suited
to this role either in terms of performance or disk usage. A k/v store might also offer
more flexible indexing methods, e.g. we might be able to drop the
``field-position`` or ``field-gene`` tables.

feature
-------
A table of metadata associated with fields. Any field in any dataset may have associated
metadata, such as labels. Currently only the cgdata clinicalMatrix reader
provides field metadata.

code
----
A table of string values associated with categorical fields (enumerations).
For example, field
"Stage" might have code "I" for value 0, code "II" for value 1, and so-forth.

field-position
--------------
A table of genomic positions, defined as a tuple (chromosome, chromStart, chromEnd,
strand). Because of the need to index position fields, these fields are stored
as rows rather than blobs, in the ``field-position`` table. A UCSC bin column
is calculated automatically when loading position fields.

field-gene
----------
A table of genes associated with a *virtual row*. To support indexing, and
a one-to-many relation between virtual rows and genes, genes are stored as
rows rather than blobs.

Schema In Detail
================

`SchemaSpy <schema/index.html>`_
