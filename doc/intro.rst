Introduction to UCSC Xena
*************************

Overview
========
The UCSC Xena Server is a turn-key, secure data server for visualization of
functional genomics data. Key features are automated data ingestion, flexible input
format, data access via HTTP, indexing for fast visualization, support for ad hoc
relational queries, and simple, ad hoc analyic queries.

Target users include bench biologists with limited IT skills and small amounts
of data, as well as organizations with dedicated IT support and larger datasets.

In concert with UCSC Xena Browser, the platform permits viewing of private data
sets alongside public, curated datasets, without the need to send the private data
off-site.

Deployment scenarios
====================

Single user
-----------

A single user having data on their laptop can start a Xena
server to allow viewing that data together with UCSC's curated data.  This
may be a single-jar install that the user can launch by double-clicking.
A simple, embedded sql backend will be used.

We should investigate install tools that can assist the user in installing
java if their host does not have an appropriate version.

Files in a data directory are loaded into the database automatically,
and updated whenever the files change, so the user can see their updated
data simply by saving new versions. The server should eventually support
a push technology, such as SSE, webRTC, or WebSockets, so clients can be
notified of data changes without user intervention.

Organization
------------

An organization can deploy a Xena server behind a firewall to allow access
by many local users. This install will likely require some IT support to
bring up java and start Xena from the command line, setting appropriate options
for the data directory, etc.

This might use an embedded sql backend, or a conventional sql server

Cloud
-----

Cloud deployment will be similar to organization deployment, with the
added possibility that UCSC will provide pre-installed images for
common cloud vendors.

Data Model
==========

Xena stores data that is essentially tabular, with limited support for
structured fields (in particular, lists of genes). The data model is
an evolution of the cgdata data model. cgdata defines three data formats
of interest: clinicalMatrix, genomicMatrix, and probeMap.

cgdata
------
A cgdata **clinicalMatrix** is a TSV with columns for each phenotype and
rows for each sampleID. Note that this orientation corresponds to the
terms *row* and *column* in database literature. Each phenotype can
have associated metadata, such as labels. Phenotype columns can be either
floating--point or categorical (enumerated, string valued). Explicit
sort order information can be provided for categorical columns.

A cgdata **genomicMatrix** is a TSV of floating--point numbers with columns for each
sampleID and
rows for each *probe* (which can be a microarray probe, or any other
identifier, such as gene, or abstract process). Note that this orientation
is opposite that of the clinicalMatrix. The clinicalMatrix orientation is
more natural, as it corresponds to the orientation used in databases. However
the genomicMatrix orientation is critical for performance on large datasets,
where there may be hundreds of thousands of probes.

A cgdata **probeMap** is an association between an abstract name (gene, probe, etc.)
and a genomic position with list of genes. Each row is a tuple of
name, position, and gene list. Like clinicalMatrix, this orientation is
concordant with database tables.

The Xena Data Model
-------------------
Xena generalizes these formats, treating a *dataset* as tabular data having
possibly heterogeneous fields of a limited number of types (float, categorical,
position, gene list), each optionally having associated metadata. This
generalization allows loading other formats as well, such as sparse genomic
data (mutationVector), refGene, and etc.

Field types
___________

Float
+++++
A floating-point field (four bytes), such as a microarray score, DNA-VAF, age, etc.

Category
++++++++
An enumerated type, such as sampleID, reference base, tumor stage, etc. Enumerated
types can be retrieved as either a string or an integer, enabling proper sorting.

Gene
++++
A list of genes. This is a structured field, i.e. one row can have multiple genes.
For example, a particular microarray probe may be associated with several genes.

Position
++++++++
A genomic position, consisting of chromosome name, start position, end
position, and optional strand. A
UCSC bin id will be added to this field automatically.

Dataset Meta Data
_________________

probeMap
++++++++
Datasets can be associated with a probeMap. Field names
of the dataset can then be joined with the probeMap ``name`` field to find
corresponding position information. Or more usually, indexes on the
gene or position fields of a probeMap dataset can be used to find probe names of
interest, which can then be retrieved from another dataset.

cohort
++++++
Datasets can be associated with a cohort name. If a dataset has a sampleID
column, the sampleIDs are considered unique within the cohort. They are not
unique outside the cohort.

Dataset Loading
================

Files are loaded from a document root directory. The document root serves two
purposes. First, it clearly deliniates which files on the host will be ingested
and served by Xena, which is important for data security. Second, it provides
a relocateable naming system for datasets. All datasets are identified by
their path from the document root. The document root can be moved without
breaking references to datasets. This is similar to how a document root
works in a web server.

By default, the document root is ``${HOME}/xena/files``.

Xena will detect new or modified files, and reload them automatically. This
currently works for small numbers of files. For larger numbers of files this
feature must be disabled with the ``--no-auto`` flag to prevent thread depletion.
This is an implmentation flaw in the file monitoring library which
should be fixed.

When automatic file loading is disabled, files are loaded by using the command
line tool.

File hashes and time stamps are recorded when they are loaded.  Files are
skipped if their hashes match the hash from the previous load. This can
be manually overridden by using the ``--force`` flag with the ``-l`` (load) flag.

Input Formats
-------------

Supported input files include plain TSV, and cgdata TSV with metadata in json.
We should investigate other formats, such as Excel. Parsing is liberal in
what it accepts, in order to accomodate human-written input files.

TSV formats are currently limited to cgdata genomicMatrix, clinicalMatrix,
clinicalFeature, and probeMap. These formats have a fixed layout.

A more flexible mutationVector TSV format is supported, which allows a variable
layout specified by a header. This format can be generalized to handle the
cgdata formats above, as well as more general genomic formats. For example, 
given an novel TSV format, we
can load well-known columns such as chromosome, chromStart, chromEnd, and
load certain classes of unknown columns, such as floating point and categorical
values, as we do today for clinical data.
