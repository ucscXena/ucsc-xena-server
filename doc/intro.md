# Introduction to cavm

# Deployment scenarios

## Single user

A single user having data on their laptop, for example, can start a xena
server to allow viewing that data together with ucsc's curated data.  This
is to be a single-jar install that the user can launch by double-clicking.
The embedded h2 sql server will be used.

We should investigate install tools that can assist the user in installing
java if the laptop does not have an appropriate version.

Files in a data directory should be loaded into the database automatically,
and updated whenever the files change, so the user can see their updated
data simply by saving new versions.

Supported input files are plain TSV, or cgdata TSV with metadata in json.
We should investigate other formats, such as Excel.

## Organization

An organization can deploy a xena server behind a firewall to allow access
by many local users. This install will likely require some IT support to
bring up java and start xena from the command line, setting appropriate options
for the data directory, server certificate, etc.

This might use the h2 backend, or a larger sql backend.

## Cloud

Cloud deployment will be similar to organization deployment, with the
added possibility that UCSC will provide pre-installed images for
common cloud vendors.

# Backends

There is currently only one backend, in h2.clj. This is an embedded sql database,
primarily for use by individuals and organizations with small amounts of data.

H2 may not scale to the size of UCSC's data repository, in which case a different
sql backend should be used. Currently no others are implemented, but it should
be simple to add mysql, or other sql backends. A key issues is whether
the h2 backend will handle concurrent users effectively. It's not
designed for highly-concurrent usage, but on a high-memory server with SSD
it may be good enough for our anticipated loads. Further testing is needed.

# Data inflation

The H2 backend inflates the size of the data significantly. The reason for this
isn't clear. Much of the inflation is in the *scores* table. The *scores* table
is essentially a key-value store, so it may be worth using a different key-value
technology.

There is an experimental branch using leveldb. Leveldb uses
an unfortunate number of file descriptors, which requires OS quotas to be
modified. This will complicate deployment in other organzations, and should be
avoided. One of the dbm variety of key/value stores may be a better solution,
or debugging the h2 data inflation.

# Loading

Loading is currently done manually, by invoking xena from the command line.
By default it will load only matrix files. It will load associated cgdata
metadata if it is available. If the metadata indicates that the data is
clinical the matrix will be transposed, in accordance with the cgdata
specification.

To load probemap files the ```-p``` option must be used. This means
probemaps must be loaded separately from matrices.
**TODO** This should be automated, so xena can identify probemap files by their
metadata, or perhaps by their content.

**TODO** Loading should be automated, at least for single-user deployments, so
the user doesn't have to run xena on the command line. The loader
stores a hash and timestamp for every input file, to facilitate this
feature. Load status (success or failure) should be saved in the database
so it can be displayed to the user. A loading strategy needs to be
chosen. xena can't know when a file is ready to be loaded, or when it
is in the process of being written. We don't want to load a large dataset,
only to have it fail when the tail of the file is not ready for reading.
Alternatively, we could require user interaction before trying to reload
the data directory.

Files are skipped if their hashes match the hash from the previous load.

In order to load probemaps separately, we have to find all of them. The
following command finds all json files describing probemaps, and loads the
corresponding probemaps.

The CACHE_SIZE setting may improve load times.


    java -jar target/cavm-0.1.0-SNAPSHOT-standalone.jar -p -r /inside/grotto/craft/CAVM/ \
        -d "/data/cancer/cavm;CACHE_SIZE=65536" \
        $(find /inside/grotto/craft/CAVM -name '*.json' \
            | xargs grep -l '"type" *: *"probeMap"'  | sed -e 's/.json$//')

## CGData probemap references

The CGData specification connects related files through abstract names. For example,
if a genomic matrix metadata refers to probemap "probemapA", and a probemap
metadata includes name "probemapA", those files are connected through the probemap
relation.

This has a number of disadvantages, including being difficult for users to understand,
and being expensive for the loader to resolve: there is no algorithm for finding
the probemap for a genomic matrix, apart from scanning every probemap until a
matching name is found.

Instead, xena expects such references to refer directly to files. A genomic
matrix metadata should have probemap equal to the file name of the related probemap.

It's worth noting that this is similar to the json-ld format. We should consider
just using json-ld.

xena has a utility command, ```--json```, which will rewrite all cgdata json metadata in
a directory tree to be file references instea of references to abstract names.

TODO: write [great documentation](http://jacobian.org/writing/great-documentation/what-to-write/)
