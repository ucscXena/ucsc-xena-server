Command Line Tool
*****************

Overview
========
The command line tool is invoked via the Xena jar file. This is typical of
jvm applications. ::

    java -jar xena.jar -h

When executed with no options the Xena web server will launch, listening on
localhost (allowing only local connections). The document
root will be monitored, and new data will be loaded automatically. Double-clicking
the jar file is the same as launching with no options. 

``--no-serve``
--------------
Disable the web server. Files will still be loaded automatically if
``--no-auto`` is not specified.

``--no-auto``
-------------
Disable the automatic file loading.

If both ``--no-serve`` and ``--no-auto`` are specified, Xena will immediately exit.

``--host``
----------
Set the IP for the listening HTTP socket. E.g. ``0.0.0.0`` will permit
connections from external hosts. Defaults to ``localhost``.

``--port``
----------
Set the port for the listening HTTP socket. Defaults to 7222.

``--load [file ..]``
--------------------
Load the given files into an already-running Xena server. This works by
making a localhost connection to the running Xena server. Currently Xena
accepts load commands only from localhost, but there is no other security in-place.
This will be problematic on shared servers where access should be limited to
a subset of users. A secure administrative socket should be used, instead.

``--force``
-----------
In conjunction with ``--load``, load the files even if their hashes are unchanged.

``--json``
----------
Convert all cgdata ``.json`` files in the document root so they are compatible
with Xena.

``--database``
--------------
Set the database file path. Defaults to ``${HOME}/xena/database``.

``--logfile``
-------------
Set the log file path. Defaults to ``${HOME}/xena/xena.log``.

``--root``
----------
Set the document root. Defaults to ``${HOME}/xena/files``.
