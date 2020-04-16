# UCSC Xena Server

Work in progress.

Turn-key data server for functional genomics visualization.

View private genomic data on your laptop or institutional server, together with public data sets, without moving your data off-site, via UCSC cancer genomics web tools.

## Installation

Download from https://genome-cancer.ucsc.edu/download/public/get-xena/index.html.

## Usage

### With installer
Double-click, or select from the application menu. This will start UCSC Xena, and present a UI for importing local data files.

### Jar (server install)

    $ java -jar cavm-0.10.0-standalone.jar [args]

If a display is available, the file import UI will be opened. Otherwise it will run headless. File imports are queued by running the jar again, with the -l (load) option.

## Options

A full list of options can be displayed with

    $ java -jar cavm-0.10.0-standalone.jar --help

## Development

Run `lein run` to install dependencies and run the server.  Make sure to
have at least leiningen 2.4.1 available, otherwise the build process
will fail.  `lein run -- --help` will explain the additional options.

The server will prompt you to upload some sample data; there're some
example files linked in the help file (click "Help" in the UCSC Xena
Client interface and scroll down to "Installing a local Xena Hub to view
your data from your laptop").

## Build jar from source

lein needs to be installed: https://leiningen.org/

run "lein uberjar"

## Acknowledgements

UCSC Xena uses the YourKit profiler.

<a href="https://www.yourkit.com/java/profiler/"><img src="https://www.yourkit.com/images/yklogo.png"></a>

YourKit supports open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/">YourKit Java Profiler</a>
and <a href="https://www.yourkit.com/.net/profiler/">YourKit .NET Profiler</a>,
innovative and intelligent tools for profiling Java and .NET applications.

## License

Copyright © 2015 The Regents of the University of California

Distributed under the Apache License, version 2.0.
