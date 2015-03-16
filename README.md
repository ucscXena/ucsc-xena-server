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

## License

Copyright © 2015 The Regents of the University of California

Distributed under the Apache License, version 2.0.
