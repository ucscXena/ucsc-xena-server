Xena Web Console
****************

A web console based on codemirror (http://codemirror.net/) is
provided for trying and debugging xena queries. The console
is available at url path ``/console.html``.

For example, if running xena on your laptop, the console is ::

    http://localhost:7222/console.html

By default the console behaves like vim. Clicking on the text
box and pressing ``i`` will enter *insert* mode, and allow
entering a xena query.

The text of the query is sent to the server whenever ``ctrl-return``
is pressed. Results (or stack trace on error) are displayed below
the text box.

Codemirror has support for other input modes (e.g. emacs), which
can be enabled in the future.

The console on genome-cancer is currently not functional due to
url path mangling that occurs during reverse-proxy. This will
be fixed.
