/*jslint browser:true */
/*globals CodeMirror: false */
(function () {
	var results,
		keys = {
			"Ctrl-Enter": function (cm) {
				cm.execCommand('xena_run');
			}
		};

	function escapeHTML(str) {
		return str.replace(/[&"<>]/g, function (m) {
			return escapeHTML.replacements[m];
		});
	}

	escapeHTML.replacements = {
		"&": "&amp;",
		'"': "&quot;",
		"<": "&lt;",
		">": "&gt;"
	};

	function makeRequest(url, content, success, error) {
		var httpRequest = new XMLHttpRequest();

		httpRequest.onreadystatechange = function () {
			if (httpRequest.readyState === 4) {
				if (httpRequest.status === 200) {
					if (success) {
						success(httpRequest.responseText);
					}
				} else {
					console.log(httpRequest);
					if (error) {
						error(httpRequest.responseText);
					}
				}
			}
		};

		httpRequest.open('POST', url, true);
		httpRequest.setRequestHeader("Content-type", "text/plain");
		httpRequest.send(content);
	}

	function main() {
		var el = document.getElementById("editor"),
			cm = CodeMirror(el, {
					vimMode: true,
					matchBrackets: false,
					autoCloseBrackets: true
			});

		cm.addKeyMap(keys);
		results = document.getElementById("results");
	}

	function disp_whitespace(str) {
		return str.replace(/ /g, '&nbsp')
			.replace(/\n/g, '<br>');
	}

	CodeMirror.commands.xena_run = function (e) {
		var query = e.getValue();
		makeRequest("data/", query,
			function (r) {
				var pr;
				try {
					pr = disp_whitespace(JSON.stringify(JSON.parse(r), null, 4));
				} catch (e) {
					pr = "Unable to parse:<br>" + e + "<br>" + escapeHTML(r);
				}
				results.innerHTML = pr;
			},
			function (r) {
				console.log(results);
				results.innerHTML = r;
			}
	   );
	};

	document.onreadystatechange = function () {
		if (document.readyState === "complete") {
			main();
		}
	};
}());
