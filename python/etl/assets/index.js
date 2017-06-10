function apiCall(path, success_handler, failure_handler) {
    var xhttp = new XMLHttpRequest();
    xhttp.onreadystatechange = function () {
        if (this.readyState === 4) {
            // console.log(path + ": " + this.statusText);
            if (this.status === 200) {
                success_handler(JSON.parse(this.responseText), this.getResponseHeader("Last-Modified"));
            } else {
                console.log("There was an error retrieving data from " + path);
                failure_handler(path);
            }
        }
    };
    xhttp.open("GET", path, true);
    xhttp.send();
}

function lostConnection(path) {
    document.getElementById("latest-error").innerHTML = "Failed to retrieve values from: " + path
}

function fetchEtlId() {
    apiCall("/api/etl-id", function (obj, ignored) {
        document.getElementById("etl-id").innerHTML = obj.id
    }, lostConnection);
}

function fetchEtlIndices() {
    apiCall("/api/indices", updateEtlIndices, lostConnection)
}

function fetchEtlEvents() {
    apiCall("/api/events", updateEtlEvents, lostConnection);
}

function updateEtlIndices(etlIndices, lastModified) {
    // Update table with the current progress meter (200px * percentage = 2 * percentage points)
    var table = "<tr><th>Name</th><th>Current Index</th><th>Final Index</th><th colspan='2'>Progress</th></tr>";
    var len = etlIndices.length;
    if (len === 0) {
        table += "<tr><td colspan='5'>(waiting...)</td></tr>";
    }
    /* else */
    for (var i = 0; i < len; i++) {
        var e = etlIndices[i];
        var percentage = (100.0 * e.current) / e.final;
        var percentageLabel = (percentage >= 10.0) ? percentage.toFixed(0) : percentage.toFixed(1);
        var indexClass;
        if (e.current === e.final) {
            indexClass = "progress complete";
        } else {
            indexClass = "progress pulsing";
        }
        table += "<tr>" +
            "<td>" + e.name + "</td>" +
            "<td>" + e.current + "</td>" +
            "<td>" + e.final + "</td>" +
            "<td>" + percentageLabel + "% </td>" +
            "<td class='" + indexClass + "'><div style='width:" + (2 * percentage).toFixed(2) + "px'></div></td>" +
            "</tr>";
    }
    document.getElementById("indices-table").innerHTML = table;
    document.getElementById("indices-table-last-modified").innerText = lastModified;
    setTimeout(fetchEtlIndices, 1000);
}

function updateEtlEvents(etlEvents, lastModified) {
    var table = "<tr>" +
        "<th>Index</th><th>Step</th><th>Target</th><th>Last Event</th><th>Timestamp</th><th>Elapsed</th>" +
        "</tr>";
    var len = etlEvents.length;
    if (len === 0) {
        table += "<tr><td colspan='6'>(waiting...)</td></tr>";
    }
    /* else */
    var now = (new Date()).valueOf();
    for (var i = 0; i < len; i++) {
        var e = etlEvents[i];
        var name = e.extra.index.name || "";
        var current = e.extra.index.current || "?";
        var timestamp = new Date(e.timestamp.replace(' ', 'T')); /* officialier ISO8601 */
        var elapsed = e.elapsed;
        var elapsedLabel;
        if (elapsed === undefined) {
            elapsed = (now - timestamp) / 1000.0;
        }
        elapsedLabel = elapsed.toFixed(1);
        var eventLabel = e.event;
        var eventClass = "event-" + eventLabel;
        if (eventLabel === "start") {
            eventLabel += "&nbsp;<div style='width: 1em'></div>";
            eventClass += " progress pulsing";
        }
        table += "<tr>" +
            "<td>" + name + " #" + current + "</td>" +
            "<td>" + e.step + "</td>" +
            "<td class='" + eventClass + "'>" + e.target + "</td>" +
            "<td class='" + eventClass + "'>" + eventLabel + "</td>" +
            "<td>" + timestamp.toISOString() + " </td>" +
            "<td class='right-aligned'> " + elapsedLabel + "s </td>" +
            "</tr>";
    }
    document.getElementById("events-table").innerHTML = table;
    document.getElementById("events-table-last-modified").innerText = lastModified;
    setTimeout(fetchEtlEvents, 1000);
}

window.onload = function () {
    fetchEtlId();
    fetchEtlIndices();
    fetchEtlEvents();
};
