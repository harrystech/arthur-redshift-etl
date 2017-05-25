function apiCall(path, handler) {
    var xhttp = new XMLHttpRequest();
    xhttp.onreadystatechange = function () {
        if (this.readyState === 4) {
            // console.log(path + ": " + this.statusText);
            if (this.status === 200) {
                handler(JSON.parse(this.responseText), this.getResponseHeader("Last-Modified"));
            } else {
                console.log("There was an error retrieving data from " + path);
            }
        }
    };
    xhttp.open("GET", path, true);
    xhttp.send();
}

function fetchEtlId() {
    apiCall("/api/etl-id", function setEtlId(obj, ignored) {
        document.getElementById("etl-id").innerHTML = obj.id
    });
}

function fetchEtlIndices() {
    apiCall("/api/indices", updateEtlIndices);
}

function updateEtlIndices(etlIndices, lastModified) {
    // Update table with the current progress meter (200px * percentage = 2 * percentage points)
    var table = "<tr><th>Name</th><th>Current Index</th><th>Final Index</th><th colspan='2'>Progress</th></tr>";
    var len = etlIndices.length;
    var done = 0;
    if (len === 0) {
       table += "<tr><td colspan='5'>(waiting...)</td></tr>";
    } /* else */
    for (var i = 0; i < len; i++) {
        var e = etlIndices[i];
        if (e.current === e.final) {
            done += 1
        }
        var percentage = (100.0 * e.current) / e.final;
        var percentageLabel = (percentage > 10.0) ? percentage.toFixed(0) : percentage.toFixed(1);
        table += "<tr>" +
            "<td>" + e.name + "</td>" +
            "<td>" + e.current + "</td>" +
            "<td>" + e.final + "</td>" +
            "<td>" + percentageLabel + "% </td>" +
            "<td class='progress'><div style='width:" + (2 * percentage).toFixed(2) + "px'></div></td>" +
            "</tr>";
    }
    document.getElementById("indices-table").innerHTML = table;
    document.getElementById("indices-table-last-modified").innerText = lastModified;
    if (done < 1 || done < len) {
        setTimeout(fetchEtlIndices, 1000);
    }
}

function fetchEtlEvents() {
    apiCall("/api/events", updateEtlEvents);
}

function updateEtlEvents(etlEvents, lastModified) {
    var table = "<tr>" +
        "<th>Index</th><th>Target</th><th>Step</th><th>Last Event</th><th>Timestamp</th><th>Elapsed</th>" +
        "</tr>";
    var len = etlEvents.length;
    if (len === 0) {
        table += "<tr><td colspan='6'>(waiting...)</td></tr>";
    } /* else */
    for (var i = 0; i < len; i++) {
        var e = etlEvents[i];
        var name = e.extra.index.name || "";
        var current = e.extra.index.current || "?";
        var elapsed = e.elapsed || 0.0; // should be: timestamp - now
        var elapsedLabel = (elapsed > 10.0) ? elapsed.toFixed(1) : elapsed.toFixed(2);
        var eventClass = "event-" + e.event;
        table += "<tr>" +
            "<td>" + name + " #" + current + "</td>" +
            "<td class='" + eventClass + "'>" + e.target + "</td>" +
            "<td>" + e.step + "</td>" +
            "<td class='" + eventClass + "'>" + e.event + "</td>" +
            "<td>" + e.timestamp.substring(0, 19) + " UTC </td>" +
            "<td class='" + eventClass + "'>" + elapsedLabel + "s </td>" +
            "</tr>";
    }
    document.getElementById("events-table").innerHTML = table;
    document.getElementById("events-table-last-modified").innerText = lastModified;
    setTimeout(fetchEtlEvents, 1000);
}

window.onload = function () {
    fetchEtlId();
    fetchEtlEvents();
    fetchEtlIndices();
};
