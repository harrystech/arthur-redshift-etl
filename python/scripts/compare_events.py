"""
This script compares events from two ETLs to highlight differences in elapsed times or row counts.

* Pre-requisites

You need to have a list of events for each ETL. Arthur can provide this using the
"query_events" command.

For example:
```
arthur.py query_events -p development 37ACEC7440AB4620 -q > 37ACEC7440AB4620.events
arthur.py query_events -p development 96BE11B234F84F39 -q > 96BE11B234F84F39.events
```

* Usage

Once you have the files, you use this script:
```
compare_events.py 37ACEC7440AB4620.events 96BE11B234F84F39.events
```

The order of those two files is: "older ETL" => "newer ETL".
"""

import csv
import re
import sys
from collections import defaultdict, namedtuple
from math import isclose

from tabulate import tabulate


def read_file(filename):
    """
    Read output from query_events command.

    The file is expected to be formatted such that there's a header line, a separator, then the
    data. The column set must contain "elapsed" and "rowcount" for later processing.
    Also Arthur prints a summary after the table, like "(100 rows)" which will be skipped if present.
    """
    column_spacing_re = re.compile(r"\s+\|\s+")
    row_count_re = re.compile(r"\(\d+\s*rows\)")

    print(f"Reading events from {filename}...")
    with open(filename) as f:
        for i, line in enumerate(f.readlines()):
            if i == 1 or row_count_re.match(line):
                # Found the separator line or the final row tally.
                continue
            yield column_spacing_re.sub("|", line).strip()


def parse_file(filename):
    """Parse the input as '|'-delimited columns."""
    lines = read_file(filename)
    reader = csv.reader(lines, delimiter="|")
    row_class = namedtuple("CsvRow", next(reader), rename=True)
    for row in reader:
        yield row_class._make(row)


def extract_values(filename):
    """Find elapsed time and rowcount for each target relation."""
    # The "lambda: None" trick allows us to use 'd[]' instead of 'd.get()' later.
    elapsed = defaultdict(lambda: None)
    rowcount = defaultdict(lambda: None)
    for row in parse_file(filename):
        elapsed[row.step, row.target] = float(row.elapsed) if row.elapsed != "---" else None
        rowcount[row.step, row.target] = int(row.rowcount) if row.rowcount != "---" else None
    return elapsed, rowcount


def delta(a, b):
    """
    Return change in percent (or None if undefined).

    The delta in percent is rounded to one decimal.
    """
    if a is None or b is None:
        return None
    if a == 0.0 and b == 0.0:
        return 0.0
    assert a != 0.0 and b != 0.0
    return round((b - a) * 1000.0 / a) / 10.0


def show_delta(previous_value, current_value, column):
    """
    Return whether the change from previous event to current event is "significant".

    If the values appear to be equal or almost equal, there's no need to report a delta.
    Also, if the values are really small and any change is inflated, skip reporting the delta.
    Note that for row count, a decrease in rows is always shown.
    """
    if previous_value is None or current_value is None:
        return False
    if previous_value == current_value:
        return False

    if column == "elapsed":
        # Decrease trigger-happiness for quick loads:
        if previous_value < 10.0 and current_value < 10.0:
            return False
        if previous_value < 30.0 or current_value < 30.0:
            return not isclose(previous_value, current_value, abs_tol=20.0)
        if previous_value < 60.0 or current_value < 60.0:
            return not isclose(previous_value, current_value, rel_tol=0.5)
        if previous_value < 300.0 or current_value < 300.0:
            return not isclose(previous_value, current_value, rel_tol=0.2)

    if column == "rowcount":
        # We expect to move forward with growing tables so smaller row counts are suspect.
        if previous_value > current_value:
            return True
        # Increase trigger-happiness for small (dimensional) tables:
        if previous_value < 1000 or current_value < 1000:
            return not isclose(previous_value, current_value, abs_tol=10)

    return not isclose(previous_value, current_value, rel_tol=0.1)


def print_comparison_table(previous_values, current_values, column):
    """Print differences between runs, sorted by relation."""
    all_events = frozenset(previous_values).union(current_values)
    has_large_diff = frozenset(
        event for event in all_events if show_delta(previous_values[event], current_values[event], column)
    )
    table = sorted(
        [
            (
                event[1],  # target
                event[0],  # step
                previous_values[event],
                current_values[event],
                delta(previous_values[event], current_values[event]),
            )
            for event in has_large_diff
        ],
        key=lambda row: row[:2],  # Avoid comparison with None values in the columns
    )
    print("Differences for '{}':\n".format(column))
    print(
        tabulate(
            table,
            headers=("target", "step", "prev. " + column, "cur. " + column, "delta %"),
            tablefmt="presto",
        )
    )


def main():
    if len(sys.argv) >= 2 and sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)
    if len(sys.argv) != 3:
        print(
            "Usage: {prog} previous_events current_events".format(prog=sys.argv[0]),
            file=sys.stderr,
        )
        sys.exit(1)

    previous_events_file, current_events_file = sys.argv[1:3]
    previous_elapsed, previous_rowcount = extract_values(previous_events_file)
    current_elapsed, current_rowcount = extract_values(current_events_file)

    print_comparison_table(previous_elapsed, current_elapsed, "elapsed")
    print()
    print_comparison_table(previous_rowcount, current_rowcount, "rowcount")


if __name__ == "__main__":
    main()
