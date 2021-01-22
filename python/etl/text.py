"""
Deal with text, mostly around iterables of texts which we want to pretty print.

Should not import Arthur modules (so that etl.errors remains widely importable)
"""

import textwrap
from typing import List


def approx_pretty_size(total_bytes) -> str:
    """
    Return a humane and pretty size approximation.

    This looks silly bellow 1KB but I'm OK with that.
    Don't call this with negative total_bytes or your pet hamster will go bald.

    >>> approx_pretty_size(50)
    '1KB'
    >>> approx_pretty_size(2000)
    '2KB'
    >>> approx_pretty_size(2048)
    '2KB'
    >>> approx_pretty_size(3000000)
    '3MB'
    >>> approx_pretty_size(4000000000)
    '4GB'
    >>> approx_pretty_size(-314)
    Traceback (most recent call last):
        ...
    ValueError: total_bytes may not be negative
    """
    if total_bytes < 0:
        raise ValueError("total_bytes may not be negative")
    for scale, unit in ((1024 * 1024 * 1024, "GB"), (1024 * 1024, "MB"), (1024, "KB")):
        div, rem = divmod(total_bytes, scale)
        if div > 0:
            if rem > 0:
                div += 1  # always round up
            break
    else:
        div, unit = 1, "KB"
    return "{:d}{}".format(div, unit)


def join_with_quotes(names):
    """
    Individually wrap the names in quotes and return comma-separated names in a string.

    If the input is a set of names, the names are sorted first.
    If the input is a list of names, the order of the list is respected.
    If the input is cheese, the order is for more red wine.

    >>> join_with_quotes(["foo", "bar"])
    "'foo', 'bar'"
    >>> join_with_quotes({"foo", "bar"})
    "'bar', 'foo'"
    >>> join_with_quotes(frozenset(["foo", "bar"]))
    "'bar', 'foo'"
    """
    if isinstance(names, (set, frozenset)):
        return ", ".join("'{}'".format(name) for name in sorted(names))
    else:
        return ", ".join("'{}'".format(name) for name in names)


def join_column_list(columns: List[str], sep=", ") -> str:
    """Return string with comma-separated, delimited column names."""
    return sep.join('"{}"'.format(column) for column in columns)


def whitespace_cleanup(value: str) -> str:
    """Return the string value with (per line) leading and any trailing whitespace removed."""
    return textwrap.dedent(value).strip("\n")


class ColumnWrapper(textwrap.TextWrapper):
    """
    Unlike the TextWrapper, we don't care about words and just treat the entire column as a chunk.

    If the column is "too long", then we force a "word break" with whitespace.
    """

    def _split(self, text):
        """
        Create eitehr one chunk that fits or three chunks with the last wider than the placeholder.

        >>> cw = ColumnWrapper(width=10, placeholder='..')
        >>> cw._split("ciao")
        ['ciao']
        >>> cw._split("good morning")
        ['good mo', ' ', '???']
        """
        chunk = text.rstrip()
        if len(chunk) > self.width:
            return [chunk[: self.width - len(self.placeholder) - 1], " ", "?" * (len(self.placeholder) + 1)]
        else:
            return [chunk]


def format_lines(value_rows, header_row=None, has_header=False, max_column_width=100) -> str:
    """
    Format a list of rows which each have a list of values, optionally with a header.

    We process whitespace by expanding tabs and then replacing all whitespace but actual spaces.
    If any column is longer than the max_column_width, a placeholder will be inserted after cutting
    the value.

    >>> print(format_lines([["aa", "b", "ccc"], ["a", "bb", "c"]]))
    col #1 | col #2 | col #3
    -------+--------+-------
    aa     | b      | ccc
    a      | bb     | c
    (2 rows)
    >>> print(format_lines(
    ...     [["name", "breed"], ["monty", "spaniel"], ["cody", "poodle"], ["cooper", "shepherd"]],
    ...     has_header=True))
    name   | breed
    -------+---------
    monty  | spaniel
    cody   | poodle
    cooper | shepherd
    (3 rows)
    >>> print(format_lines([["windy"]], header_row=["weather"]))
    weather
    -------
    windy
    (1 row)
    >>> print(format_lines([]))
    (0 rows)
    >>> print(format_lines([], header_row=["nothing"]))
    nothing
    -------
    (0 rows)
    >>> format_lines([["a", "b"], ["c"]])
    Traceback (most recent call last):
    ValueError: unexpected row length: got 1, expected 2
    """
    if header_row is not None and has_header is True:
        raise ValueError("cannot have separate header row and mark first row as header")
    # Make sure that we are working with a list of lists of strings (and not generators and such).
    wrapper = ColumnWrapper(
        width=max_column_width,
        max_lines=1,
        placeholder="...",
        expand_tabs=True,
        replace_whitespace=True,
        drop_whitespace=False,
    )
    matrix = [[wrapper.fill(str(column)) for column in row] for row in value_rows]
    # Add header row as needed, have number of columns depend on header
    if header_row is not None:
        n_columns = len(header_row)
        matrix.insert(0, [str(column) for column in header_row])
    elif has_header:
        n_columns = len(matrix[0])
    else:
        n_columns = len(matrix[0]) if len(matrix) > 0 else 0
        matrix.insert(0, ["col #{:d}".format(i + 1) for i in range(n_columns)])
    assert len(matrix) > 0
    for i, row in enumerate(matrix):
        if len(row) != n_columns:
            raise ValueError("unexpected row length: got {:d}, expected {:d}".format(len(row), n_columns))
    row_count = "({:d} {})".format(len(matrix) - 1, "row" if len(matrix) == 2 else "rows")
    # Determine column widths, add separator between header and body as dashed line
    column_width = [max(len(row[i]) for row in matrix) for i in range(n_columns)]
    if max_column_width is not None:
        column_width = [min(actual_width, max_column_width) for actual_width in column_width]
    matrix.insert(1, ["-" * width for width in column_width])
    # Now rewrite the matrix values to fill the columns
    if header_row or len(matrix) > 2:
        matrix = [["{:{}s}".format(row[i], column_width[i]) for i in range(n_columns)] for row in matrix]
        rows = [" | ".join(row).rstrip() for row in matrix]
        rows[1] = rows[1].replace(" | ", "-+-")
        rows.append(row_count)
    else:
        rows = [row_count]  # without data, don't even try to print the column headers, like "col #1".
    return "\n".join(rows)
