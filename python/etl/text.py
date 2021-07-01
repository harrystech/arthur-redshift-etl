"""
Deal with text, mostly around iterables of texts which we want to pretty print.

Should not import other Arthur modules so that etl.text remains widely importable.
"""

import textwrap
from typing import Iterable

from tabulate import tabulate


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
    for scale, _unit in ((1024 * 1024 * 1024, "GB"), (1024 * 1024, "MB"), (1024, "KB")):
        div, rem = divmod(total_bytes, scale)
        if div > 0:
            if rem > 0:
                div += 1  # always round up
            break
    else:
        div, _unit = 1, "KB"
    return f"{div:d}{_unit}"


def join_with_single_quotes(names: Iterable[str]) -> str:
    """
    Individually wrap the names in quotes and return comma-separated names in a string.

    If the input is a set of names, the names are sorted first.
    If the input is a list of names, the order of the list is respected.
    If the input is cheese, the order is for more red wine.

    >>> join_with_single_quotes(["foo", "bar"])
    "'foo', 'bar'"
    >>> join_with_single_quotes({"foo", "bar"})
    "'bar', 'foo'"
    >>> join_with_single_quotes(frozenset(["foo", "bar"]))
    "'bar', 'foo'"
    """
    if isinstance(names, (set, frozenset)):
        return ", ".join("'{}'".format(name) for name in sorted(names))
    return ", ".join("'{}'".format(name) for name in names)


def join_with_double_quotes(names: Iterable[str], sep=", ", prefix="") -> str:
    """
    Return string with comma-separated, delimited names.

    This step ensures that our identifiers are wrapped in double quotes.
    >>> join_with_double_quotes(["foo", "bar"])
    '"foo", "bar"'
    >>> join_with_double_quotes(["foo", "bar"], sep=", USER ", prefix="USER ")
    'USER "foo", USER "bar"'
    """
    return prefix + sep.join('"{}"'.format(name) for name in names)


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
        Create either one chunk that fits or three chunks with the last wider than the placeholder.

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


def format_lines(
    value_rows, header_row=None, has_header=False, max_column_width=100, skip_rows_count=False, tablefmt="presto"
) -> str:
    """
    Format a list of rows which each have a list of values, optionally with a header.

    We process whitespace by expanding tabs and then replacing all whitespace but actual spaces.
    If any column is longer than the max_column_width, a placeholder will be inserted after cutting
    the value.

    >>> print(format_lines([["aa", "b", "ccc"], ["a", "bb", "c"]]))
     col #1   | col #2   | col #3
    ----------+----------+----------
     aa       | b        | ccc
     a        | bb       | c
    (2 rows)
    >>> print(format_lines(
    ...     [["name", "breed"], ["monty", "spaniel"], ["cody", "poodle"], ["cooper", "shepherd"]],
    ...     has_header=True))
     name   | breed
    --------+----------
     monty  | spaniel
     cody   | poodle
     cooper | shepherd
    (3 rows)
    >>> print(format_lines([["windy"]], header_row=["weather"]))
     weather
    -----------
     windy
    (1 row)
    >>> print(format_lines([]))
    (0 rows)
    >>> print(format_lines([], header_row=["nothing"]))
     nothing
    -----------
    (0 rows)
    >>> format_lines([["a", "b"], ["c"]])
    Traceback (most recent call last):
    ValueError: unexpected length of row 2: got 1, expected 2
    """
    if header_row is not None and has_header is True:
        raise ValueError("cannot have separate header row and mark first row as header")

    # Rewrite input in case "value_rows" has some generator magic or columns have spaces or tabs.
    wrapper = ColumnWrapper(
        width=max_column_width,
        max_lines=1,
        placeholder="...",
        expand_tabs=True,
        replace_whitespace=True,
        drop_whitespace=False,
    )
    matrix = [[wrapper.fill(column) if isinstance(column, str) else column for column in row] for row in value_rows]

    if has_header:
        row_count = len(matrix) - 1
        column_count = len(matrix[0])
    elif header_row is not None:
        row_count = len(matrix)
        column_count = len(header_row)
    else:
        row_count = len(matrix)
        column_count = len(matrix[0]) if row_count else 0
        header_row = ["col #{:d}".format(i + 1) for i in range(column_count)]
    # Quick check whether we built the matrix correctly.
    for i, row in enumerate(matrix):
        if len(row) != column_count:
            raise ValueError(f"unexpected length of row {i + 1}: got {len(row)}, expected {column_count}")

    if row_count == 0 and not (has_header or header_row):
        formatted = ""
    elif has_header:
        formatted = tabulate(matrix, headers="firstrow", tablefmt=tablefmt) + "\n"
    else:
        formatted = tabulate(matrix, headers=header_row, tablefmt=tablefmt) + "\n"
    if skip_rows_count:
        return formatted.rstrip("\n")
    return formatted + "({:d} {})".format(row_count, "row" if row_count == 1 else "rows")
