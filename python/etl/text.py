"""
Deal with text, mostly around matrices of texts which we want to pretty print.
"""

import textwrap


def format_lines(value_rows, header_row=None, has_header=False, max_column_width=100) -> str:
    """
    Format a list of rows which each have a list of values, optionally with a header.

    We process whitespace by expanding tabs and then replacing all whitespace but actual spaces.
    If any column is longer than the max_column_width, a placeholder will be inserted after cutting the value.

    >>> print(format_lines([["aa", "b", "ccc"], ["a", "bb", "c"]]))
    #1 | #2 | #3
    ---+----+----
    aa | b  | ccc
    a  | bb | c
    (2 rows)
    >>> print(format_lines([["name", "breed"], ["monty", "spaniel"], ["cody", "poodle"], ["cooper", "shepherd"]],
    ...                    has_header=True))
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
    >>> format_lines([["a", "b"], ["c"]])
    Traceback (most recent call last):
    ValueError: unexpected row length: got 1, expected 2
    """
    if header_row is not None and has_header is True:
        raise ValueError("cannot have separate header row and mark first row as header")
    # Make sure that we are working with a list of lists of strings (and not generators and such).
    wrapper = textwrap.TextWrapper(width=max_column_width, max_lines=1, placeholder="...",
                                   expand_tabs=True, replace_whitespace=True, drop_whitespace=False)
    matrix = [[wrapper.fill(str(column)) for column in row] for row in value_rows]
    n_columns = len(matrix[0]) if len(matrix) > 0 else 0
    # Add header row as needed
    if header_row is not None:
        matrix.insert(0, [str(column) for column in header_row])
    elif not has_header:
        matrix.insert(0, ["#{:d}".format(i + 1) for i in range(n_columns)])
    for i, row in enumerate(matrix):
        if len(row) != n_columns:
            raise ValueError("unexpected row length: got {:d}, expected {:d}".format(len(row), n_columns))
    if len(matrix) == 1:
        return "(0 rows)"
    final = "\n({:d} {})".format(len(matrix) - 1, "row" if len(matrix) == 2 else "rows")
    # Determine column widths, add separator between header and body as dashed line
    column_width = [max(len(row[i]) for row in matrix) for i in range(n_columns)]
    if max_column_width is not None:
        column_width = [min(actual_width, max_column_width) for actual_width in column_width]
    matrix.insert(1, ["-" * width for width in column_width])
    # Now rewrite the matrix values to fill the columns
    matrix = [["{:{}s}".format(row[i], column_width[i]) for i in range(n_columns)] for row in matrix]
    rows = [' | '.join(row).rstrip() for row in matrix]
    rows[1] = rows[1].replace(' | ', '-+-')
    return '\n'.join(rows) + final
