# NEWS

This file contains user-visible changes by version in reverse chronolical sequence.

## v0.13.0 (2016-10-27)

During an update operation, arthur will no longer change view definition.
(This affects both the update sub-command of arthur as well as the etl sub-command
if invoked without the `--force` option.)
We discovered that re-creating views inside a transaction during an update
can run into a deadlock situation if there's a concurrent read on that view
from another transaction.
