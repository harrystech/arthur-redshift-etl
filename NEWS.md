# NEWS

This file contains user-visible changes by version in reverse chronological sequence.

## v0.14.? (2017-01-05)

* Add `show_dependencies` and `show_pipelines` commands

## v0.14.0 (2016-12-20)

* Add `unload` command to export tables into CSV files.

## v0.13.2 (2016-11-09)

* Remove comparison dump from nightly data pipeline

## v0.13.1 (2016-11-03)

* Add script for one-off EC2 instances
* Revise ping to cronut at top and bottom of ETL
* Change tag of resources to use `DataWarehouseEnvironment` (as much as possible)

## v0.13.0 (2016-10-27)

During an update operation, arthur will no longer change view definition.
(This affects both the update sub-command of arthur as well as the etl sub-command
if invoked without the `--force` option.)
We discovered that re-creating views inside a transaction during an update
can run into a deadlock situation if there's a concurrent read on that view
from another transaction.
