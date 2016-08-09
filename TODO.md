# TODO

## Features, bug fixes, nitpicks etc.

Warn on validate when a description field is missing.

Allow to specify which group has access to any source.  (Goal: differentiate users of ETL, full read-only analytics groups)

Query the database for the existence of a user's schema or the current search path instead of relying on args.

Warn if distribution key is not also a sort key.

Test whether relation is table or view to invoke the correct drop command.  (When switching from VIEW to CTAS
(or back) the --drop option breaks.)

When loading data from upstream, cut off at midnight NY?

For common failure patterns in table designs, provide hints beyond error messages.

Add checks for partitions to have correct size after download (count rows while partitioning?)
