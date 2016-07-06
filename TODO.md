# TODO

## Features, bug fixes, nitpicks etc.

Change validation to go all in for views vs. tables instead of for each attribute.

Find duplicate table names in S3 bucket and warn.

Enable syncing local files with S3 (to remove files that no longer exist locally).

Allow for "description" string in the sources array of the settings file.

Allow to specify which group has access to any source.

(Goal: differentiate users of ETL, full read-only analytics groups)

Ask twice for password when password for create_user is entered on the commandline.

Query the data base for the existence of a user's schema or the current search path instead of relying on args.
