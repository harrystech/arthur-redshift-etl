For files named `<number>_<USER>_<description>.sql`:
    * Run them in alpha-numeric sort order (which should sort by `number`)
    * Execute them as the `ADMIN` or `ETL` user, depending on `USER`

(And ideally, create a PR to have an Arthur step do that during `initialize`.)
