_schemas_completion()
{
    local SCHEMA_START SCHEMAS
    SCHEMA_START="${COMP_WORDS[COMP_CWORD]}"
    SCHEMAS=$(find schemas -name '*.yaml' | sed -e 's:schemas/\([^/]*\)/[^-]*-\([^.]*\).yaml:\1.\2:')
    COMPREPLY=( $(compgen -W "$SCHEMAS" -- "$SCHEMA_START") )
}

for script in "copy_to_s3.py dump_to_s3.py load_to_redshift.py update_from_ctas.py update_views.py"
do
    complete -F _schemas_completion $script
done
