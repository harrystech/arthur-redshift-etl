_schemas_completion()
{
    local SCHEMA_START SCHEMAS
    SCHEMA_START="${COMP_WORDS[COMP_CWORD]}"
    case "$SCHEMA_START" in
        *.*)
            SCHEMAS=$(find schemas -type f -name '*.yaml' | sed -e 's:schemas/\([^/]*\)/[^-]*-\([^.]*\).yaml:\1.\2:')
            ;;
        *)
            SCHEMAS=$(find schemas -type f -name '*.yaml' | sed -e 's:schemas/\([^/]*\)/[^-]*-\([^.]*\).yaml:\1:' | uniq)
            ;;
    esac
    COMPREPLY=( $(compgen -W "$SCHEMAS" -- "$SCHEMA_START") )
}

for script in "copy_to_s3.py dump_to_s3.py load_to_redshift.py update_in_redshift.py"
do
    complete -F _schemas_completion $script
done
