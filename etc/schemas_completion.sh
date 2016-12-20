#! /bin/bash

_schemas_completion()
{

    local cur prev opts
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    # All sub-commands
    opts="ls initialize create_user design sync dump load unload update etl validate explain ping"

    if [ "$prev" = "arthur.py" ]; then
        COMPREPLY=( $(compgen -W "$opts" -- "$cur") )
    elif [ ! -d schemas ]; then
        COMPREPLY=( )
    else
        local SCHEMAS
        case "$cur" in
          *.*)
            SCHEMAS=$(find -L schemas -type f -name '*.yaml' | sed -e 's:schemas/\([^/]*\)/[^-]*-\([^.]*\).yaml:\1.\2:')
            ;;
          *)
            SCHEMAS=$(find -L schemas -type f -name '*.yaml' | sed -e 's:schemas/\([^/]*\)/[^-]*-\([^.]*\).yaml:\1:' | uniq)
            ;;
        esac
        COMPREPLY=( $(compgen -W "$SCHEMAS" -- "$cur") )
    fi

}

complete -F _schemas_completion arthur.py
