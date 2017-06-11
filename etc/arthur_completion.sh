#! /bin/bash

# Look here for inspiration: https://github.com/scop/bash-completion

_arthur_completion()
{

    local cur prev opts
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD - 1]}"

    case "$prev" in
        "arthur.py")
            opts="initialize create_user
                  design auto_design bootstrap_sources bootstrap_transformations sync
                  extract load upgrade update unload
                  create_schemas restore_schemas validate explain ls ping
                  show_dependents show_dependency_chain show_pipelines selftest self-test
                  --submit --config"
            COMPREPLY=( $(compgen -W "$opts" -- "$cur") )
            ;;
        "-c"|"--config")
            opts=$(find -L  . -maxdepth 2 -name '*.yaml' -or -name '*.sh' | sed -e 's:^\./::')
            COMPREPLY=( $(compgen -W "$opts" -d -- "$cur") )
            ;;
        "--submit"*)
            COMPREPLY=( $(compgen -A variable -P '$' -- "${cur#'$'}") )
            ;;
        "auto_design"|"bootstrap_transformations")
            COMPREPLY=( $(compgen -W "CTAS VIEW" -- "$cur") )
            ;;
        "selftest"|"self-test")
            COMPREPLY=( $(compgen -W "all doctest type-check" -- "$cur") )
            ;;
        *)
            case "$cur" in
                "@"*)
                    # compopt -o filenames
                    COMPREPLY=( $(compgen -A file -P "@" -- "${cur#@}") )
                    ;;
                *.*)
                    opts=$(find -L schemas -type f \( -name '*.yaml' -o -name '*.sql' \) 2>/dev/null |
                           sed -n -e 's:schemas/\([^/]*\)/[^-]*-\([^.]*\)\..*:\1.\2:p' | uniq)
                    COMPREPLY=( $(compgen -W "$opts" -- "$cur") )
                    ;;
                *)
                    opts=$(find -L schemas -type f \( -name '*.yaml' -o -name '*.sql' \) 2>/dev/null |
                           sed -n -e 's:schemas/\([^/]*\)/[^-]*-\([^.]*\)\..*:\1:p' | uniq)
                    # compopt -o nospace
                    COMPREPLY=( $(compgen -W "$opts" -- "$cur") )
                    ;;
            esac
            ;;
    esac

} &&
complete -F _arthur_completion arthur.py
