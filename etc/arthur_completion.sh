#! /bin/bash

# Look here for inspiration: https://github.com/scop/bash-completion

_arthur_completion()
{

    local cur prev opts
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD - 1]}"

    case "$prev" in
        "arthur.py")
            opts="
                --config
                --submit
                auto_design
                bootstrap_sources
                bootstrap_transformations
                check_constraints
                create_external_schemas
                create_groups
                create_index
                create_schemas
                create_users
                delete_finished_pipelines
                design
                explain
                extract
                help
                initialize
                list_tags
                list_users
                load
                ls
                ping
                promote_schemas
                query_events
                render_template
                run_query
                run_sql_template
                selftest
                settings
                show_ddl
                show_dependents
                show_downstream_dependents
                show_pipelines
                show_upstream_dependencies
                show_value
                show_vars
                summarize_events
                sync
                tail_events
                tail_logs
                terminate_sessions
                unload
                update
                update_user
                upgrade
                vacuum
                validate
                "
            # shellcheck disable=SC2207
            COMPREPLY=( $(compgen -W "$opts" -- "$cur") )
            ;;
        "-c"|"--config")
            opts=$(find -L  . -maxdepth 2 -name '*.yaml' -or -name '*.sh' | sed -e 's:^\./::')
            # shellcheck disable=SC2207
            COMPREPLY=( $(compgen -W "$opts" -d -- "$cur") )
            ;;
        "--submit"*)
            # shellcheck disable=SC2207
            COMPREPLY=( $(compgen -A variable -P '$' -- "${cur#'$'}") )
            ;;
        "auto_design"|"bootstrap_transformations")
            # shellcheck disable=SC2207
            COMPREPLY=( $(compgen -W "CTAS VIEW update check-only" -- "$cur") )
            ;;
        *)
            case "$cur" in
                "@"*)
                    # compopt -o filenames
                    # shellcheck disable=SC2207
                    COMPREPLY=( $(compgen -A file -P "@" -- "${cur#@}") )
                    ;;
                *.*)
                    # Split schemas/dw/dw-fact.sql to dw.fact (to list all tables that match).
                    opts=$(
                        find -L schemas -type f \( -name '*.yaml' -o -name '*.sql' \) 2>/dev/null |
                            sed -n -e 's:^schemas/\([^/]\{1,\}\)/\([^-]*-\)\{0,1\}\([^.]\{1,\}\)[.].*:\1.\3:p' |
                            sort -u
                    )
                    # shellcheck disable=SC2207
                    COMPREPLY=( $(compgen -W "$opts" -- "$cur") )
                    ;;
                *)
                    # Split schemas/dw/dw-fact.sql to dw (to list all schemas that match).
                    opts=$(
                        find -L schemas -type f \( -name '*.yaml' -o -name '*.sql' \) 2>/dev/null |
                            sed -n -e 's:^schemas/\([^/]\{1,\}\)/\([^-]*-\)\{0,1\}\([^.]\{1,\}\)[.].*:\1:p' |
                            sort -u
                    )
                    # compopt -o nospace
                    # shellcheck disable=SC2207
                    COMPREPLY=( $(compgen -W "$opts" -- "$cur") )
                    ;;
            esac
            ;;
    esac

}

complete -F _arthur_completion arthur.py
complete -F _arthur_completion install_extraction_pipeline.sh
complete -F _arthur_completion install_upgrade_pipeline.sh
