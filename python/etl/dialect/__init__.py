from typing import List

# If we ever support more than one "dialect" of SQL, we'd have to pick the correct one here.
from etl.dialect.redshift import build_table_ddl, build_view_ddl
from etl.relation import RelationDescription


def show_ddl(relations: List[RelationDescription]) -> None:
    """Print to stdout the DDL used to create the corresponding tables or views."""
    for i, relation in enumerate(relations):
        if i > 0:
            print()
        if relation.is_view_relation:
            ddl_stmt = build_view_ddl(
                relation.target_table_name, relation.unquoted_columns, relation.query_stmt
            )
        else:
            ddl_stmt = build_table_ddl(relation.target_table_name, relation.table_design)
        print(ddl_stmt + "\n;")
