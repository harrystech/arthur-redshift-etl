import os
import re
from collections import namedtuple
from typing import Sequence
import docker

DbtModelIdentifier = namedtuple('DbtModelIdentifier', ['schema', 'table'])


class DBTProject:

    def __init__(self, dbt_project_root, dbt_profiles_dir):
        self.dbt_root = dbt_project_root
        self.dbt_profiles_dir = dbt_profiles_dir
        self.local_dbt_path = 'dbt'
        self.client = docker.from_env()
        self.tag = 'arthur_dbt:latest'

    def build_image(self):
        return self.client.api.build(
            path='dbt',
            tag=self.tag,
            dockerfile='Dockerfile',
            quiet=False,
            # nocache=False
        )

    def run_cmd(self, cmd):
        try:
            return self.client.containers.run(self.tag,
                                              cmd,
                                              volumes={self.dbt_root: {'bind': '/dbt', 'mode': 'rw'},
                                                       self.dbt_profiles_dir: {'bind': '/root/.dbt/profiles.yml',
                                                                               'mode': 'ro'}},
                                              stderr=True,
                                              stdout=True,
                                              ).decode("utf-8")
        except docker.errors.ContainerError as exc:
            print(exc.container.logs())
            raise

    @staticmethod
    def get_files_in_path(path, file_types=None, prefix=""):
        for root, dirs, files in os.walk(path):
            for file in files:
                if (not file_types or file.split('.')[-1] in file_types) and file.startswith(prefix):
                    yield (root, file)

    def show_downstream_dbt_parents(self, dbt_model_indentifiers: Sequence[DbtModelIdentifier]):
        dbt_sql_files = self.get_files_in_path(self.local_dbt_path, file_types=('sql'))
        db_source_regex = r"db_source\(\s*'(.*)'\s*,\s*'(.*)'\s*\)"

        for model_path, sql_file_path in dbt_sql_files:
            # print(model_path, sql_file_path)
            with open(os.path.join(model_path, sql_file_path), "r") as f:
                sql_file = f.read()
            db_sources = re.findall(db_source_regex, sql_file)
            # print(db_sources)
            for db_source in db_sources:
                schema, table = db_source
                for dmi in dbt_model_indentifiers:
                    if dmi.schema == schema and dmi.table == table:
                        yield sql_file_path.rstrip('.sql')




