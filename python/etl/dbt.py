import json
import logging
import os
import re
from collections import namedtuple
from typing import Sequence

import docker

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

DbtModelIdentifier = namedtuple("DbtModelIdentifier", ["schema", "table"])

DBTRelation = namedtuple("DBTRelation", ["name", "depends_on", "type", "is_required"])


class DBTProject:
    def __init__(self, dbt_project_root, dbt_profiles_dir):
        self.dbt_root = dbt_project_root
        self.dbt_profiles_dir = dbt_profiles_dir
        self.local_dbt_path = "dbt"
        self.client = docker.from_env()
        self.tag = "arthur_dbt:latest"

    def build_image(self):
        logging.info("Building DBT image")
        img = self.client.api.build(
            path="dbt", tag=self.tag, dockerfile="Dockerfile", quiet=False, nocache=False
        )

        # Wait for the image to build
        for _ in img:
            continue
        logging.info(f"{self.tag} docker image built")

    def run_cmd(self, cmd):
        try:
            logging.info(f"Executing inside dbt container {self.tag}: $ {cmd}")
            return self.client.containers.run(
                self.tag,
                cmd,
                volumes={
                    self.dbt_root: {"bind": "/dbt", "mode": "rw"},
                    self.dbt_profiles_dir: {"bind": "/root/.dbt/profiles.yml", "mode": "ro"},
                },
                stderr=True,
                stdout=True,
            ).decode("utf-8")
        except docker.errors.ContainerError as exc:
            print(exc.container.logs())
            raise

    @staticmethod
    def get_files_in_path(path, file_types=None, prefix=""):
        for root, _, files in os.walk(path):
            for file in files:
                if (not file_types or file.split(".")[-1] in file_types) and file.startswith(prefix):
                    yield (root, file)

    def show_downstream_dbt_parents(self, dbt_model_indentifiers: Sequence[DbtModelIdentifier]):
        dbt_sql_files = self.get_files_in_path(self.local_dbt_path, file_types=("sql"))
        db_source_regex = r"db_source\(\s*'(.*)'\s*,\s*'(.*)'\s*\)"

        for model_path, sql_file_path in dbt_sql_files:
            with open(os.path.join(model_path, sql_file_path), "r") as f:
                sql_file = f.read()
            db_sources = re.findall(db_source_regex, sql_file)
            for db_source in db_sources:
                schema, table = db_source
                for dmi in dbt_model_indentifiers:
                    if dmi.schema == schema and dmi.table == table:
                        yield os.path.splitext(sql_file_path)[0]

    def parse_dbt_run_stdout(self, res):
        res_list = res.strip().split("\n")
        relations = []
        for e in res_list:
            try:
                d = json.loads(e)
            except json.decoder.JSONDecodeError:
                continue
            d["depends_on"] = [node.split(".")[-1] for node in d["depends_on"]["nodes"]]
            d["type"] = d["config"]["materialized"].upper()
            d["is_required"] = "required" in d["config"]["tags"]
            relations.append(DBTRelation(d["name"], d["depends_on"], d["type"], d["is_required"]))

        return relations

    def render_dbt_list(self, dbt_relations, with_dependencies=False, with_dependents=False):
        current_index = {relation.name: i + 1 for i, relation in enumerate(dbt_relations)}
        width_selected = max(len(name) for name in current_index)
        line_template = (
            "{relation.name:{width}s}"
            " # {relation.type} index={index:4d}"
            " flag={flag:9s}"
            " is_required={relation.is_required}"
        )

        for relation in dbt_relations:
            print(
                line_template.format(
                    flag="DBT",
                    index=current_index[relation.name],
                    relation=relation,
                    width=width_selected,
                )
            )
        return
