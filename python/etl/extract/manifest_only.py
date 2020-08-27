import logging
from typing import Dict, List

from etl.config.dw import DataWarehouseSchema
from etl.extract.extractor import Extractor
from etl.relation import RelationDescription


class ManifestOnlyExtractor(Extractor):
    """Generate manifest files for already-extracted data in S3."""

    def __init__(
        self,
        schemas: Dict[str, DataWarehouseSchema],
        relations: List[RelationDescription],
        keep_going: bool,
        dry_run: bool,
    ) -> None:
        # For static sources, we go straight to failure when the success file does not exist
        super().__init__("manifest-only", schemas, relations, keep_going, needs_to_wait=False, dry_run=dry_run)
        self.logger = logging.getLogger(__name__)

    def extract_table(self, source: DataWarehouseSchema, relation: RelationDescription):
        """Build a manifest file for the given table and write it to S3."""
        self.write_manifest_file(relation, relation.bucket_name, relation.data_directory())
