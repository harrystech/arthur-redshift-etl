import os.path

from etl.config import DataWarehouseSchema
from etl.extract.extractor import Extractor
from etl.relation import RelationDescription
from etl.thyme import Thyme


class StaticExtractor(Extractor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "static"

    def extract_table(self, source: DataWarehouseSchema, description: RelationDescription):
        """
        Render the S3 path template for a given source to check for data files before writing
        out a manifest file
        """
        rendered_template = Thyme.render_template(source.s3_path_template, {"prefix": description.prefix})
        prefix = os.path.join(rendered_template, description.csv_path_name)
        bucket = source.s3_bucket
        self.write_manifest_file(description, bucket, prefix)
