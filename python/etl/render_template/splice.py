import logging

import yaml

import etl.config
import etl.render_template


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def splice_pipeline(base_pipeline_template, new_template, insertion_point='PingCronutAfterBootstrap', replace=False,
                    compact=False):
    base_pipeline = yaml.safe_load(etl.render_template._render_template_string(base_pipeline_template)[0])
    splice_pipeline = yaml.safe_load(etl.render_template._render_template_string(new_template)[0])

    base_pipeline_objects = base_pipeline['objects']
    [base_insertion_index] = [ix for ix, v in enumerate(base_pipeline_objects)
                              if v['id'] == insertion_point]

    final_pipeline_objects = (base_pipeline_objects[:base_insertion_index] +
                              splice_pipeline['objects'] +
                              base_pipeline_objects[base_insertion_index + int(replace):])

    final_pipeline = dict(base_pipeline, objects=final_pipeline_objects)
    final_pipeline['parameters'].append(splice_pipeline.get('parameters', []))
    final_pipeline['values'].update(splice_pipeline.get('values', {}))
    etl.render_template._print_template(final_pipeline, as_json=True, compact=compact)
