"""
JsonLogger -- send ETL events to a database

This is a bit experimental in that we're trying to figure
where to send the JSON blobs to that are coming from the application.
"""
import collections
import decimal
import logging

import boto3
import simplejson as json

import etl.config
import etl.pg


class DecimalEncoder(json.JSONEncoder):
    # From the AWS documentation:
    # http://docs.aws.amazon.com/amazondynamodb/latest/gettingstartedguide/GettingStarted.Python.03.html
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class JsonLogger:
    _json_store = None
    _rdbms_store = None

    @staticmethod
    def emit(record):
        if JsonLogger._json_store is None:
            JsonLogger._json_store = JsonStore(record['environment'])

        if JsonLogger._rdbms_store is None:
            JsonLogger._rdbms_store = RDBMS(record['environment'])

        clean_record = dict(record)
        clean_record['timestamp'] = decimal.Decimal(clean_record['timestamp'])

        logging.getLogger(__name__).info("Sending record to dynamodb table")
        JsonLogger._json_store.push(clean_record)

        logging.getLogger(__name__).info("Sending record to relational db table")
        JsonLogger._rdbms_store.push(clean_record)


def query_for(target_list, etl_id=None):
    logger = logging.getLogger(__name__)
    logger.info("Querying for: etl_id=%s target=%s", etl_id, target_list)
    # XXX more stuff here to query dynamodb


class JsonStore:
    def __init__(self, environment):
        logging.getLogger(__name__).info("Initializing connection to non-relational store")
        self._resource = boto3.resource('dynamodb')
        self._client = boto3.client('dynamodb')
        self._table_name = environment + '_etl_events'
        self.create_table_if_not_exists()

    def push(self, payload):
        self._resource.Table(self._table_name).put_item(Item=payload)

    def create_table_if_not_exists(self):
        logging.getLogger(__name__).info("Checking to see if " + self._table_name + " already exists...")
        try:
            table_desc = self._client.describe_table(TableName=self._table_name)
            logging.getLogger(__name__).info(self._table_name + " already exists.")
        except Exception as e:
            if "Requested resource not found: Table" in str(e):
                table = self._resource.create_table(
                    TableName=self._table_name,
                    KeySchema=[
                        {
                            'AttributeName': 'target',
                            'KeyType': 'HASH'  # Partition key
                        },
                        {
                            'AttributeName': 'timestamp',
                            'KeyType': 'RANGE'  # Sort key
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'target',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'timestamp',
                            'AttributeType': 'N'
                        },

                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 10,
                        'WriteCapacityUnits': 10
                    }
                )
                table.meta.client.get_waiter('table_exists').wait(TableName=self._table_name)
                logging.getLogger(__name__).info("Table status: " + table.table_status)
            else:
                logging.getLogger(__name__).exception(self._table_name + " cannot be created.")
                raise


class RDBMS:
    def __init__(self, environment):
        self._table_name = environment + '_etl_events'
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        logging.getLogger(__name__).info("Creating " + self._table_name +
                                         " in a RDBMS if it doesn't already exist.")
        try:
            dsn = etl.config.env_value("ETL_EVENT_TABLE_URI")
            conn = etl.pg.connection(dsn)

            try:
                curs = conn.cursor()
                create_table_cmd = '''
                    CREATE TABLE IF NOT EXISTS %s (
                    id serial primary key,
                    etl_id character varying (255) NOT NULL,
                    "timestamp" numeric NOT NULL,
                    aws_pipeline_id character varying(255),
                    aws_emr_id character varying(255),
                    aws_step_id character varying(255),
                    aws_instance_id character varying(255),
                    target character varying(255),
                    step character varying(255),
                    source_name character varying(255),
                    source_schema character varying(255),
                    source_table character varying(255),
                    source_bucket_name character varying(255),
                    source_object_key character varying(255),
                    destination_name character varying (255),
                    destination_schema character varying(255),
                    destination_table character varying(255),
                    destination_bucket_name character varying(255),
                    destination_object_key character varying(255),
                    event character varying(255),
                    environment character varying(255),
                    error_code character varying(255),
                    error_message character varying(255))''' % self._table_name
                curs.execute(create_table_cmd)
                conn.commit()
                logging.getLogger(__name__).info("Table created.")

            finally:
                conn.close()
                del conn
        except Exception as e:
            logging.getLogger(__name__).exception("Connecting to relational store failed.")

    def push(self, payload):
        def flatten(d, parent_key='', sep='_'):
            items = []
            for k, v in d.items():
                new_key = parent_key + sep + k if parent_key else k
                if isinstance(v, collections.MutableMapping):
                    items.extend(flatten(v, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
            return dict(items)

        try:
            dsn = etl.config.env_value("ETL_EVENT_TABLE_URI")
            conn = etl.pg.connection(dsn)

            try:
                curs = conn.cursor()
                flattened_payload = flatten(payload)
                quoted_column_names = ", ".join('"{}"'.format(column) for column in flattened_payload.keys())
                SQL = """INSERT INTO %s (%s) VALUES %%s""" % (self._table_name, quoted_column_names)
                values = (tuple(flattened_payload.values()),)
                curs.execute(SQL, values)
                conn.commit()
            finally:
                conn.close()
                del conn

        except Exception as e:
            logging.getLogger(__name__).exception("Push to RDBMS failed")
