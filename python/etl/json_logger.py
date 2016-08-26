"""
JsonLogger -- send ETL events to a database

This is a bit experimental in that we're trying to figure
where to send the JSON blobs to that are coming from the application.
"""
import collections
from datetime import datetime
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

        logging.getLogger(__name__).info("Sending record to dynamodb table ...")
        JsonLogger._json_store.push(clean_record)

        logging.getLogger(__name__).info("Sending record to relational db table...")
        JsonLogger._rdbms_store.push(clean_record)


def query_for(target_list, etl_id=None):
    logger = logging.getLogger(__name__)
    logger.info("Querying for: etl_id=%s target=%s", etl_id, target_list)
    # XXX more stuff here to query dynamodb


class JsonStore:
    def __init__(self, environment):
        logging.getLogger(__name__).info("Initializing connection to DynamoDB...")
        self._resource_name = 'dynamodb'
        self._resource = boto3.resource(self._resource_name)
        self._client = boto3.client(self._resource_name)
        self._table_name = 'etl_events_%s' % environment
        self.create_table_if_not_exists()

    def push(self, payload):
        # we cannot store datetime in dynamo so we resort to epoch as numeric
        payload['timestamp'] = decimal.Decimal(payload['timestamp'])
        self._resource.Table(self._table_name).put_item(Item=payload)

    def create_table_if_not_exists(self):
        logging.getLogger(__name__).info("Checking to see if %s already exists..." % self._table_name)
        try:
            table_desc = self._client.describe_table(TableName=self._table_name)
            logging.getLogger(__name__).info("%s already exists." % self._table_name)
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
                logging.getLogger(__name__).info("Table status: %s" % table.table_status)
            else:
                logging.getLogger(__name__).exception("%s cannot be created." % self._table_name)
                raise


class RDBMS:
    def __init__(self, environment):
        self._table_name = 'etl_events_%s' % environment
        self._resource_name = 'postgresql'
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        logging.getLogger(__name__).info("Creating %s in a %s if it doesn't already exist."
                                         % (self._table_name, self._resource_name))
        try:
            dsn = etl.config.env_value("ETL_EVENT_TABLE_URI")
            conn = etl.pg.connection(dsn)

            try:
                curs = conn.cursor()
                create_table_cmd = '''
                    CREATE TABLE IF NOT EXISTS %s (
                    id serial primary key,
                    etl_id character varying (255) NOT NULL,
                    "timestamp" timestamp without TIME ZONE NOT NULL,
                    target character varying(255) NOT NULL,
                    step character varying(255) NOT NULL,
                    event character varying(255) NOT NULL,
                    payload jsonb)''' % self._table_name
                curs.execute(create_table_cmd)
                conn.commit()
                logging.getLogger(__name__).info("Table (%s) created." % self._table_name)

            finally:
                conn.close()
                del conn
        except Exception as e:
            logging.getLogger(__name__).exception("Connecting to %s failed." % self._resource_name)

    def push(self, payload):
        # we can store date time in RDBMS, so we should
        payload['timestamp'] = datetime.fromtimestamp(payload['timestamp']).isoformat(' ')
        reformed_payload = {}
        reformed_payload['etl_id'] = payload['etl_id']
        reformed_payload['timestamp'] = payload['timestamp']
        reformed_payload['target'] = payload['target']
        reformed_payload['step'] = payload['step']
        reformed_payload['event'] = payload['event']
        reformed_payload['payload'] = json.dumps(payload)
        try:
            dsn = etl.config.env_value("ETL_EVENT_TABLE_URI")
            conn = etl.pg.connection(dsn)
            try:
                curs = conn.cursor()
                quoted_column_names = ", ".join('"{}"'.format(column) for column in reformed_payload.keys())
                SQL = """INSERT INTO %s (%s) VALUES %%s""" % (self._table_name, quoted_column_names)
                values = (tuple(reformed_payload.values()),)
                curs.execute(SQL, values)
                conn.commit()
            finally:
                conn.close()
                del conn

        except Exception as e:
            logging.getLogger(__name__).exception("Push to %s failed" % self._resource_name)
