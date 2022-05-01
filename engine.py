"""Initiate all tasks for this job."""

import sys
import uuid
import luigi
import logging
import warnings
from datetime import datetime
from .tasks.utils import *
from .tasks.transformations import *

from .tasks.database_reader import DatabaseReaderFactory, RedisStore
from .tasks.constants import PARTIAL_LOAD_TYPE, LAST_RUN_BUCKET_NAME, TARGET_TYPE_ORACLE
from .tasks.aws_utils import get_last_run, save_last_run
from .tasks.database_reader import ORACLEDB


warnings.simplefilter("ignore")
logger = logging.getLogger('main')


class EXECUTOR(luigi.Task):

    source_type = luigi.Parameter()
    target_type = luigi.Parameter()
    query_file = luigi.Parameter()
    source_parameters = luigi.Parameter()
    target_parameters = luigi.Parameter()
    table_name = luigi.Parameter()
    schema_name = luigi.Parameter()
    column_mapping = luigi.Parameter()
    extra_columns_class = luigi.Parameter()
    transformations = luigi.Parameter()
    pk_rec_keys = luigi.Parameter()
    job_id = luigi.Parameter(default=str(uuid.uuid4()))
    storage = luigi.Parameter()
    load_type = luigi.Parameter()
    job_name = luigi.Parameter()

    def _validate_yaml(self):
        if not self.source_type:
            sys.exit("Please provide source_url under db_properties in Properties.yaml")
        if not self.target_type:
            sys.exit("Please provide dest_url under db_properties in Properties.yaml")
        if not self.query_file:
            sys.exit("Please provide query_file under db_properties in Properties.yaml")
        if not self.source_parameters:
            sys.exit("Please provide dest_db under db_properties in Properties.yaml")
        if not self.target_parameters:
            sys.exit("Please provide dest_password_key under db_properties in Properties.yaml")
        if not self.table_name:
            sys.exit("Please provide table_name in Properties.yaml")
        if not self.schema_name:
            sys.exit("Please provide table_name in Properties.yaml")
        if not self.column_mapping:
            sys.exit("Please provide column_mapping under query_output_columns in Properties.yaml")
        if not self.extra_columns_class:
            sys.exit("Please provide extra_columns_class in Properties.yaml if not provide empty string")
        if not self.transformations:
            sys.exit("Please provide transformations in Properties.yaml if not provide empty string")

    def get_query_from_file(self):
        filename = 'config/queries/' + str(self.query_file)
        fd = open(filename, 'r')
        sqlFile = fd.read()
        fd.close()
        sqlCommands = sqlFile.split(';')
        return sqlCommands[0]

    def is_redis_storage_enabled(self):
        if self.storage.get('redis') == 'enabled':
            return True
        if self.storage.get('redis') == 'disabled':
            return False

    def get_temp_storage(self):
        if self.is_redis_storage_enabled():
            if True:
                return RedisStore()
        return None

    def _get_connections(self):
        self.target_parameters['table_name'] = self.table_name
        self.source_instance = DatabaseReaderFactory.get(self.source_type, self.source_parameters)
        self.target_instance = DatabaseReaderFactory.get(self.target_type, self.target_parameters)
        self.temp_storage = self.get_temp_storage()
        try:
            self.transformation_instance = eval(str(self.transformations))(table_name=self.table_name,
                                                                           pk_rec_keys=self.get_pk_rec_keys(),
                                                                           target_type=self.target_type)
        except:
            logger.exception(f"transformation class not defined. {self.job_id}, {self.job_name}")
            raise Exception("transformation class not defined")

    def get_pk_rec_keys(self):
        return list(map(lambda x: x.strip(), self.pk_rec_keys.split(',')))

    def requires(self):
        pass
    '''
    stored_data = ['(a, b)']
    '''

    def update_extra_columns(self, item, index):
        item_dict = dict(zip(self.column_mapping.keys(), item))
        extra_column_mapping = {}
        if hasattr(self.transformation_instance, 'get_extra_columns'):
            extra_column_mapping = self.transformation_instance.get_extra_columns(item)
        if self.target_type == TARGET_TYPE_ORACLE:
            oracle_extra_column_mapping = get_extra_columns_for_oracle(
                ORACLEDB(table_name=self.table_name), self.pk_rec_keys, item_dict)
            extra_column_mapping = {**extra_column_mapping, **oracle_extra_column_mapping}

        for k, v in extra_column_mapping.items():
            if isinstance(item, tuple):
                item += (v,)
            elif isinstance(item, list):
                item.append(v)
            else:
                logger.error("cannot add to item as item is not a list/tuple")
                raise Exception("cannot add to item as item is not a list/tuple")
            if index == 0:
                self.column_mapping[k] = k

        return item

    def get_transformed_data(self, data):
        try:
            updated_data = []
            for i, item in enumerate(data):
                item = self.transformation_instance.transform(item)
                item = self.update_extra_columns(item, i)
                updated_data.append(item)
                return updated_data

            else:
                return data
        except Exception as err:
            raise err

    def write_using_redis(self):
        batch_size = 1000
        start = -1
        end = batch_size
        while True:
            stored_data = self.temp_storage.read_list(self.job_id, start, end)
            sanitized_data = []
            for data in stored_data:
                value = ['null' if x is None else x for x in data]
                sanitized_data.append(value)
            self.target_instance.write(self.table_name, sanitized_data, self.column_mapping.values())
            start += batch_size
            end += batch_size
            if not stored_data:
                break

    def write_using_local(self, source_data):
        write_batch_size = 10000
        current_batch = []
        for i, data in enumerate(source_data):
            sanitized_data = ['null' if x is None else x for x in data]
            current_batch.append(sanitized_data)
            if len(current_batch) >= write_batch_size:
                self.target_instance.write(self.table_name, current_batch, self.column_mapping.values())
                current_batch = []

        self.target_instance.write(self.table_name, current_batch, self.column_mapping.values())

        del source_data
        del current_batch

    def read_and_get_source_data(self):
        read_start_time = datetime.now()
        last_run_timestamp = None
        if self.load_type == PARTIAL_LOAD_TYPE:
            last_run_timestamp = get_last_run(LAST_RUN_BUCKET_NAME, self.job_name)

        source_data = self.source_instance.execute(self.get_query_from_file(), self.column_mapping.keys(),
                                                   last_run_timestamp=last_run_timestamp)

        save_last_run(LAST_RUN_BUCKET_NAME, self.job_name, str(read_start_time))

        logger.info(f"INFO: Unique_key_id used to load into Redis: {self.job_id}")
        source_data = self.get_transformed_data(source_data)
        if self.is_redis_storage_enabled():
            self.temp_storage.add_to_list_in_batches(self.job_id, source_data, chunk_size=10000)
            del source_data
            return

        return source_data

    def run(self):
        self._validate_yaml()
        self._get_connections()

        source_data = self.read_and_get_source_data()

        if self.is_redis_storage_enabled():
            self.write_using_redis()
        else:
            self.write_using_local(source_data)
