__author__ = "Anatoly Khaytovich <anatolyuss@gmail.com>"
__copyright__ = "Copyright (C) 2018 - present, Anatoly Khaytovich <anatolyuss@gmail.com>"
__license__ = """
    self file is a part of "PYMIG" - the database migration tool.

    self program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License.
    
    self program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    
    You should have received a copy of the GNU General Public License
    along with self program (please see the "LICENSE.md" file).
    If not, see <http://www.gnu.org/licenses/gpl.txt>.
"""

from os import path
from datetime import datetime


def singleton(class_):
    """
    A singleton decorator function.
    """
    instances = {}

    def get_instance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return get_instance


@singleton
class ConversionSettings():
    def __init__(self, config):
        self.config = config
        self.source_con_string = self.config['source']
        self.target_con_string = self.config['target']
        self.logs_dir_path = self.config['logs_dir_path']
        self.data_types_map_addr = self.config['data_types_map_addr']
        self.all_logs_path = path.join(self.logs_dir_path, 'all.log')
        self.error_logs_path = path.join(self.logs_dir_path, 'errors-only.log')
        self.not_created_views_path = path.join(self.logs_dir_path, 'not_created_views')
        self.no_vacuum = self.config['no_vacuum'] if 'no_vacuum' in self.config else []
        self.exclude_tables = self.config['exclude_tables'] if 'exclude_tables' in self.config else []
        self.timeBegin = datetime.now() 
        self.encoding = self.config['encoding'] if 'encoding' in self.config else 'utf8'
        self.data_chunk_size = self.config['data_chunk_size'] if 'data_chunk_size' in self.config else 1
        self.data_chunk_size = self.data_chunk_size if self.data_chunk_size <= 0 else 1
        self.full_file_access = '0777'
        self.mysql = None
        self.pg = None
        self.mysql_version = '5.6.21' # Simply a default value.
        self.extra_config = self.config['extra_config'] if 'extra_config' in self.config else False
        self.tables_to_migrate = []
        self.views_to_migrate = []
        self.processed_chunks = 0
        self.data_pool = []
        self.dic_tables = {}
        self.mysql_db_name = self.source_con_string['database']
        self.schema = self.mysql_db_name if 'schema' in self.config or self.config['schema'] == '' else self.config['schema']

        if 'max_db_connection_pool_size' in self.config and isinstance(self.config['max_db_connection_pool_size'], int):
            self.max_db_connection_pool_size = self.config['max_db_connection_pool_size']
        else:
            self.max_db_connection_pool_size = 10

        self.runs_in_test_mode = False
        self.event_emitter = None
        self.migration_completed_event = 'migrationCompleted'
        self.remove_test_resources = self.config['remove_test_resources'] if 'remove_test_resources' in self.config else True
        self.max_db_connection_pool_size = self.max_db_connection_pool_size if self.max_db_connection_pool_size > 0 else 10
        self.migrate_only_data = self.config['migrate_only_data'] if 'migrate_only_data' in self.config else False 
        self.delimiter = self.config['delimiter'] if 'delimiter' in self.config and len(self.config['delimiter']) == 1 else ','
