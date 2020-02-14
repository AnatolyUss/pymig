__author__ = "Anatoly Khaytovich <anatolyuss@gmail.com>"
__copyright__ = "Copyright (C) 2015 - present, Anatoly Khaytovich <anatolyuss@gmail.com>"
__license__ = """
    This file is a part of "FromMySqlToPostgreSql" - the database migration tool.
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program (please see the "LICENSE.md" file).
    If not, see <http://www.gnu.org/licenses/gpl.txt>.
"""
import os


class Conversion:
    def __init__(self, config):
        """
        Constructor.
        :param config: dictionary, the configuration object.
        """
        self.config = config
        self.source_con_string = self.config['source']
        self.target_con_string = self.config['target']
        self.mysql = None
        self.pg = None
        self.logs_dir_path = self.config['logs_dir_path']
        self.data_types_map = None
        self.data_types_map_addr = self.config['data_types_map_addr']
        self.all_logs_path = os.path.join(self.logs_dir_path, 'all.log')
        self.error_logs_path = os.path.join(self.logs_dir_path, 'errors-only.log')
        self.not_created_views_path = os.path.join(self.logs_dir_path, 'not_created_views')
        self.no_vacuum = self.config['no_vacuum'] if 'no_vacuum' in self.config else []
        self.exclude_tables = self.config['exclude_tables'] if 'exclude_tables' in self.config else []
        self.include_tables = self.config['include_tables'] if 'include_tables' in self.config else []
        self.encoding = self.config['encoding'] if 'encoding' in self.config else 'utf_8'
        self.time_begin = None
        self.mysql_version = '5.6.21'
        self.extra_config = self.config['extra_config'] if 'extra_config' in self.config else False
        self.tables_to_migrate = []
        self.views_to_migrate = []
        self.data_pool = []
        self.dic_tables = {}
        self.mysql_db_name = self.source_con_string['database']
        self.schema = self.config['schema'] if 'schema' in self.config else self.mysql_db_name

        self.max_db_connection_pool_size = self.config['max_db_connection_pool_size'] \
            if 'max_db_connection_pool_size' in self.config else 10

        self.runs_in_test_mode = False

        self.remove_test_resources = self.config.remove_test_resources \
            if 'remove_test_resources' in self.config else True

        self.migrate_only_data = self.config['migrate_only_data'] if 'migrate_only_data' in self.config else False
        self.delimiter = self.config['delimiter'] if 'delimiter' in self.config else ','

    def should_migrate_only_data(self):
        """
        Checks if there are actions to take other than data migration.
        :return: bool
        """
        return self.migrate_only_data
