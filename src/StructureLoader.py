__author__ = "Anatoly Khaytovich <anatolyuss@gmail.com>"
__copyright__ = "Copyright (C) 2018 - present, Anatoly Khaytovich <anatolyuss@gmail.com>"
__license__ = """
    This file is a part of "PYMIG" - the database migration tool.
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

import DBVendors
from Utils import Utils
from Table import Table
from DBAccess import DBAccess
from TableProcessor import TableProcessor
from DataChunksProcessor import DataChunksProcessor
from ExtraConfigProcessor import ExtraConfigProcessor
from MigrationStateManager import MigrationStateManager


class StructureLoader:
    @staticmethod
    def load_structure(conversion):
        """
        Loads source tables and views, that need to be migrated.
        :param conversion: Conversion, Pymig configuration object.
        :return: None
        """
        log_title = 'StructureLoader::load_structure'
        StructureLoader.__get_mysql_version(conversion)
        have_tables_loaded = MigrationStateManager.get(conversion, 'tables_loaded')
        sql = 'SHOW FULL TABLES IN `%s` WHERE 1 = 1' % conversion.mysql_db_name

        if len(conversion.include_tables) != 0:
            include_tables = ','.join(list(map(lambda table_name: '"%s"' % table_name, conversion.include_tables)))
            sql += ' AND Tables_in_%s IN(%s)' % (conversion.mysql_db_name, include_tables)

        if len(conversion.exclude_tables) != 0:
            exclude_tables = ','.join(list(map(lambda table_name: '"%s"' % table_name, conversion.exclude_tables)))
            sql += ' AND Tables_in_%s NOT IN(%s)' % (conversion.mysql_db_name, exclude_tables)

        sql += ';'
        result = DBAccess.query(conversion, log_title, sql, DBVendors.MYSQL, True, False)
        tables_cnt, views_cnt = 0, 0

        for row in result.data:
            relation_name = row['Tables_in_' + conversion.mysql_db_name]

            if row['Table_type'] == 'BASE TABLE' and Utils.get_index_of(relation_name, conversion.exclude_tables) == -1:
                relation_name = ExtraConfigProcessor.get_table_name(conversion, relation_name, False)
                conversion.tables_to_migrate.append(relation_name)
                conversion.dic_tables[relation_name] = Table('%s/%s.log' % (conversion.logs_dir_path, relation_name))

                # TODO: use multiple threads to create tables in parallel.
                StructureLoader.process_table_before_data_loading(conversion, relation_name, have_tables_loaded)

                tables_cnt += 1
            elif row['Table_type'] == 'VIEW':
                conversion.views_to_migrate.append(relation_name)
                views_cnt += 1

    @staticmethod
    def process_table_before_data_loading(conversion, table_name, have_data_chunks_processed):
        """
        Processes current table before data loading.
        :param conversion: Conversion
        :param table_name: string
        :param have_data_chunks_processed: bool
        :return: None
        """
        TableProcessor.create_table(conversion, table_name)
        DataChunksProcessor.prepare_data_chunks(conversion, table_name, have_data_chunks_processed)

    @staticmethod
    def __get_mysql_version(conversion):
        """
        Retrieves the source db version.
        :param conversion: Conversion, Pymig configuration object.
        :return: None
        """
        log_title = 'StructureLoader::get_mysql_version'
        sql = 'SELECT VERSION() AS mysql_version;'
        result = DBAccess.query(conversion, log_title, sql, DBVendors.MYSQL, False, False)

        if not result.error:
            split_version = result.data[0]['mysql_version'].split('.')
            major_version = split_version[0]
            minor_version = ''.join(split_version[1:])
            conversion.mysql_version = '{0}.{1}'.format(major_version, minor_version)
