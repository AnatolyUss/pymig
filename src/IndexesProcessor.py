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
from FsOps import FsOps
from ExtraConfigProcessor import ExtraConfigProcessor
from ConcurrencyManager import ConcurrencyManager
from DBAccess import DBAccess
import DBVendors


class IndexesProcessor:
    @staticmethod
    def create_indexes(conversion, table_name):
        """
        Creates indexes, including PK, on given table.
        :param conversion: Conversion
        :param table_name: str
        :return: None
        """
        log_title = 'IndexesProcessor::create_indexes'
        original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, should_get_original=True)
        show_index_result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql='SHOW INDEX FROM `%s`;' % original_table_name,
            vendor=DBVendors.MYSQL,
            process_exit_on_error=False,
            should_return_client=False
        )

        if show_index_result.error:
            return

        pg_indexes = {}

        for index in show_index_result.data:
            pg_column_name = ExtraConfigProcessor.get_column_name(
                conversion=conversion,
                original_table_name=original_table_name,
                current_column_name=index['Column_name'],
                should_get_original=False
            )

            if index['Key_name'] in pg_indexes:
                pg_indexes[index['Key_name']]['column_name'].append('"%s"' % pg_column_name)
                continue

            pg_index_type = 'GIST' if index['Index_type'] == 'SPATIAL' else index['Index_type']
            pg_indexes[index['Key_name']] = {
                'is_unique': index['Non_unique'] == 0,
                'column_name': ['"%s"' % pg_column_name],
                'index_type': ' USING %s' % pg_index_type,
            }

        params = [
            [conversion, index_name, table_name, pg_indexes, idx]
            for idx, index_name in enumerate(pg_indexes.keys())
        ]

        ConcurrencyManager.run_in_parallel(conversion, IndexesProcessor._set_index, params)

        msg = '\t--[%s] "%s"."%s": PK/indices are successfully set...' % (log_title, conversion.schema, table_name)
        FsOps.log(conversion, msg, conversion.dic_tables[table_name].table_log_path)

    @staticmethod
    def _set_index(conversion, index_name, table_name, pg_indexes, idx):
        """
        Sets appropriate index.
        :param conversion: conversion
        :param index_name: str
        :param table_name: str
        :param pg_indexes: dict
        :param idx: int
        :return: None
        """
        log_title = 'IndexesProcessor::_set_index'
        sql_add_index = ''

        if index_name.lower() == 'primary':
            sql_add_index += 'ALTER TABLE "%s"."%s" ADD PRIMARY KEY(%s);' \
                % (conversion.schema, table_name, ','.join(pg_indexes[index_name]['column_name']))
        else:
            column_name = '%s%s' % (pg_indexes[index_name]['column_name'][0][1:-1], str(idx))
            index_type = 'UNIQUE' if pg_indexes[index_name]['is_unique'] else ''
            sql_add_index += 'CREATE %s INDEX' % index_type
            sql_add_index += ' "%s_%s_%s_idx"' % (conversion.schema, table_name, column_name)
            sql_add_index += ' ON "%s"."%s"' % (conversion.schema, table_name)
            sql_add_index += ' %s ' % pg_indexes[index_name]['index_type']
            sql_add_index += '(%s);' % ','.join(pg_indexes[index_name]['column_name'])

        DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql_add_index,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=False
        )
