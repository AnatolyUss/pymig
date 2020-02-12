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
from TableProcessor import TableProcessor
from ConcurrencyManager import ConcurrencyManager
from DBAccess import DBAccess
from Utils import Utils
import DBVendors


class DefaultProcessor:
    @staticmethod
    def process_default(conversion, table_name):
        """
        Determines which columns of the given table have default value.
        Sets default values where appropriate.
        :param conversion: Conversion
        :param table_name: str
        :return: None
        """
        log_title = 'DefaultProcessor::process_default'
        msg = '\t--[%s] Determines default values for table: "%s"."%s"' % (log_title, conversion.schema, table_name)
        FsOps.log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
        original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, should_get_original=True)
        pg_numeric_types = ('money', 'numeric', 'decimal', 'double precision', 'real', 'bigint', 'int', 'smallint')
        sql_reserved_values = {
            'CURRENT_DATE': 'CURRENT_DATE',
            '0000-00-00': "'-INFINITY'",
            'CURRENT_TIME': 'CURRENT_TIME',
            '00:00:00': '00:00:00',
            'CURRENT_TIMESTAMP': 'CURRENT_TIMESTAMP',
            '0000-00-00 00:00:00': "'-INFINITY'",
            'LOCALTIME': 'LOCALTIME',
            'LOCALTIMESTAMP': 'LOCALTIMESTAMP',
            'NULL': 'NULL',
            'null': 'NULL',
            'UTC_DATE': "(CURRENT_DATE AT TIME ZONE 'UTC')",
            'UTC_TIME': "(CURRENT_TIME AT TIME ZONE 'UTC')",
            'UTC_TIMESTAMP': "(NOW() AT TIME ZONE 'UTC')",
        }

        params = [
            [conversion, table_name, original_table_name, column, sql_reserved_values, pg_numeric_types]
            for column in conversion.dic_tables[table_name].table_columns
        ]

        ConcurrencyManager.run_in_parallel(conversion, DefaultProcessor._set_default, params)

    @staticmethod
    def _set_default(conversion, table_name, original_table_name, column, sql_reserved_values, pg_numeric_types):
        """
        Sets default value for given column.
        :param conversion: Conversion
        :param table_name: str
        :param original_table_name: str
        :param column: dict
        :param sql_reserved_values: dict
        :param pg_numeric_types: tuple
        :return: None
        """
        pg_data_type = TableProcessor.map_data_types(conversion.data_types_map, column['Type'])
        log_title = 'DefaultProcessor::_set_default'
        column_name = ExtraConfigProcessor.get_column_name(
            conversion=conversion,
            original_table_name=original_table_name,
            current_column_name=column['Field'],
            should_get_original=False
        )

        sql = 'ALTER TABLE "%s"."%s" ALTER COLUMN "%s" SET DEFAULT ' % (conversion.schema, table_name, column_name)

        if column['Default'] in sql_reserved_values:
            sql += '%s;' % sql_reserved_values[column['Default']]
        elif Utils.get_index_of(pg_data_type, pg_numeric_types) == -1 and column['Default'] is not None:
            sql += "'%s';" % column['Default']
        elif column['Default'] is None:
            sql += 'NULL;'
        else:
            sql += "%s;" % column['Default']

        result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=False
        )

        if not result.error:
            msg = '\t--[%s] Sets default value for "%s"."%s"."%s"...' \
                  % (log_title, conversion.schema, table_name, column_name)

            FsOps.log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
