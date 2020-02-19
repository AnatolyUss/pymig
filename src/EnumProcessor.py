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
import DBVendors
from Utils import Utils
from FsOps import FsOps
from DBAccess import DBAccess
from ExtraConfigProcessor import ExtraConfigProcessor
from ConcurrencyManager import ConcurrencyManager


class EnumProcessor:
    @staticmethod
    def process_enum(conversion, table_name):
        """
        Defines which columns of the given table are of type "enum".
        Sets an appropriate constraint, if appropriate.
        :param conversion: Conversion
        :param table_name: str
        :return: None
        """
        log_title = 'EnumProcessor::process_enum'
        msg = '\t--[%s] Defines "ENUMs" for table "%s"."%s"' % (log_title, conversion.schema, table_name)
        FsOps.log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
        original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, True)
        params = [
            [conversion, table_name, original_table_name, column]
            for column in conversion.dic_tables[table_name].table_columns
            if EnumProcessor._is_enum(column)
        ]

        ConcurrencyManager.run_in_parallel(conversion, EnumProcessor._set_enum, params)

    @staticmethod
    def _is_enum(column):
        """
        Checks if given column is of type enum.
        :param column: dict
        :return: bool
        """
        if Utils.get_index_of('(', column['Type']) != -1:
            list_type = column['Type'].split('(')
            return list_type[0] == 'enum'

        return False

    @staticmethod
    def _set_enum(conversion, table_name, original_table_name, column):
        """
        Checks if given column is an enum.
        Sets the enum, if appropriate.
        :param conversion: Conversion
        :param table_name: str
        :param original_table_name: str
        :param column: dict
        :return: None
        """
        log_title = 'EnumProcessor::_set_enum'
        column_name = ExtraConfigProcessor.get_column_name(
            conversion=conversion,
            original_table_name=original_table_name,
            current_column_name=column['Field'],
            should_get_original=False
        )

        enum_values = column['Type'].split('(')[1]  # Exists due to EnumProcessor._is_enum execution result.
        sql = 'ALTER TABLE "%s"."%s" ADD CHECK ("%s" IN (%s);' \
              % (conversion.schema, table_name, column_name, enum_values)

        result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=False
        )

        if not result.error:
            msg = '\t--[%s] Set "ENUM" for "%s"."%s"."%s"...' \
                  % (log_title, conversion.schema, table_name, column_name)

            FsOps.log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
