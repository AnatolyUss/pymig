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


class NullProcessor:
    @staticmethod
    def process_null(conversion, table_name):
        """
        Defines which columns of the given table can contain the "NULL" value.
        Sets an appropriate constraint.
        :param conversion: Conversion
        :param table_name: str
        :return: None
        """
        log_title = 'NullProcessor::process_null'
        msg = '\t--[%s] Sets "NOT NULL" constraints for table: "%s"."%s"' % (log_title, conversion.schema, table_name)
        FsOps.log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
        original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, should_get_original=True)
        params = [
            [conversion, table_name, original_table_name, column]
            for column in conversion.dic_tables[table_name].table_columns
            if column['Null'].lower() == 'no'
        ]

        ConcurrencyManager.run_concurrently(conversion, NullProcessor._set_not_null, params)

    @staticmethod
    def _set_not_null(conversion, table_name, original_table_name, column):
        """
        Sets the NOT NULL constraint for given column.
        :param conversion: Conversion
        :param table_name: str
        :param original_table_name: str
        :param column: dict
        :return: bool
        """
        log_title = 'NullProcessor::_set_not_null'
        column_name = ExtraConfigProcessor.get_column_name(
            conversion=conversion,
            original_table_name=original_table_name,
            current_column_name=column['Field'],
            should_get_original=False
        )

        result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql='ALTER TABLE "%s"."%s" ALTER COLUMN "%s" SET NOT NULL;' % (conversion.schema, table_name, column_name),
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=False
        )

        if not result.error:
            msg = '\t--[%s] Set NOT NULL for "%s"."%s"."%s"...' \
                  % (log_title, conversion.schema, table_name, column_name)

            FsOps.log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
