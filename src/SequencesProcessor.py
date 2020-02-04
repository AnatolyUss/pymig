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
from ExtraConfigProcessor import ExtraConfigProcessor
from DBAccess import DBAccess
from FsOps import FsOps
import DBVendors


class SequencesProcessor:
    @staticmethod
    def set_sequence_value(conversion, table_name):
        """
        Sets sequence value.
        :param conversion: Conversion
        :param table_name: string
        :return: None
        """
        original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, True)
        table_columns_list = conversion.dic_tables[table_name].table_columns
        auto_increment_columns = [column for column in table_columns_list if column['Extra'] == 'auto_increment']

        if len(auto_increment_columns) == 0:
            return  # No auto-incremented column found.

        log_title = 'SequenceProcessor::set_sequence_value'
        auto_increment_column = auto_increment_columns[0]['Field']
        column_name = ExtraConfigProcessor.get_column_name(
            conversion=conversion,
            original_table_name=original_table_name,
            current_column_name=auto_increment_column,
            should_get_original=False
        )

        seq_name = '%s_%s_seq' % (table_name, column_name)
        sql = 'SELECT SETVAL(\'"%s"."%s"\',' % (conversion.schema, seq_name)
        sql += '(SELECT MAX("%s") FROM "%s"."%s"));' % (column_name, conversion.schema, table_name)
        result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=False
        )

        if not result.error:
            msg = '\t--[%s] Sequence "%s"."%s" is created...' % (log_title, conversion.schema, seq_name)
            FsOps.log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
