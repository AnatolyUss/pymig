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
    def create_sequence(conversion, table_name):
        """
        Defines which column in given table has the "auto_increment" attribute.
        Creates an appropriate sequence.
        :param conversion: Conversion
        :param table_name: str
        :return: None
        """
        original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, True)
        table_columns_list = conversion.dic_tables[table_name].table_columns
        auto_increment_columns = [column for column in table_columns_list if column['Extra'] == 'auto_increment']

        if len(auto_increment_columns) == 0:
            return  # No auto-incremented column found.

        log_title = 'SequenceProcessor::create_sequence'
        auto_increment_column = auto_increment_columns[0]['Field']
        column_name = ExtraConfigProcessor.get_column_name(
            conversion=conversion,
            original_table_name=original_table_name,
            current_column_name=auto_increment_column,
            should_get_original=False
        )

        seq_name = '%s_%s_seq' % (table_name, column_name)
        create_sequence_result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql='CREATE SEQUENCE "%s"."%s";' % (conversion.schema, seq_name),
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=True
        )

        if create_sequence_result.error:
            DBAccess.release_db_client(conversion, create_sequence_result.client)
            return

        sql_set_next_val = 'ALTER TABLE "%s"."%s" ALTER COLUMN "%s"' % (conversion.schema, table_name, column_name)
        sql_set_next_val += " SET DEFAULT NEXTVAL('%s.%s');" % (conversion.schema, seq_name)

        set_next_val_result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql_set_next_val,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=True,
            client=create_sequence_result.client
        )

        if set_next_val_result.error:
            DBAccess.release_db_client(conversion, set_next_val_result.client)
            return

        sql_set_sequence_owner = 'ALTER SEQUENCE "{0}"."{1}" OWNED BY "{0}"."{2}"."{3}";'\
            .format(conversion.schema, seq_name, table_name, column_name)

        set_sequence_owner_result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql_set_sequence_owner,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=True,
            client=set_next_val_result.client
        )

        if set_sequence_owner_result.error:
            DBAccess.release_db_client(conversion, set_sequence_owner_result.client)
            return

        sql_set_sequence_value = 'SELECT SETVAL(\'"{0}"."{1}"\', (SELECT MAX("{2}") FROM "{0}"."{3}"));'\
            .format(conversion.schema, seq_name, column_name, table_name)

        set_sequence_value_result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql_set_sequence_value,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=False,
            client=set_sequence_owner_result.client
        )

        if not set_sequence_value_result.error:
            msg = '\t--[%s] Sequence "%s"."%s" is created...' % (log_title, conversion.schema, seq_name)
            FsOps.log(conversion, msg, conversion.dic_tables[table_name].table_log_path)

    @staticmethod
    def set_sequence_value(conversion, table_name):
        """
        Sets sequence value.
        :param conversion: Conversion
        :param table_name: str
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
