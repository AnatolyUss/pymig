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


class CommentsProcessor:
    @staticmethod
    def process_comments(conversion, table_name):
        """
        Migrates comments.
        :param conversion: Conversion
        :param table_name: str
        :return: None
        """
        log_title = 'CommentsProcessor::processComments'
        msg = '\t--[%s] Creates comments for table "%s"."%s"...' % (log_title, conversion.schema, table_name)
        FsOps.log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
        CommentsProcessor._process_table_comments(conversion, table_name)
        CommentsProcessor._process_columns_comments(conversion, table_name)

    @staticmethod
    def _process_table_comments(conversion, table_name):
        """
        Creates table comments.
        :param conversion: Conversion
        :param table_name: str
        :return: None
        """
        log_title = 'CommentsProcessor::_process_table_comments'
        sql_select_comment = '''
            SELECT table_comment AS table_comment FROM information_schema.tables 
            WHERE table_schema = '%s' AND table_name = '%s';
        ''' % (
            conversion.mysql_db_name,
            ExtraConfigProcessor.get_table_name(conversion, table_name, should_get_original=True)
        )

        select_comments_result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql_select_comment,
            vendor=DBVendors.MYSQL,
            process_exit_on_error=False,
            should_return_client=False
        )

        if select_comments_result.error:
            return

        comment = CommentsProcessor._escape_quotes(select_comments_result.data[0]['table_comment'])
        sql_create_comment = 'COMMENT ON TABLE "%s"."%s" IS \'%s\';' % (conversion.schema, table_name, comment)
        create_comment_result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql_create_comment,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=False
        )

        if create_comment_result.error:
            return

        msg = '\t--[%s] Successfully set comment for table "%s"."%s"' % (log_title, conversion.schema, table_name)
        FsOps.log(conversion, msg, conversion.dic_tables[table_name].table_log_path)

    @staticmethod
    def _process_columns_comments(conversion, table_name):
        """
        Creates columns comments.
        :param conversion: Conversion
        :param table_name: str
        :return: None
        """
        original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, should_get_original=True)
        params = [
            [conversion, table_name, original_table_name, column]
            for column in conversion.dic_tables[table_name].table_columns
            if column['Comment'] != ''
        ]

        ConcurrencyManager.run_concurrently(conversion, CommentsProcessor._set_column_comment, params)

    @staticmethod
    def _set_column_comment(conversion, table_name, original_table_name, column):
        """
        Creates comment on specified column.
        :param conversion: Conversion
        :param table_name: str
        :param original_table_name: str
        :param column: dict
        :return: None
        """
        log_title = 'CommentsProcessor::_set_column_comment'
        column_name = ExtraConfigProcessor.get_column_name(
            conversion=conversion,
            original_table_name=original_table_name,
            current_column_name=column['Field'],
            should_get_original=False
        )

        comment = CommentsProcessor._escape_quotes(column['Comment'])
        create_comment_result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql='COMMENT ON COLUMN "%s"."%s"."%s" IS \'%s\';' % (conversion.schema, table_name, column_name, comment),
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=False
        )

        if create_comment_result.error:
            return

        msg = '\t--[%s] Set comment for "%s"."%s" column: "%s"...' \
              % (log_title, conversion.schema, table_name, column_name)

        FsOps.log(conversion, msg, conversion.dic_tables[table_name].table_log_path)

    @staticmethod
    def _escape_quotes(string):
        """
        Escapes quotes inside given string.
        :param string: str
        :return: str
        """
        return string.replace("'", "''")
