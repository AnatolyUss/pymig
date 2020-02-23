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
import DBVendors
from Utils import Utils
from FsOps import FsOps
from DBAccess import DBAccess
from ConcurrencyManager import ConcurrencyManager
from MigrationStateManager import MigrationStateManager


class ViewGenerator:
    @staticmethod
    def generate_views(conversion):
        """
        Attempts to convert MySQL views to PostgreSQL views.
        :param conversion: Conversion
        :return: None
        """
        views_loaded = MigrationStateManager.get(conversion, 'views_loaded')

        if views_loaded:
            return

        params = [[conversion, view_name] for view_name in conversion.views_to_migrate]
        ConcurrencyManager.run_in_parallel(conversion, ViewGenerator._generate_single_view, params)

    @staticmethod
    def _generate_single_view(conversion, view_name):
        """
        Attempts to convert given view from MySQL to PostgreSQL.
        :param conversion: Conversion
        :param view_name: str
        :return: None
        """
        log_title = 'ViewGenerator::_generate_single_view'
        show_create_view_result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            vendor=DBVendors.MYSQL,
            process_exit_on_error=False,
            should_return_client=False,
            sql='SHOW CREATE VIEW `%s`;' % view_name
        )

        if show_create_view_result.error:
            return

        create_pg_view_sql = ViewGenerator._generate_view_code(
            schema=conversion.schema,
            view_name=view_name,
            mysql_view_code=show_create_view_result.data[0]['Create View']
        )

        create_pg_view_result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=False,
            sql=create_pg_view_sql
        )

        if create_pg_view_result.error:
            ViewGenerator._log_not_created_view(conversion, view_name, create_pg_view_sql)
            return

        FsOps.log(conversion, '\t--[%s] View "%s"."%s" is created...' % (log_title, conversion.schema, view_name))

    @staticmethod
    def _log_not_created_view(conversion, view_name, sql):
        """
        Writes a log, containing a code of the view FromMySqlToPostgreSql has just failed to create.
        :param conversion: Conversion
        :param view_name: str
        :param sql: str
        :return: None
        """
        view_file_path = os.path.join(conversion.not_created_views_path, '%s.sql' % view_name)
        FsOps.write_to_file(view_file_path, 'w', sql)

    @staticmethod
    def _generate_view_code(schema, view_name, mysql_view_code):
        """
        Attempts to convert MySQL view to PostgreSQL view.
        Attempts to generate a PostgreSQL equivalent of MySQL view.
        :param schema: str
        :param view_name: str
        :param mysql_view_code: str
        :return: str
        """
        mysql_view_code = '"'.join(mysql_view_code.split('`'))
        query_start_position = Utils.get_index_of('AS', mysql_view_code)
        mysql_view_code = mysql_view_code[query_start_position:]
        mysql_view_code_list = mysql_view_code.split(' ')
        mysql_view_code_list_length = len(mysql_view_code_list)

        for position, key_word in enumerate(mysql_view_code_list):
            key_word_lower = key_word.lower()
            next_position = position + 1

            if key_word_lower == 'from' or key_word_lower == 'join' and next_position < mysql_view_code_list_length:
                mysql_view_code_list[next_position] = '"%s".%s' % (schema, mysql_view_code_list[next_position])

        return 'CREATE OR REPLACE VIEW "%s"."%s" %s;' % (schema, view_name, ' '.join(mysql_view_code_list))
