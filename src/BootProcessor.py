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
import time
import sys
import DBVendors
from FsOps import FsOps
from DBAccess import DBAccess


class BootProcessor:
    @staticmethod
    def boot(conversion):
        """
        Boots the migration.
        :param conversion: Conversion, the configuration object.
        "return" None
        """
        connection_error_message = BootProcessor._check_connection(conversion)

        if connection_error_message:
            error_message = '\t --[BootProcessor::boot] %s.' % connection_error_message
            FsOps.generate_error(conversion, error_message)
            sys.exit(1)

        sql = """
        SELECT EXISTS(SELECT 1 FROM information_schema.tables
        WHERE table_schema = '%s' AND table_name = '%s') AS state_logs_table_exist;
        """ % (conversion.schema, conversion.schema + conversion.mysql_db_name)

        result = DBAccess.query(
            conversion=conversion,
            caller='BootProcessor::boot',
            sql=sql,
            vendor=DBVendors.PG,
            process_exit_on_error=True,
            should_return_client=False
        )

        state_logs_table_exist = result.data[0]['state_logs_table_exist']
        state_message = '''\n\t--[BootProcessor::boot] FromMySqlToPostgreSql is ready to restart after some failure.
        \n\t--[BootProcessor::boot] Consider checking log files at the end of migration.''' \
            if state_logs_table_exist \
            else '\n\t--[BootProcessor::boot] FromMySqlToPostgreSql is ready to start.'

        print(state_message)
        conversion.time_begin = time.time()

    @staticmethod
    def get_introduction_message():
        """
        Returns the introduction message.
        :return: str
        """
        return '\n\n\tFromMySqlToPostgreSql - the database migration tool.' \
               + '\n\tCopyright (C) 2015 - present, Anatoly Khaytovich <anatolyuss@gmail.com>' \
               + '\n\t--[BootProcessor::boot] Configuration has been just loaded.'

    @staticmethod
    def _check_connection(conversion):
        """
        Checks correctness of connection details of both MySQL and PostgreSQL.
        :param conversion: Conversion, the configuration object.
        :return: str
        """
        log_title = 'BootProcessor::check_connection'
        result_message = ''
        sql = 'SELECT 1;'

        mysql_result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql,
            vendor=DBVendors.MYSQL,
            process_exit_on_error=False,
            should_return_client=False
        )

        result_message += '	MySQL connection error: %s' % mysql_result.error if mysql_result.error else ''

        pg_result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=False
        )

        result_message += '	PostgreSQL connection error: %s' % pg_result.error if pg_result.error else ''
        return result_message
