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
        db_access = DBAccess(conversion)
        connection_error_message = BootProcessor.__check_connection(db_access)

        if connection_error_message:
            error_message = '\t --[BootProcessor::boot] %s.' % connection_error_message
            FsOps.generate_error(conversion, error_message)
            sys.exit(-1)

        sql = """
        SELECT EXISTS(SELECT 1 FROM information_schema.tables
        WHERE table_schema = '%s' AND table_name = '%s') AS state_logs_table_exist;
        """ % (conversion.schema, conversion.schema + conversion.mysql_db_name)
        result = db_access.query('BootProcessor::boot', sql, DBVendors.PG, True, False)
        state_logs_table_exist = result.data[0]['state_logs_table_exist']
        state_message = '''\n\t--[BootProcessor::boot] PYMIG is ready to restart after some failure.
        \n\t--[BootProcessor::boot] Consider checking log files at the end of migration.''' \
            if state_logs_table_exist \
            else '\n\t--[BootProcessor::boot] PYMIG is ready to start.'

        state_message += '\n\t--[BootProcessor::boot] Proceed? [Y/n]\n\t'

        while True:
            user_input = input(state_message)
            user_input = user_input.strip()

            if user_input == 'N' or user_input == 'n':
                print('\t--[BootProcessor::boot] Migration aborted.\n')
                sys.exit(0)
            elif user_input == 'Y' or user_input == 'y':
                return
            else:
                hint = '\t--[BootProcessor::boot] Unexpected input %s \n\t--[BootProcessor::boot] ' \
                       + 'Expected input is upper case Y or lower case n\n'
                print(hint % user_input)

    @staticmethod
    def get_introduction_message():
        """
        Returns Pymig's introduction message.
        :return: string
        """
        return '\n\n\tPYMIG - the database migration tool.' \
               + '\n\tCopyright (C) 2018 - present, Anatoly Khaytovich <anatolyuss@gmail.com>' \
               + '\n\t--[BootProcessor::boot] Configuration has been just loaded.'

    @staticmethod
    def __check_connection(db_access):
        """
        Checks correctness of connection details of both MySQL and PostgreSQL.
        :param db_access: DBAccess instance
        :return: string
        """
        log_title = 'BootProcessor::check_connection'
        result_message = ''
        sql = 'SELECT 1;'
        mysql_result = db_access.query(log_title, sql, DBVendors.MYSQL, False, False)
        result_message += '	MySQL connection error: %s' % mysql_result.error if mysql_result.error else ''
        pg_result = db_access.query(log_title, sql, DBVendors.PG, False, False)
        result_message += '	PostgreSQL connection error: %s' % pg_result.error if pg_result.error else ''
        return result_message
