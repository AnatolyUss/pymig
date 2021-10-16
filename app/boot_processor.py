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

import app.db_access as DBAccess
from app.db_vendor import DBVendor
from app.fs_ops import generate_error
from app.conversion import Conversion


def boot(conversion: Conversion) -> None:
    """
    Boots the migration.
    """
    connection_error_message = _check_connection(conversion)

    if connection_error_message:
        error_message = f'\t --[{boot.__name__}] {connection_error_message}'
        generate_error(conversion, error_message)
        sys.exit(1)

    table_name = conversion.schema + conversion.mysql_db_name
    sql = ("SELECT EXISTS(SELECT 1 FROM information_schema.tables"
           f"WHERE table_schema = '{conversion.schema}' AND table_name = '{table_name}') AS state_logs_table_exist;")

    result = DBAccess.query(
        conversion=conversion,
        caller=boot.__name__,
        sql=sql,
        vendor=DBVendor.PG,
        process_exit_on_error=True,
        should_return_client=False
    )

    state_logs_table_exist = result.data[0]['state_logs_table_exist']
    recovery_state_message = (f'''\n\t--[{boot.__name__}] FromMySqlToPostgreSql is ready to restart after some failure.
                              \n\t--[{boot.__name__}] Consider checking log files at the end of migration''')

    normal_state_message = f'\n\t--[{boot.__name__}] FromMySqlToPostgreSql is ready to start'
    print(recovery_state_message if state_logs_table_exist else normal_state_message)
    conversion.time_begin = time.time()


def get_introduction_message() -> str:
    """
    Returns the introduction message.
    """
    return ('\n\n\tFromMySqlToPostgreSql - the database migration tool'
            '\n\tCopyright (C) 2015 - present, Anatoly Khaytovich <anatolyuss@gmail.com>'
            f'\n\t--[{boot.__name__}] Configuration has been just loaded')


def _check_connection(conversion: Conversion) -> str:
    """
    Checks correctness of connection details of both MySQL and PostgreSQL.
    """
    sql = 'SELECT 1;'
    mysql_result = DBAccess.query(
        conversion=conversion,
        caller=_check_connection.__name__,
        sql=sql,
        vendor=DBVendor.MYSQL,
        process_exit_on_error=False,
        should_return_client=False
    )

    result_message = f'	MySQL connection error: {mysql_result.error}' if mysql_result.error else ''
    pg_result = DBAccess.query(
        conversion=conversion,
        caller=_check_connection.__name__,
        sql=sql,
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=False
    )

    result_message += f'	PostgreSQL connection error: {pg_result.error}' if pg_result.error else ''
    return result_message
