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

import DBVendors
from DBAccess import DBAccess


def boot(conversion):
    """
    Boots the migration.
    :param conversion: Conversion, the configuration object.
    "return" None
    """
    db_access = DBAccess(conversion)
    connection_error_message = check_connection(db_access)
    # Get logo.
    if connection_error_message:
        pass


def check_connection(db_access):
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
