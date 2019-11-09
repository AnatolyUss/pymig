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
from FsOps import FsOps


class MigrationStateManager:
    @staticmethod
    def create_state_logs_table(conversion):
        """
        Creates the "{schema}"."state_logs_{schema + mysql_db_name}" temporary table.
        :param conversion: Conversion, Pymig configuration object.
        :return: None
        """
        log_title = 'MigrationStateManager::create_state_logs_table'
        state_logs_table_name = '"{0}"."state_logs_{0}{1}"'.format(conversion.schema, conversion.mysql_db_name)
        sql = '''
        CREATE TABLE IF NOT EXISTS %s("tables_loaded" BOOLEAN, "per_table_constraints_loaded" BOOLEAN, 
        "foreign_keys_loaded" BOOLEAN, "views_loaded" BOOLEAN);
        ''' % state_logs_table_name

        db_access = DBAccess(conversion)
        result = db_access.query(log_title, sql, DBVendors.PG, True, True)
        sql = 'SELECT COUNT(1) AS cnt FROM %s' % state_logs_table_name
        result = db_access.query(log_title, sql, DBVendors.PG, True, True, result.client)
        msg = '\t --[%s] Table %s' % (log_title, state_logs_table_name)

        if result.data[0]['cnt'] == 0:
            sql = 'INSERT INTO %s VALUES (FALSE, FALSE, FALSE, FALSE);' % state_logs_table_name
            db_access.query(log_title, sql, DBVendors.PG, True, False, result.client)
            msg += ' is created.'
        else:
            msg += ' already exists.'

        FsOps.log(conversion, msg)