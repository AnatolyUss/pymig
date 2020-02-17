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

import json
import DBVendors
from DBAccess import DBAccess
from FsOps import FsOps


class MigrationStateManager:
    @staticmethod
    def get_state_logs_table_name(conversion):
        """
        Returns state-logs table name.
        :param conversion: Conversion, the configuration object.
        :return: str
        """
        return '"{0}"."state_logs_{0}{1}"'.format(conversion.schema, conversion.mysql_db_name)

    @staticmethod
    def get_data_pool_table_name(conversion):
        """
        Returns data-pool table name.
        :param conversion: Conversion, the configuration object.
        :return: str
        """
        return '"{0}"."data_pool_{0}{1}"'.format(conversion.schema, conversion.mysql_db_name)

    @staticmethod
    def get(conversion, param):
        """
        Retrieves appropriate state-log.
        :param conversion: Conversion, the configuration object.
        :param param: str, state-log parameter.
        :return: bool
        """
        table_name = MigrationStateManager.get_state_logs_table_name(conversion)
        result = DBAccess.query(
            conversion=conversion,
            caller='MigrationStateManager::get',
            sql='SELECT %s FROM %s;' % (param, table_name),
            vendor=DBVendors.PG,
            process_exit_on_error=True,
            should_return_client=False
        )

        return result.data[0][param]

    @staticmethod
    def set(conversion, *states):
        """
        Updates the state-log.
        :param conversion: Conversion
        :param states: tuple
        :return: None
        """
        table_name = MigrationStateManager.get_state_logs_table_name(conversion)
        states_sql = ','.join(['%s = TRUE' % state for state in states])
        DBAccess.query(
            conversion=conversion,
            caller='MigrationStateManager::set',
            sql='UPDATE %s SET %s;' % (table_name, states_sql),
            vendor=DBVendors.PG,
            process_exit_on_error=True,
            should_return_client=False
        )

    @staticmethod
    def create_data_pool_table(conversion):
        """
        Creates the "{schema}"."data_pool_{schema + mysql_db_name}" temporary table.
        :param conversion: Conversion, the configuration object.
        :return: None
        """
        log_title = 'MigrationStateManager::create_data_pool_table'
        table_name = MigrationStateManager.get_data_pool_table_name(conversion)
        DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql='CREATE TABLE IF NOT EXISTS %s("id" BIGSERIAL, "metadata" TEXT);' % table_name,
            vendor=DBVendors.PG,
            process_exit_on_error=True,
            should_return_client=False
        )

        FsOps.log(conversion, '\t--[%s] table %s is created...' % (log_title, table_name))

    @staticmethod
    def read_data_pool(conversion):
        """
        Reads temporary table ("{schema}"."data_pool_{schema + mysql_db_name}"), and generates data-pool.
        :param conversion: Conversion
        :return: None
        """
        log_title = 'MigrationStateManager::read_data_pool'
        table_name = MigrationStateManager.get_data_pool_table_name(conversion)
        result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql='SELECT id AS id, metadata AS metadata FROM %s;' % table_name,
            vendor=DBVendors.PG,
            process_exit_on_error=True,
            should_return_client=False
        )

        for row in result.data:
            metadata = json.loads(row['metadata'])
            metadata['_id'] = row['id']
            conversion.data_pool.append(metadata)

        FsOps.log(conversion, '\t--[%s] Data-Pool is loaded...' % log_title)

    @staticmethod
    def create_state_logs_table(conversion):
        """
        Creates the "{schema}"."state_logs_{schema + mysql_db_name}" temporary table.
        :param conversion: Conversion, the configuration object.
        :return: None
        """
        log_title = 'MigrationStateManager::create_state_logs_table'
        table_name = MigrationStateManager.get_state_logs_table_name(conversion)
        sql = '''
        CREATE TABLE IF NOT EXISTS %s("tables_loaded" BOOLEAN, "per_table_constraints_loaded" BOOLEAN, 
        "foreign_keys_loaded" BOOLEAN, "views_loaded" BOOLEAN);
        ''' % table_name

        result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql,
            vendor=DBVendors.PG,
            process_exit_on_error=True,
            should_return_client=True
        )

        result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql='SELECT COUNT(1) AS cnt FROM %s' % table_name,
            vendor=DBVendors.PG,
            process_exit_on_error=True,
            should_return_client=True,
            client=result.client
        )

        msg = '\t--[%s] Table %s' % (log_title, table_name)

        if result.data[0]['cnt'] == 0:
            DBAccess.query(
                conversion=conversion,
                caller=log_title,
                sql='INSERT INTO %s VALUES (FALSE, FALSE, FALSE, FALSE);' % table_name,
                vendor=DBVendors.PG,
                process_exit_on_error=True,
                should_return_client=False,
                client=result.client
            )

            msg += ' is created.'
        else:
            msg += ' already exists.'

        FsOps.log(conversion, msg)

    @staticmethod
    def drop_state_logs_table(conversion):
        """
        Drop state logs temporary table.
        :param conversion: Conversion
        :return: None
        """
        DBAccess.query(
            conversion=conversion,
            caller='MigrationStateManager::drop_state_logs_table',
            sql='DROP TABLE %s;' % MigrationStateManager.get_state_logs_table_name(conversion),
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=False
        )
