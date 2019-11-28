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

import json
import DBVendors
from FsOps import FsOps
from Conversion import Conversion
from DBAccess import DBAccess
from MigrationStateManager import MigrationStateManager


class DataLoader:
    @staticmethod
    def load(config, data_pool_item):
        """
        Loads the data using separate process.
        :param config: dict
        :param data_pool_item: dict
        :return: None
        """
        log_title = 'DataLoader::load'
        conversion = Conversion(config)
        msg = '\t--[%s] Loading the data into "%s"."%s" table...' \
              % (log_title, conversion.schema, data_pool_item['_tableName'])

        FsOps.log(conversion, msg)
        is_recovery_mode = DataLoader.data_transferred(conversion, data_pool_item['_id'])

        if not is_recovery_mode:
            DataLoader.populate_table_worker(conversion,
                                             data_pool_item['_tableName'],
                                             data_pool_item['_selectFieldList'],
                                             data_pool_item['_rowsCnt'],
                                             data_pool_item['_id'])

            return

        pg_client = DBAccess.get_db_client(conversion, DBVendors.PG)
        DataLoader.delete_data_pool_item(conversion, data_pool_item['_id'], pg_client)

    @staticmethod
    def populate_table_worker(conversion, table_name, str_select_field_list, rows_cnt, data_pool_id):
        """
        Loads a chunk of data using "PostgreSQL COPY".
        :param conversion: Conversion
        :param table_name: string
        :param str_select_field_list: string
        :param rows_cnt: int
        :param data_pool_id: int
        :return: None
        """
        # TODO: implement.
        pass

    @staticmethod
    def delete_data_pool_item(conversion, data_pool_id, pg_client, original_session_replication_role):
        """
        Deletes given record from the data-pool.
        :param conversion: Conversion
        :param data_pool_id: int
        :param pg_client: PooledSharedDBConnection
        :param original_session_replication_role: string | None
        :return: None
        """
        log_title = 'DataLoader::delete_data_pool_item'
        data_pool_table_name = MigrationStateManager.get_data_pool_table_name(conversion)
        sql = 'DELETE FROM %s WHERE id = %d;' % (data_pool_table_name, data_pool_id)
        DBAccess.query(conversion, log_title, sql, DBVendors.PG, False, True, pg_client)

        if original_session_replication_role:
            DataLoader.enable_triggers(conversion, pg_client, original_session_replication_role)

    @staticmethod
    def enable_triggers(conversion, pg_client, original_session_replication_role):
        """
        Enables all triggers and rules for current database session.
        !!!DO NOT release the client, it will be released after current data-chunk deletion.
        :param conversion: Conversion
        :param pg_client: PooledSharedDBConnection
        :param original_session_replication_role: string
        :return: None
        """
        log_title = 'DataLoader::enable_triggers'
        sql = 'SET session_replication_role = %s;' % original_session_replication_role
        DBAccess.query(conversion, log_title, sql, DBVendors.PG, False, False, pg_client)

    @staticmethod
    def data_transferred(conversion, data_pool_id):
        """
        Enforces consistency before processing a chunk of data.
        Ensures there are no data duplications.
        In case of normal execution - it is a good practice.
        In case of rerunning Pymig after unexpected failure - it is absolutely mandatory.
        :param conversion: Conversion
        :param data_pool_id: int
        :return: bool
        """
        log_title = 'DataLoader::data_transferred'
        data_pool_table_name = MigrationStateManager.get_data_pool_table_name(conversion)
        sql_get_metadata = 'SELECT metadata AS metadata FROM %s WHERE id = %d;' % (data_pool_table_name, data_pool_id)
        result = DBAccess.query(conversion, log_title, sql_get_metadata, DBVendors.PG, True, True)
        metadata = json.loads(result.data[0]['metadata'])
        target_table_name = '"%s"."%s"' % (conversion.schema, metadata['_tableName'])
        sql_get_first_row = 'SELECT * FROM %s LIMIT 1 OFFSET 0;' % target_table_name
        probe = DBAccess.query(conversion, log_title, sql_get_first_row, DBVendors.PG, True, False, result.client)
        return len(probe.data) != 0
