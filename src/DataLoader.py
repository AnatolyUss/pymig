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

import io
import json
import DBVendors
from FsOps import FsOps
from Conversion import Conversion
from DBAccess import DBAccess
from MigrationStateManager import MigrationStateManager
from ExtraConfigProcessor import ExtraConfigProcessor


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

        if is_recovery_mode:
            pg_client = DBAccess.get_db_client(conversion, DBVendors.PG)
            DataLoader.delete_data_pool_item(conversion, data_pool_item['_id'], pg_client)
        else:
            DataLoader.populate_table_worker(conversion,
                                             data_pool_item['_tableName'],
                                             data_pool_item['_selectFieldList'],
                                             data_pool_item['_rowsCnt'],
                                             data_pool_item['_id'])

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
        original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, True)
        DataLoader.__retrieve_source_data(conversion, str_select_field_list, original_table_name, table_name)

    @staticmethod
    def __retrieve_source_data(conversion, str_select_field_list, original_table_name, target_table_name):
        """
        TODO: add description.
        :param conversion: Conversion
        :param str_select_field_list: string
        :param original_table_name: string
        :param target_table_name: string
        :return: None
        """
        mysql_client = DBAccess.get_mysql_unbuffered_client(conversion)
        mysql_cursor = mysql_client.cursor()
        sql = 'SELECT %s FROM `%s`;' % (str_select_field_list, original_table_name)
        mysql_cursor.execute(sql)
        text_stream = None

        pg_client = DBAccess.get_db_client(conversion, DBVendors.PG)
        pg_cursor = pg_client.cursor()

        try:
            while True:
                batch = mysql_cursor.fetchmany(100000)  # TODO: think about batch size calculation.

                if len(batch) == 0:
                    break

                row = '\n'.join('\t'.join('\\N' if column is None else str(column) for column in row) for row in batch)
                text_stream = io.StringIO()
                text_stream.write(row)
                text_stream.seek(0)
                pg_cursor.copy_from(text_stream, '"%s"."%s"' % (conversion.schema, target_table_name))
                pg_client.commit()
        finally:
            if text_stream:
                text_stream.close()

            for cursor in (pg_cursor, mysql_cursor):
                if cursor:
                    cursor.close()

            if mysql_client:
                mysql_client.close()

            print('DONE')

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

        DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=True,
            client=pg_client
        )

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
        DBAccess.query(
            conversion=conversion,
            caller='DataLoader::enable_triggers',
            sql='SET session_replication_role = %s;' % original_session_replication_role,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=False,
            client=pg_client
        )

    @staticmethod
    def disable_triggers(conversion, pg_client):
        """
        Disables all triggers and rules for current database session.
        !!!DO NOT release the client, it will be released after current data-chunk deletion.
        :param conversion: Conversion
        :param pg_client: PooledSharedDBConnection
        :return: string
        """
        sql = 'SHOW session_replication_role;'
        original_session_replication_role = 'origin'
        log_title = 'DataLoader::disable_triggers'

        query_result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=True,
            client=pg_client
        )

        if query_result.data:
            original_session_replication_role = query_result.data[0]['session_replication_role']

        DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql='SET session_replication_role = replica;',
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=False,
            client=query_result.client
        )

        return original_session_replication_role

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

        result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql='SELECT metadata AS metadata FROM %s WHERE id = %d;' % (data_pool_table_name, data_pool_id),
            vendor=DBVendors.PG,
            process_exit_on_error=True,
            should_return_client=True
        )

        metadata = json.loads(result.data[0]['metadata'])
        target_table_name = '"%s"."%s"' % (conversion.schema, metadata['_tableName'])

        probe = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql='SELECT * FROM %s LIMIT 1 OFFSET 0;' % target_table_name,
            vendor=DBVendors.PG,
            process_exit_on_error=True,
            should_return_client=False,
            client=result.client
        )

        return len(probe.data) != 0
