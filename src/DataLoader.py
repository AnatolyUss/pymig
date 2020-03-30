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
import io
from multiprocessing import cpu_count, connection, Pipe
from concurrent.futures import ProcessPoolExecutor
import DBVendors
from FsOps import FsOps
from Conversion import Conversion
from DBAccess import DBAccess
from MigrationStateManager import MigrationStateManager
from ExtraConfigProcessor import ExtraConfigProcessor
from ColumnsDataArranger import ColumnsDataArranger
from ConstraintsProcessor import ConstraintsProcessor


class DataLoader:
    @staticmethod
    def send_data(conversion):
        """
        Sends the data to the loader processes.
        :param conversion: Conversion
        :return: None
        """
        if len(conversion.data_pool) == 0:
            return

        params_list = [[conversion.config, meta] for meta in conversion.data_pool]
        reader_connections = []
        number_of_workers = min(
            conversion.max_each_db_connection_pool_size,
            len(conversion.data_pool),
            cpu_count()
        )

        with ProcessPoolExecutor(max_workers=number_of_workers) as executor:
            while len(params_list) != 0:
                reader_connection, writer_connection = Pipe(duplex=False)
                reader_connections.append(reader_connection)
                params = params_list.pop()
                params.append(writer_connection)
                executor.submit(DataLoader._load, *params)

            while reader_connections:
                for reader_connection in connection.wait(object_list=reader_connections):
                    just_populated_table_name = ''

                    try:
                        just_populated_table_name = reader_connection.recv()
                        reader_connection.close()
                    finally:
                        reader_connections.remove(reader_connection)
                        ConstraintsProcessor.process_constraints_per_table(conversion, just_populated_table_name)

        MigrationStateManager.set(conversion, 'per_table_constraints_loaded')

    @staticmethod
    def _load(config, data_pool_item, connection_to_master):
        """
        Loads the data using separate process.
        :param config: dict
        :param data_pool_item: dict
        :param connection_to_master: multiprocessing.connection.PipeConnection
        :return: None
        """
        log_title = 'DataLoader::_load'
        conversion = Conversion(config)
        msg = '\t--[%s] Loading the data into "%s"."%s" table...' \
              % (log_title, conversion.schema, data_pool_item['_tableName'])

        FsOps.log(conversion, msg)
        is_recovery_mode = DataLoader.data_transferred(conversion, data_pool_item['_id'])

        if is_recovery_mode:
            pg_client = DBAccess.get_db_client(conversion, DBVendors.PG)
            DataLoader.delete_data_pool_item(conversion, data_pool_item['_id'], pg_client)
        else:
            DataLoader.populate_table_worker(
                conversion,
                data_pool_item['_tableName'],
                data_pool_item['_selectFieldList'],
                data_pool_item['_rowsCnt'],
                data_pool_item['_id'],
                connection_to_master
            )

    @staticmethod
    def populate_table_worker(
            conversion,
            table_name,
            str_select_field_list,
            rows_cnt,
            data_pool_id,
            connection_to_master
    ):
        """
        Loads a chunk of data using "PostgreSQL COPY".
        :param conversion: Conversion
        :param table_name: str
        :param str_select_field_list: str
        :param rows_cnt: int
        :param data_pool_id: int
        :param connection_to_master: multiprocessing.connection.Connection
        :return: None
        """
        log_title = 'DataLoader::populate_table_worker'
        original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, True)
        sql = 'SELECT %s FROM `%s`;' % (str_select_field_list, original_table_name)
        original_session_replication_role = None
        text_stream = None
        pg_cursor = None
        pg_client = None
        mysql_cursor = None
        mysql_client = None

        try:
            pg_client = DBAccess.get_db_client(conversion, DBVendors.PG)
            pg_cursor = pg_client.cursor()

            if conversion.should_migrate_only_data():
                original_session_replication_role = DataLoader.disable_triggers(conversion, pg_client)

            mysql_client = DBAccess.get_mysql_unbuffered_client(conversion)
            mysql_cursor = mysql_client.cursor()
            mysql_cursor.execute(sql)
            number_of_inserted_rows = 0

            while True:
                batch_size = 50000  # TODO: think about batch size calculation.
                batch = mysql_cursor.fetchmany(batch_size)
                rows_to_insert = len(batch)

                if rows_to_insert == 0:
                    break

                rows = ColumnsDataArranger.prepare_batch_for_copy(batch)
                text_stream = io.StringIO()
                text_stream.write(rows)
                text_stream.seek(0)
                pg_cursor.copy_from(text_stream, '"%s"."%s"' % (conversion.schema, table_name))
                pg_client.commit()

                number_of_inserted_rows += rows_to_insert
                msg = '\t--[{0}] For now inserted: {4} rows, Total rows to insert into "{2}"."{3}": {1}' \
                    .format(log_title, rows_cnt, conversion.schema, table_name, number_of_inserted_rows)

                FsOps.log(conversion, msg)
        except Exception as e:
            msg = 'Data retrieved by following MySQL query has been rejected by the target PostgreSQL server.'
            FsOps.generate_error(conversion, '\t--[{0}] {1}\n\t--[{0}] {2}'.format(log_title, e, msg), sql)
        finally:
            try:
                connection_to_master.send(table_name)
            except Exception as ex:
                msg = 'Failed to notify master that %s table\'s populating is finished.' % table_name
                FsOps.generate_error(conversion, '\t--[{0}] {1}\n\t--[{0}] {2}'.format(log_title, ex, msg))
            finally:
                for resource in (connection_to_master, text_stream, pg_cursor, mysql_cursor, mysql_client):
                    if resource:
                        resource.close()

                DataLoader.delete_data_pool_item(conversion, data_pool_id, pg_client, original_session_replication_role)

    @staticmethod
    def delete_data_pool_item(conversion, data_pool_id, pg_client, original_session_replication_role):
        """
        Deletes given record from the data-pool.
        :param conversion: Conversion
        :param data_pool_id: int
        :param pg_client: PooledDedicatedDBConnection
        :param original_session_replication_role: str | None
        :return: None
        """
        log_title = 'DataLoader::delete_data_pool_item'
        data_pool_table_name = MigrationStateManager.get_data_pool_table_name(conversion)
        sql = 'DELETE FROM %s WHERE id = %d;' % (data_pool_table_name, data_pool_id)

        result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=True,
            client=pg_client
        )

        if original_session_replication_role and result.client:
            DataLoader.enable_triggers(conversion, result.client, original_session_replication_role)

    @staticmethod
    def enable_triggers(conversion, pg_client, original_session_replication_role):
        """
        Enables all triggers and rules for current database session.
        !!!DO NOT release the client, it will be released after current data-chunk deletion.
        :param conversion: Conversion
        :param pg_client: PooledDedicatedDBConnection
        :param original_session_replication_role: str
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
        :param pg_client: PooledDedicatedDBConnection
        :return: str
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
        In case of rerunning migration after unexpected failure - it is absolutely mandatory.
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

        metadata = result.data[0]['metadata']
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
