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
from typing import Optional, Any, Union, cast
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import (
    cpu_count,
    Pipe,
    connection as MultiprocessingConnection
)

import pandas as pd
from dbutils.pooled_db import PooledDedicatedDBConnection

import app.db_access as DBAccess
import app.migration_state_manager as MigrationStateManager
import app.extra_config_processor as ExtraConfigProcessor
from app.db_vendor import DBVendor
from app.fs_ops import log, generate_error
from app.conversion import Conversion
from app.constraints_processor import process_constraints_per_table


# According to Python docs:
# https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor
MAX_WORKER_PROCESSES_DEFAULT = 61


def send_data(conversion: Conversion) -> None:
    """
    Sends the data to the loader processes.
    """
    if len(conversion.data_pool) == 0:
        return

    params_list: list[list[Union[MultiprocessingConnection.Connection, dict[str, Any]]]] = [
        [conversion.config, meta]
        for meta in conversion.data_pool
    ]

    reader_connections = []
    number_of_workers = min(
        conversion.max_each_db_connection_pool_size,
        len(conversion.data_pool),
        cpu_count(),
        MAX_WORKER_PROCESSES_DEFAULT,
    )

    with ProcessPoolExecutor(max_workers=number_of_workers) as executor:
        while len(params_list) != 0:
            reader_connection, writer_connection = Pipe(duplex=False)
            reader_connections.append(reader_connection)
            params = params_list.pop()
            params.append(writer_connection)
            executor.submit(_load, *params)

        while reader_connections:
            for _reader_connection in MultiprocessingConnection.wait(object_list=reader_connections):
                _reader_connection = cast(MultiprocessingConnection.Connection, _reader_connection)
                just_populated_table_name = ''

                try:
                    just_populated_table_name = _reader_connection.recv()
                    _reader_connection.close()
                finally:
                    reader_connections.remove(_reader_connection)
                    process_constraints_per_table(conversion, just_populated_table_name)

    MigrationStateManager.set(conversion, 'per_table_constraints_loaded')


def _load(
    config: dict,
    data_pool_item: dict,
    connection_to_master: MultiprocessingConnection.Connection
) -> None:
    """
    Loads the data using separate process.
    """
    conversion = Conversion(config)
    table_name = data_pool_item['_tableName']
    msg = f'[{_load.__name__}] Loading the data into "{conversion.schema}"."{table_name}" table...'
    log(conversion, msg)
    is_recovery_mode = data_transferred(conversion, data_pool_item['_id'])

    if is_recovery_mode:
        pg_client = DBAccess.get_db_client(conversion, DBVendor.PG)
        delete_data_pool_item(conversion, data_pool_item['_id'], pg_client)
        return

    populate_table_worker(
        conversion,
        data_pool_item['_tableName'],
        data_pool_item['_selectFieldList'],
        data_pool_item['_rowsCnt'],
        data_pool_item['_id'],
        connection_to_master
    )


def populate_table_worker(
    conversion: Conversion,
    table_name: str,
    str_select_field_list: str,
    rows_cnt: int,
    data_pool_id: int,
    connection_to_master: MultiprocessingConnection.Connection
) -> None:
    """
    Loads a chunk of data using "PostgreSQL COPY".
    """
    original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, True)
    sql = f'SELECT {str_select_field_list} FROM `{original_table_name}`;'
    original_session_replication_role = None
    text_stream, pg_cursor, pg_client, mysql_client, mysql_cursor = None, None, None, None, None

    try:
        mysql_client = DBAccess.get_mysql_unbuffered_client(conversion)
        mysql_cursor = mysql_client.cursor()
        mysql_cursor.execute(sql)
        number_of_inserted_rows = 0
        number_of_workers = min(
            int(conversion.max_each_db_connection_pool_size / 2),
            cpu_count(),
            MAX_WORKER_PROCESSES_DEFAULT,
        )

        with ProcessPoolExecutor(max_workers=number_of_workers) as executor:
            while True:
                batch_size = 25000  # TODO: think about batch size calculation.
                batch = mysql_cursor.fetchmany(batch_size)
                rows_to_insert = len(batch)

                if rows_to_insert == 0:
                    break

                executor.submit(
                    _arrange_and_load_batch,
                    conversion.config,
                    table_name,
                    batch,
                    rows_cnt,
                    rows_to_insert,
                    number_of_inserted_rows
                )
    except Exception as e:
        msg = 'Data retrieved by following MySQL query has been rejected by the target PostgreSQL server.'
        error_message = f'[{populate_table_worker.__name__}] {e}\n\t--[{populate_table_worker.__name__}] {msg}'
        generate_error(conversion, error_message, sql)
    finally:
        try:
            connection_to_master.send(table_name)
        except Exception as ex:
            msg = f'Failed to notify master that {table_name} table\'s populating is finished.'
            error_message = f'[{populate_table_worker.__name__}] {ex}\n\t--[{populate_table_worker.__name__}] {msg}'
            generate_error(conversion, error_message)
        finally:
            for resource in (connection_to_master, text_stream, pg_cursor, mysql_cursor, mysql_client):
                if resource:
                    resource.close()

            delete_data_pool_item(conversion, data_pool_id, pg_client, original_session_replication_role)


def _arrange_and_load_batch(
    conversion_config: dict,
    table_name: str,
    batch: list[tuple],
    rows_cnt: int,
    rows_to_insert: int,
    number_of_inserted_rows: int
) -> None:
    """
    Formats a batch of data as csv, and passes it to COPY.
    """
    conversion = Conversion(conversion_config)

    try:
        pg_client = DBAccess.get_db_client(conversion, DBVendor.PG)
        pg_cursor = pg_client.cursor()

        if conversion.should_migrate_only_data():
            # TODO: how to pass original_session_replication_role to the parent?
            original_session_replication_role = disable_triggers(conversion, pg_client)

        # Notice, the "inline" columns encoding conversion cannot be implemented,
        # since MySQL's UTF-8 implementation isn't the same as PostgreSQL's one.
        rows = pd.DataFrame(batch).to_csv(
            index=False,
            header=False,
            encoding=conversion.encoding,
            na_rep='\\N',
            sep='\t',
        )

        text_stream = io.StringIO()
        text_stream.write(rows)
        text_stream.seek(0)
        pg_cursor.copy_from(text_stream, f'"{conversion.schema}"."{table_name}"')
        pg_client.commit()

        number_of_inserted_rows += rows_to_insert
        msg = (f'[{_arrange_and_load_batch.__name__}] For now inserted: {number_of_inserted_rows} rows, '
               f'Total rows to insert into "{conversion.schema}"."{table_name}": {rows_cnt}')

        print(msg)  # TODO: check why log() below doesn't work as expected.
        log(conversion, msg)
    except Exception as e:
        error_message = f'[{_arrange_and_load_batch.__name__}] {type(e).__name__} {repr(e)}'
        generate_error(conversion, error_message)


def delete_data_pool_item(
    conversion: Conversion,
    data_pool_id: int,
    pg_client: PooledDedicatedDBConnection,
    original_session_replication_role: Optional[str] = None
) -> None:
    """
    Deletes given record from the data-pool.
    """
    data_pool_table_name = MigrationStateManager.get_data_pool_table_name(conversion)
    sql = f'DELETE FROM {data_pool_table_name} WHERE id = {data_pool_id};'
    result = DBAccess.query(
        conversion=conversion,
        caller=delete_data_pool_item.__name__,
        sql=sql,
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=True,
        client=pg_client
    )

    log(conversion, f'[{delete_data_pool_item.__name__}] Deleted #{data_pool_id} from data-pool')

    if original_session_replication_role and result.client:
        enable_triggers(conversion, result.client, original_session_replication_role)


def enable_triggers(
    conversion: Conversion,
    pg_client: PooledDedicatedDBConnection,
    original_session_replication_role: str
) -> None:
    """
    Enables all triggers and rules for current database session.
    !!!DO NOT release the client, it will be released after current data-chunk deletion.
    """
    DBAccess.query(
        conversion=conversion,
        caller=enable_triggers.__name__,
        sql=f'SET session_replication_role = {original_session_replication_role};',
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=False,
        client=pg_client
    )


def disable_triggers(
    conversion: Conversion,
    pg_client: PooledDedicatedDBConnection
) -> str:
    """
    Disables all triggers and rules for current database session.
    !!!DO NOT release the client, it will be released after current data-chunk deletion.
    """
    original_session_replication_role = 'origin'
    query_result = DBAccess.query(
        conversion=conversion,
        caller=disable_triggers.__name__,
        sql='SHOW session_replication_role;',
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=True,
        client=pg_client
    )

    if query_result.data:
        original_session_replication_role = query_result.data[0]['session_replication_role']

    DBAccess.query(
        conversion=conversion,
        caller=disable_triggers.__name__,
        sql='SET session_replication_role = replica;',
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=False,
        client=query_result.client
    )

    return original_session_replication_role


def data_transferred(conversion: Conversion, data_pool_id: int) -> bool:
    """
    Enforces consistency before processing a chunk of data.
    Ensures there are no data duplications.
    In case of normal execution - it is a good practice.
    In case of rerunning migration after unexpected failure - it is absolutely mandatory.
    """
    data_pool_table_name = MigrationStateManager.get_data_pool_table_name(conversion)
    result = DBAccess.query(
        conversion=conversion,
        caller=data_transferred.__name__,
        sql=f'SELECT metadata AS metadata FROM {data_pool_table_name} WHERE id = {data_pool_id};',
        vendor=DBVendor.PG,
        process_exit_on_error=True,
        should_return_client=True
    )

    result_data = cast(list[dict[str, Any]], result.data)
    metadata = result_data[0]['metadata']
    table_name = metadata['_tableName']
    target_table_name = f'"{conversion.schema}"."{table_name}"'

    probe = DBAccess.query(
        conversion=conversion,
        caller=data_transferred.__name__,
        sql=f'SELECT * FROM {target_table_name} LIMIT 1 OFFSET 0;',
        vendor=DBVendor.PG,
        process_exit_on_error=True,
        should_return_client=False,
        client=result.client
    )

    probe_data = cast(list[dict[str, Any]], probe.data)
    return len(probe_data) != 0
