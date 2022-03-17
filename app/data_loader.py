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
from typing import Optional, Any, cast
from concurrent.futures import ProcessPoolExecutor, as_completed

from dbutils.pooled_db import PooledDedicatedDBConnection

import app.db_access as DBAccess
import app.migration_state_manager as MigrationStateManager
import app.extra_config_processor as ExtraConfigProcessor
from app.db_vendor import DBVendor
from app.fs_ops import log, generate_error
from app.conversion import Conversion
from app.constraints_processor import process_constraints_per_table
from app.utils import track_memory, get_cpu_count


@track_memory
def send_data(conversion: Conversion) -> None:
    """
    Sends the data to the loader processes.
    """
    if len(conversion.data_pool) == 0:
        return

    params_list: list[list[dict[str, Any]]] = [
        [conversion.config, meta]
        for meta in conversion.data_pool
    ]

    number_of_workers = min(
        conversion.max_each_db_connection_pool_size,
        len(conversion.data_pool),
        get_cpu_count(),
        conversion.number_of_loader_processes,
    )

    with ProcessPoolExecutor(max_workers=number_of_workers) as executor:
        futures = [executor.submit(_load, *params) for params in params_list]

        for future in as_completed(futures):
            try:
                just_populated_table_name = future.result()
                process_constraints_per_table(conversion, just_populated_table_name)
            except Exception as e:
                generate_error(conversion, repr(e))

    MigrationStateManager.set(conversion, 'per_table_constraints_loaded')


def _load(config: dict, data_pool_item: dict) -> str:
    """
    Loads the data into target table.
    Notice, this function runs in separate process.
    """
    conversion = Conversion(config)
    table_name = data_pool_item['table_name']
    msg = f'[{_load.__name__}] Loading the data into "{conversion.schema}"."{table_name}" table...'
    log(conversion, msg)
    is_recovery_mode = data_transferred(conversion, data_pool_item['_id'])

    if is_recovery_mode:
        pg_client = DBAccess.get_db_client(conversion, DBVendor.PG)
        delete_data_pool_item(conversion, data_pool_item['_id'], pg_client)
        return cast(str, data_pool_item['table_name'])

    return cast(str, populate_table_worker(
        conversion=conversion,
        table_name=data_pool_item['table_name'],
        select_field_list=data_pool_item['select_field_list'],
        rows_cnt=data_pool_item['rows_cnt'],
        data_pool_id=data_pool_item['_id'],
    ))


@track_memory
def populate_table_worker(
    conversion: Conversion,
    table_name: str,
    select_field_list: str,
    rows_cnt: int,
    data_pool_id: int,
) -> str:
    """
    Inserts given table's data using "PostgreSQL COPY".
    Returns a name of just loaded table.
    """
    original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, True)
    sql = f'SELECT {select_field_list} FROM `{original_table_name}`;'
    original_session_replication_role = None
    text_stream, pg_cursor, pg_client, mysql_client, mysql_cursor = None, None, None, None, None

    try:
        mysql_client = DBAccess.get_mysql_unbuffered_client(conversion)
        mysql_cursor = mysql_client.cursor()
        mysql_cursor.execute(sql)  # Notice, no significant memory allocations happen until mysql_cursor.fetchmany call.
        number_of_inserted_rows = 0

        # Notice:
        # 1.
        # We get maximal performance boost with only one write-worker.
        # 2.
        # This way first process (reader-process) constantly retrieves data from source db
        # and submits it to the second process (writer-process or write-worker), which inserts the data into target db.
        # 3.
        # Retrieval rate of the reader-process is roughly equal to insertion rate of the writer-process (write-worker),
        # hence only a small amount of data is buffered in executor's "Call Queue".
        # It allows to keep memory consumption low.
        # 4.
        # !!!No need to increase number of write-workers, since write-worker always writes data to the same table,
        # which means it writes data to the same location on disk,
        # which means when one write-worker writes data - other write-workers wait.
        # While other write-workers wait, reader-process continues submitting data from source db.
        # This data is buffered in executor's "Call Queue" - hence memory consumption gets higher without
        # significant performance increase.
        with ProcessPoolExecutor(max_workers=1) as executor:
            batch_size = 30000
            buffered_batches = 0
            max_buffered_batches = 3

            while True:
                # Notice:
                # 1. Additional memory allocation happens below.
                # 2. This "while True" loop DOES NOT aggregate memory, so memory consumption level remains steady.
                # 3. The data retrieved by "mysql_cursor.fetchmany" is eventually copied to the write-worker.
                # 4. Batch size of 30000 rows seems reasonable for maximal speed without memory spikes.
                # 5. !!!Significant increase of batch size DOES NOT lead to noticeable performance improvement.
                batch = mysql_cursor.fetchmany(batch_size)
                buffered_batches += 1
                rows_to_insert = len(batch)

                if rows_to_insert == 0:
                    # No more records to insert.
                    break

                rows = '\n'.join(['\t'.join(record) for record in batch])
                text_stream = io.StringIO()
                text_stream.write(rows)
                text_stream.seek(0)

                _arrange_and_load_batch_params = [
                    conversion.config,
                    table_name,
                    text_stream,
                    rows_cnt,
                    rows_to_insert,
                    number_of_inserted_rows,
                ]

                future = executor.submit(_arrange_and_load_batch, *_arrange_and_load_batch_params)

                # !!!Below, use only "is None" comparison, and not "if not..."
                # _arrange_and_load_batch always returns string (which may be empty),
                # while the original value of "original_session_replication_role" is None.
                # This way it is possible to distinguish between the first batch and the rest.
                if original_session_replication_role is None or buffered_batches > max_buffered_batches:
                    for completed_future in as_completed([future]):
                        try:
                            original_session_replication_role = completed_future.result()
                        except Exception as ex:
                            generate_error(conversion, repr(ex))
                        finally:
                            buffered_batches -= 1
    except Exception as e:
        msg = 'Data retrieved by following MySQL query has been rejected by the target PostgreSQL server.'
        error_message = f'[{populate_table_worker.__name__}] {e}\n\t--[{populate_table_worker.__name__}] {msg}'
        generate_error(conversion, error_message, sql)
    finally:
        for resource in (text_stream, pg_cursor, mysql_cursor, mysql_client):
            if resource:
                resource.close()

        delete_data_pool_item(
            conversion=conversion,
            data_pool_id=data_pool_id,
            pg_client=cast(PooledDedicatedDBConnection, pg_client),
            original_session_replication_role=original_session_replication_role,
        )

        return table_name


def _arrange_and_load_batch(
    conversion_config: dict,
    table_name: str,
    text_stream: io.StringIO,
    rows_cnt: int,
    rows_to_insert: int,
    number_of_inserted_rows: int
) -> str:
    """
    Formats a batch of data as csv, and passes it to PG COPY.
    Notice, this function runs in separate process.
    """
    conversion = Conversion(conversion_config)
    original_session_replication_role = ''  # !!!MUST be left as an empty string.

    try:
        pg_client = DBAccess.get_db_client(conversion, DBVendor.PG)
        pg_cursor = pg_client.cursor()

        if conversion.should_migrate_only_data():
            original_session_replication_role = disable_triggers(conversion, pg_client)

        sql_copy = (f'COPY "{conversion.schema}"."{table_name}" FROM STDIN'
                    f' WITH(FORMAT text, DELIMITER \'\t\', ENCODING \'{conversion.target_con_string["charset"]}\');')

        pg_cursor.copy_expert(sql=sql_copy, file=text_stream)
        pg_client.commit()

        number_of_inserted_rows += rows_to_insert
        msg = (f'[{_arrange_and_load_batch.__name__}] For now inserted: {number_of_inserted_rows} rows, '
               f'Total rows to insert into "{conversion.schema}"."{table_name}": {rows_cnt}')

        log(conversion, msg)
    except Exception as e:
        error_message = f'[{_arrange_and_load_batch.__name__}] {type(e).__name__} {repr(e)}'
        generate_error(conversion, error_message)
    finally:
        return original_session_replication_role


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
    table_name = metadata['table_name']
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
