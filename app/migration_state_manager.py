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
from typing import Any, cast

import app.db_access as DBAccess
from app.db_vendor import DBVendor
from app.fs_ops import log
from app.conversion import Conversion


def get_state_logs_table_name(conversion: Conversion) -> str:
    """
    Returns state-logs table name.
    """
    return f'"{conversion.schema}"."state_logs_{conversion.schema}{conversion.mysql_db_name}"'


def get_data_pool_table_name(conversion: Conversion) -> str:
    """
    Returns data-pool table name.
    """
    return f'"{conversion.schema}"."data_pool_{conversion.schema}{conversion.mysql_db_name}"'


def get(conversion: Conversion, param: str) -> bool:
    """
    Retrieves appropriate state-log.
    """
    table_name = get_state_logs_table_name(conversion)
    result = DBAccess.query(
        conversion=conversion,
        caller=get.__name__,
        sql=f'SELECT {param} FROM {table_name};',
        vendor=DBVendor.PG,
        process_exit_on_error=True,
        should_return_client=False
    )

    records = cast(list[dict[str, Any]], result.data)
    return cast(bool, records[0][param])


def set(conversion: Conversion, *states: str) -> None:
    """
    Updates the state-log.
    """
    table_name = get_state_logs_table_name(conversion)
    states_sql = ','.join([f'{state} = TRUE' for state in states])
    DBAccess.query(
        conversion=conversion,
        caller=set.__name__,
        sql=f'UPDATE {table_name} SET {states_sql};',
        vendor=DBVendor.PG,
        process_exit_on_error=True,
        should_return_client=False
    )


def create_data_pool_table(conversion: Conversion) -> None:
    """
    Creates data pool temporary table.
    """
    table_name = get_data_pool_table_name(conversion)
    DBAccess.query(
        conversion=conversion,
        caller=create_data_pool_table.__name__,
        sql=f'CREATE TABLE IF NOT EXISTS {table_name}("id" BIGSERIAL, "metadata" JSON);',
        vendor=DBVendor.PG,
        process_exit_on_error=True,
        should_return_client=False
    )

    log(conversion, f'[{create_data_pool_table.__name__}] table {table_name} is created...')


def drop_data_pool_table(conversion: Conversion) -> None:
    """
    Drops data pool temporary table.
    """
    table_name = get_data_pool_table_name(conversion)
    DBAccess.query(
        conversion=conversion,
        caller=drop_data_pool_table.__name__,
        sql=f'DROP TABLE {table_name};',
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=False
    )

    log(conversion, f'[{drop_data_pool_table.__name__}] table {table_name} is dropped...')


def read_data_pool(conversion: Conversion) -> None:
    """
    Reads temporary table ("{schema}"."data_pool_{schema + mysql_db_name}"), and generates data-pool.
    """
    table_name = get_data_pool_table_name(conversion)
    result = DBAccess.query(
        conversion=conversion,
        caller=read_data_pool.__name__,
        sql=f'SELECT id AS id, metadata AS metadata FROM {table_name};',
        vendor=DBVendor.PG,
        process_exit_on_error=True,
        should_return_client=False
    )

    results = cast(list[dict[str, Any]], result.data)

    for row in results:
        metadata = row['metadata']
        metadata['_id'] = row['id']
        conversion.data_pool.append(metadata)

    log(conversion, f'[{read_data_pool.__name__}] Data-Pool is loaded...')


def create_state_logs_table(conversion: Conversion) -> None:
    """
    Creates the "{schema}"."state_logs_{schema + mysql_db_name}" temporary table.
    """
    table_name = get_state_logs_table_name(conversion)
    sql = f'''
    CREATE TABLE IF NOT EXISTS {table_name}("tables_loaded" BOOLEAN, "per_table_constraints_loaded" BOOLEAN, 
    "foreign_keys_loaded" BOOLEAN, "views_loaded" BOOLEAN);
    '''

    result = DBAccess.query(
        conversion=conversion,
        caller=create_state_logs_table.__name__,
        sql=sql,
        vendor=DBVendor.PG,
        process_exit_on_error=True,
        should_return_client=True
    )

    result = DBAccess.query(
        conversion=conversion,
        caller=create_state_logs_table.__name__,
        sql=f'SELECT COUNT(1) AS cnt FROM {table_name};',
        vendor=DBVendor.PG,
        process_exit_on_error=True,
        should_return_client=True,
        client=result.client
    )

    msg = f'[{create_state_logs_table.__name__}] Table {table_name}'
    results = cast(list[dict[str, Any]], result.data)

    if results[0]['cnt'] == 0:
        DBAccess.query(
            conversion=conversion,
            caller=create_state_logs_table.__name__,
            sql=f'INSERT INTO {table_name} VALUES (FALSE, FALSE, FALSE, FALSE);',
            vendor=DBVendor.PG,
            process_exit_on_error=True,
            should_return_client=False,
            client=result.client
        )

        msg += ' is created'
    else:
        msg += ' already exists'

    log(conversion, msg)


def drop_state_logs_table(conversion: Conversion) -> None:
    """
    Drop state logs temporary table.
    """
    state_logs_table_name = get_state_logs_table_name(conversion)
    DBAccess.query(
        conversion=conversion,
        caller=drop_state_logs_table.__name__,
        sql=f'DROP TABLE {state_logs_table_name};',
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=False
    )
