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
import sys
from typing import Optional, Union

import pymysql
from pymysql.connections import Connection as PymysqlConnection
import psycopg2
from psycopg2.extras import RealDictCursor
from dbutils.pooled_db import PooledDB, PooledDedicatedDBConnection

from app.db_access_query_result import DBAccessQueryResult
from app.fs_ops import generate_error
from app.db_vendor import DBVendor
from app.conversion import Conversion


def _ensure_mysql_connection(conversion: Conversion) -> None:
    """
    Ensures MySQL connection pool existence.
    """
    if not conversion.mysql:
        conversion.mysql = _get_pooled_db(conversion, DBVendor.MYSQL, conversion.source_con_string)


def _ensure_pg_connection(conversion: Conversion) -> None:
    """
    Ensures PostgreSQL connection pool existence.
    """
    if not conversion.pg:
        conversion.pg = _get_pooled_db(conversion, DBVendor.PG, conversion.target_con_string)


def _get_pooled_db(
    conversion: Conversion,
    db_vendor: DBVendor,
    db_connection_details: dict
) -> PooledDB:
    """
    Creates DBUtils.PooledDB instance.
    """
    connection_details = {
        # Basic connection details.
        'port': db_connection_details['port'],
        'host': db_connection_details['host'],
        'user': db_connection_details['user'],
        'password': db_connection_details['password'],
        'database': db_connection_details['database'],

        # Determines behavior when exceeding the maximum.
        # If True, blocks and waits until the number of connections decreases, otherwise, by default, raises exception.
        'blocking': True,

        # Maximum number of idle connections in the pool.
        # Default value of 0 or None means unlimited pool size.
        'maxcached': conversion.max_each_db_connection_pool_size,

        # Maximum number of allowed connections.
        # Default value of 0 or None means any number of connections.
        'maxconnections': conversion.max_each_db_connection_pool_size,
    }

    if db_vendor == DBVendor.MYSQL:
        connection_details.update({
            'creator': pymysql,
            'cursorclass': pymysql.cursors.DictCursor,
            'charset': db_connection_details['charset'],
        })
    elif db_vendor == DBVendor.PG:
        connection_details.update({
            'creator': psycopg2,
            'client_encoding': db_connection_details['charset'],
        })
    else:
        generate_error(conversion, f'[{_get_pooled_db.__name__}] unknown db_vendor {db_vendor.value}')
        sys.exit(1)

    return PooledDB(**connection_details)


def close_connection_pools(conversion: Conversion) -> None:
    """
    Closes both connection-pools.
    """
    for pool in (conversion.mysql, conversion.pg):
        if pool:
            try:
                pool.close()
            except Exception as e:
                generate_error(conversion, f'[{close_connection_pools.__name__}] {repr(e)}')


def get_mysql_unbuffered_client(conversion: Conversion) -> PymysqlConnection:
    """
    Returns MySQL unbuffered client.
    """
    return pymysql.connect(
        port=conversion.source_con_string['port'],
        host=conversion.source_con_string['host'],
        user=conversion.source_con_string['user'],
        password=conversion.source_con_string['password'],
        charset=conversion.source_con_string['charset'],
        db=conversion.source_con_string['database'],
        cursorclass=pymysql.cursors.SSCursor
    )


def get_db_client(
    conversion: Conversion,
    db_vendor: DBVendor
) -> PooledDedicatedDBConnection:
    """
    Obtains PooledDedicatedDBConnection instance.
    Returned PooledDedicatedDBConnection instance is non-shareable, dedicated connection.
    """
    if db_vendor == DBVendor.PG:
        _ensure_pg_connection(conversion)
        return conversion.pg.connection(shareable=False)  # type: ignore
    elif db_vendor == DBVendor.MYSQL:
        _ensure_mysql_connection(conversion)
        return conversion.mysql.connection(shareable=False)  # type: ignore
    else:
        generate_error(conversion, f'[{get_db_client.__name__}] unexpected db_vendor {db_vendor.value}')
        sys.exit(1)


def release_db_client(
    conversion: Conversion,
    client: PooledDedicatedDBConnection
) -> None:
    """
    Releases MySQL or PostgreSQL connection back to appropriate pool.
    """
    if client:
        try:
            client.close()
            client = None
        except Exception as e:
            generate_error(conversion, f'[{release_db_client.__name__}] {repr(e)}')


def _release_db_client_if_necessary(
    conversion: Conversion,
    client: PooledDedicatedDBConnection,
    should_hold_client: bool
) -> None:
    """
    Checks if there are no more queries to be sent using current client.
    In such case the client should be released.
    """
    if not should_hold_client:
        release_db_client(conversion, client)


def query(
    conversion: Conversion,
    caller: str,
    sql: str,
    vendor: DBVendor,
    process_exit_on_error: bool,
    should_return_client: bool,
    client: Optional[PooledDedicatedDBConnection] = None,
    bindings: Optional[Union[dict, tuple]] = None,
    should_return_programming_error: bool = False
) -> DBAccessQueryResult:
    """
    Sends given SQL query to specified DB.
    Performs appropriate actions (requesting/releasing client) against target connections pool.
    """
    cursor, data, error = None, None, None

    try:
        if not client:
            # Checks if there is an available client.
            # If the client is not available then it must be requested from the connection pool.
            client = get_db_client(conversion, vendor)

        cursor = client.cursor(cursor_factory=RealDictCursor) if vendor == DBVendor.PG else client.cursor()

        if bindings:
            cursor.execute(sql, bindings)
        else:
            cursor.execute(sql)

        client.commit()
        data = cursor.fetchall()
    except psycopg2.ProgrammingError as programming_error:
        if should_return_programming_error:
            error = programming_error
            generate_error(conversion, f'[{caller}] {repr(error)}', sql)

            if process_exit_on_error:
                sys.exit(1)

        data = []
    except Exception as e:
        error = e  # type: ignore
        generate_error(conversion, f'[{caller}] {repr(e)}', sql)

        if process_exit_on_error:
            sys.exit(1)
    finally:
        if cursor:
            cursor.close()

        # Determines if the client (instance of PooledDedicatedDBConnection) should be released.
        _release_db_client_if_necessary(conversion, client, should_return_client)
        return DBAccessQueryResult(client=client, data=data, error=error)


def query_without_transaction(
    conversion: Conversion,
    caller: str,
    sql: str
) -> DBAccessQueryResult:
    """
    Sends given query to the target PostgreSQL database without wrapping it with transaction.
    """
    client, cursor, error = None, None, None

    try:
        connection_details = {
            'port': conversion.target_con_string['port'],
            'host': conversion.target_con_string['host'],
            'user': conversion.target_con_string['user'],
            'password': conversion.target_con_string['password'],
            'database': conversion.target_con_string['database'],
            'client_encoding': conversion.target_con_string['charset'],
        }

        client = psycopg2.connect(**connection_details)
        client.set_isolation_level(0)  # type: ignore
        cursor = client.cursor()
        cursor.execute(sql)  # type: ignore
    except Exception as e:
        error = e
        generate_error(conversion, f'[{caller}] {e}', sql)
    finally:
        if cursor:
            cursor.close()  # type: ignore

    release_db_client(conversion, client)
    return DBAccessQueryResult(client=None, data=None, error=error)
