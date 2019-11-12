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

import sys
import pymysql
import psycopg2
from psycopg2.extras import RealDictCursor
from DBUtils.PooledDB import PooledDB
from DBAccessQueryResult import DBAccessQueryResult
from FsOps import FsOps
import DBVendors


class DBAccess:
    @staticmethod
    def __ensure_mysql_connection(conversion):
        """
        Ensures MySQL connection pool existence.
        :param conversion: Conversion, Pymig configuration object.
        :return: None
        """
        if not conversion.mysql:
            conversion.mysql = DBAccess.__get_pooled_db(conversion, DBVendors.MYSQL, conversion.source_con_string)

    @staticmethod
    def __ensure_pg_connection(conversion):
        """
        Ensures PostgreSQL connection pool existence.
        :param conversion: Conversion, Pymig configuration object.
        :return: None
        """
        if not conversion.pg:
            conversion.pg = DBAccess.__get_pooled_db(conversion, DBVendors.PG, conversion.target_con_string)

    @staticmethod
    def __get_pooled_db(conversion, db_vendor, db_connection_details):
        """
        Creates DBUtils.PooledDB instance.
        :param conversion: Conversion, Pymig configuration object.
        :param db_vendor: int
        :param db_connection_details: dict
        :return: DBUtils.PooledDB instance
        """
        if db_vendor == DBVendors.MYSQL:
            return PooledDB(creator=pymysql,
                            host=db_connection_details['host'],
                            user=db_connection_details['user'],
                            password=db_connection_details['password'],
                            database=db_connection_details['database'],
                            charset=db_connection_details['charset'],
                            blocking=False,
                            cursorclass=pymysql.cursors.DictCursor,
                            maxcached=conversion.max_db_connection_pool_size,
                            maxshared=conversion.max_db_connection_pool_size,
                            maxconnections=conversion.max_db_connection_pool_size)
        elif db_vendor == DBVendors.PG:
            return PooledDB(creator=psycopg2,
                            host=db_connection_details['host'],
                            user=db_connection_details['user'],
                            password=db_connection_details['password'],
                            database=db_connection_details['database'],
                            client_encoding=db_connection_details['charset'],
                            blocking=False,
                            maxcached=conversion.max_db_connection_pool_size,
                            maxshared=conversion.max_db_connection_pool_size,
                            maxconnections=conversion.max_db_connection_pool_size)
        else:
            FsOps.generate_error(conversion, '\t --[DBAccess::__get_pooled_db] unknown db_vendor %s.' % db_vendor)
            sys.exit(-1)

    @staticmethod
    def get_db_client(conversion, db_vendor):
        """
        Obtains PooledSharedDBConnection instance.
        :param conversion: Conversion, Pymig configuration object.
        :param db_vendor: int, mimics enum, representing database vendors: MySQL and PostgreSQL.
        :return: PooledSharedDBConnection
        """
        if db_vendor == DBVendors.PG:
            DBAccess.__ensure_pg_connection(conversion)
            return conversion.pg.connection(shareable=True)
        elif db_vendor == DBVendors.MYSQL:
            DBAccess.__ensure_mysql_connection(conversion)
            return conversion.mysql.connection(shareable=True)
        else:
            FsOps.generate_error(conversion, '\t --[DBAccess::get_db_client] unknown db_vendor %s.' % db_vendor)
            sys.exit(-1)

    @staticmethod
    def release_db_client(conversion, client):
        """
        Releases MySQL or PostgreSQL connection back to appropriate pool.
        :param conversion: Conversion, Pymig configuration object.
        :param client: PooledSharedDBConnection
        :return: None
        """
        try:
            client.close()
            client = None
        except Exception as e:
            FsOps.generate_error(conversion, '\t--[DBAccess::release_db_client] %s' % e)

    @staticmethod
    def __release_db_client_if_necessary(conversion, client, should_hold_client):
        """
        Checks if there are no more queries to be sent using current client.
        In such case the client should be released.
        :param conversion: Conversion, Pymig configuration object.
        :param client: PooledSharedDBConnection
        :param should_hold_client: boolean
        :return: None
        """
        if not should_hold_client:
            DBAccess.release_db_client(conversion, client)

    @staticmethod
    def query(conversion, caller, sql, vendor, process_exit_on_error, should_return_client, client=None, bindings=None):
        """
        Sends given SQL query to specified DB.
        Performs appropriate actions (requesting/releasing client) against target connections pool.
        :param conversion: Conversion, Pymig configuration object.
        :param caller: string, a name of the function, that has just sent the query for execution.
        :param sql: string
        :param vendor: int, mimics enum, representing database vendors: MySQL and PostgreSQL.
        :param process_exit_on_error: boolean, determines should the app terminate on error.
        :param should_return_client: boolean, determines should the client be returned.
        :param client: PooledSharedDBConnection | None
        :param bindings: tuple | None
        :return: DBAccessQueryResult
        """
        cursor, data, error = None, None, None
        try:
            if not client:
                # Checks if there is an available client.
                # If the client is not available then it must be requested from the connection pool.
                client = DBAccess.get_db_client(conversion, DBVendors.PG) \
                    if vendor == DBVendors.PG else DBAccess.get_db_client(conversion, DBVendors.MYSQL)

            cursor = client.cursor(cursor_factory=RealDictCursor) if vendor == DBVendors.PG else client.cursor()

            if bindings:
                cursor.execute(sql, bindings)
            else:
                cursor.execute(sql)

            client.commit()
            data = cursor.fetchall()
        except psycopg2.ProgrammingError:
            data = []
        except Exception as e:
            error = e
            FsOps.generate_error(conversion, '\t--[%s] %s' % (caller, e), sql)
            if process_exit_on_error:
                sys.exit(-1)
        finally:
            if cursor:
                cursor.close()

            DBAccess.__release_db_client_if_necessary(conversion, client, should_return_client)
            return DBAccessQueryResult(client, data, error)
