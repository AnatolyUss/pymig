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
from DBUtils.PooledDB import PooledDb
from DBAccessQueryResult import DBAccessQueryResult
import FsOps
import DBVendors


class DBAccess:
    def __init__(self, conversion):
        self.conversion = conversion

    def __get_mysql_connection(self):
        """
        Ensures MySQL connection pool existence.
        :return: None
        """
        if not self.conversion['mysql']:
            try:
                self.conversion['mysql'] = PooledDb(creator=pymysql,
                                                    host=self.conversion.source_con_string.host,
                                                    user=self.conversion.source_con_string.user,
                                                    password=self.conversion.source_con_string.password,
                                                    database=self.conversion.source_con_string.database,
                                                    autocommit=True,
                                                    charset=self.conversion.source_con_string.charset,
                                                    blocking=False,
                                                    cursorclass=pymysql.cursors.DictCursor,
                                                    maxcached=self.conversion.max_db_connection_pool_size,
                                                    maxshared=self.conversion.max_db_connection_pool_size,
                                                    maxconnections=self.conversion.max_db_connection_pool_size)
            except Exception as e:
                msg = '\t--[get_mysql_connection] Cannot connect to MySQL server...\n%s' % e
                FsOps.generate_error(self.conversion, msg)
                sys.exit(-1)

    def get_mysql_client(self):
        """
        Obtains PooledSharedDBConnection instance.
        :return: PooledSharedDBConnection
        """
        self.__get_mysql_connection()
        return self.conversion['mysql'].connection(shareable=True)

    def get_pg_client(self):
        pass

    def release_db_client(self, client):
        """
        Releases MySQL or PostgreSQL connection back to appropriate pool.
        :param client:
        :return:
        """
        try:
            client.close()
        except Exception as e:
            msg = '\t--[DBAccess::release_db_client] %s' % e
            FsOps.generate_error(self.conversion, msg)

    def __release_db_client_if_necessary(self, client, should_hold_client):
        """
        Checks if there are no more queries to be sent using current client.
        In such case the client should be released.
        :param client: PooledSharedDBConnection
        :param should_hold_client: boolean
        :return: None
        """
        if not should_hold_client:
            self.release_db_client(client)

    def __query_mysql(self, caller, sql, process_exit_on_error, should_return_client, client=None, bindings=None):
        """
        Sends given SQL query to MySQL.
        :param caller: string, a name of the function, that has just sent the query for execution.
        :param sql: string
        :param process_exit_on_error: boolean, determines should the app terminate on error.
        :param should_return_client: boolean, determines should the client be returned.
        :param client: PooledSharedDBConnection
        :param bindings: tuple | None
        :return: DBAccessQueryResult
        """
        cursor = None
        try:
            cursor = client.cursor()
            if bindings:
                cursor.execute(sql, bindings)
            else:
                cursor.execute(sql)

            data = cursor.fetchall()
            self.__release_db_client_if_necessary(client, should_return_client)
            return DBAccessQueryResult(client, data, None)
        except Exception as e:
            FsOps.generate_error(self.conversion, '\t--[%s] %s' % (caller, e), sql)
            if process_exit_on_error:
                sys.exit(-1)

            return DBAccessQueryResult(client, None, e)
        finally:
            if cursor:
                cursor.close()

    def __query_pg(self, caller, sql, process_exit_on_error, should_return_client, client=None, bindings=None):
        pass

    def query(self, caller, sql, vendor, process_exit_on_error, should_return_client, client=None, bindings=None):
        """
        Sends given SQL query to specified DB.
        Performs appropriate actions (requesting/releasing client) against target connections pool.
        :param caller: string, a name of the function, that has just sent the query for execution.
        :param sql: string
        :param vendor: int, mimics enum, representing database vendors: MySQL and PostgreSQL.
        :param process_exit_on_error: boolean, determines should the app terminate on error.
        :param should_return_client: boolean, determines should the client be returned.
        :param client: PooledSharedDBConnection | None
        :param bindings: tuple | None
        :return: DBAccessQueryResult
        """
        if not client:  # Checks if there is an available client.
            try:
                # Client is undefined.
                # It must be requested from the connections pool.
                client = self.get_mysql_client() if vendor == DBVendors.MYSQL else self.get_pg_client()
            except Exception as e:
                FsOps.generate_error(self.conversion, '\t--[%s] %s' % (caller, e), sql)
                if process_exit_on_error:
                    sys.exit(-1)

                return DBAccessQueryResult(client, None, e)

        return self.__query_mysql(caller, sql, process_exit_on_error, should_return_client, client, bindings) \
            if vendor == DBVendors.MYSQL \
            else self.__query_pg(caller, sql, process_exit_on_error, should_return_client, client, bindings)
