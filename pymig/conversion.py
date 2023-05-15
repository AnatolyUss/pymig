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
import os
from typing import cast, Optional, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

from dbutils.pooled_db import PooledDB

from pymig.table import Table


class Conversion:
    config: dict
    source_con_string: dict
    target_con_string: dict
    mysql: Optional[PooledDB]
    pg: Optional[PooledDB]
    logs_dir_path: str
    data_types_map: dict
    data_types_map_addr: str
    all_logs_path: str
    error_logs_path: str
    not_created_views_path: str
    exclude_tables: list[str]
    include_tables: list[str]
    time_begin: Optional[float]
    mysql_version: str
    extra_config: dict
    tables_to_migrate: list[str]
    views_to_migrate: list[str]
    data_pool: list[dict]
    dic_tables: dict[str, Table]
    mysql_db_name: str
    schema: str
    max_each_db_connection_pool_size: int
    runs_in_test_mode: bool
    remove_test_resources: bool
    migrate_only_data: bool
    delimiter: str
    debug: bool
    number_of_loader_processes: int
    index_types_map: dict[str, str]
    index_types_map_addr: str
    _thread_pool_executor: ThreadPoolExecutor

    __slots__ = (
        'config', 'source_con_string', 'target_con_string', 'mysql', 'pg', 'logs_dir_path', 'data_types_map',
        'data_types_map_addr', 'all_logs_path', 'error_logs_path', 'not_created_views_path', 'exclude_tables',
        'include_tables', 'time_begin', 'mysql_version', 'extra_config', 'tables_to_migrate',
        'views_to_migrate', 'data_pool', 'dic_tables', 'mysql_db_name', 'schema', 'max_each_db_connection_pool_size',
        'runs_in_test_mode', 'remove_test_resources', 'migrate_only_data', 'delimiter', 'debug',
        'number_of_loader_processes', '_thread_pool_executor', 'index_types_map', 'index_types_map_addr',
    )

    def __init__(self, config: dict):
        """
        Conversion class constructor.
        """
        self.config = config
        self.source_con_string = self.config['source']
        self.target_con_string = self.config['target']
        self.mysql = None
        self.pg = None
        self.logs_dir_path = self.config['logs_dir_path']
        self.index_types_map = {}
        self.index_types_map_addr = self.config['index_types_map_addr']
        self.data_types_map = {}
        self.data_types_map_addr = self.config['data_types_map_addr']
        self.all_logs_path = os.path.join(self.logs_dir_path, 'all.log')
        self.error_logs_path = os.path.join(self.logs_dir_path, 'errors-only.log')
        self.not_created_views_path = os.path.join(self.logs_dir_path, 'not_created_views')
        self.exclude_tables = self.config['exclude_tables'] if 'exclude_tables' in self.config else []
        self.include_tables = self.config['include_tables'] if 'include_tables' in self.config else []
        self.time_begin = None
        self.mysql_version = '5.6.21'
        self.extra_config = self.config['extra_config'] if 'extra_config' in self.config else {}
        self.tables_to_migrate = []
        self.views_to_migrate = []
        self.data_pool = []
        self.dic_tables = {}
        self.mysql_db_name = self.source_con_string['database']
        self.schema = self.config['schema'] if 'schema' in self.config else self.mysql_db_name

        self.max_each_db_connection_pool_size = (self.config['max_each_db_connection_pool_size']
                                                 if 'max_each_db_connection_pool_size' in self.config
                                                 else 20)

        self.runs_in_test_mode = False

        self.remove_test_resources = (self.config['remove_test_resources']
                                      if 'remove_test_resources' in self.config
                                      else True)

        self.migrate_only_data = self.config['migrate_only_data'] if 'migrate_only_data' in self.config else False
        self.delimiter = self.config['delimiter'] if 'delimiter' in self.config else ','
        self.debug = self.config['debug'] if 'debug' in self.config else False
        self.number_of_loader_processes = self._parse_number_of_loader_processes()

        # Notice, all the threads in this pool will execute io-bound tasks only (sending queries to dbs asynchronously).
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=self.max_each_db_connection_pool_size)

    def _parse_number_of_loader_processes(self) -> int:
        """
        Parses the 'number_of_simultaneously_running_loader_processes' config parameter,
        and returns its integer representation.
        """
        default_number_of_loader_processes = 4
        number_of_loader_processes = self.config['number_of_simultaneously_running_loader_processes']

        if not number_of_loader_processes:
            return default_number_of_loader_processes

        if isinstance(number_of_loader_processes, str):
            return (default_number_of_loader_processes
                    if number_of_loader_processes == 'DEFAULT'
                    else int(number_of_loader_processes))

        return cast(int, number_of_loader_processes)

    def shutdown_thread_pool_executor(self) -> None:
        """
        Signals the executor that it should free any resources that it is using
        when all currently pending futures are done executing.
        """
        self._thread_pool_executor.shutdown(wait=True, cancel_futures=False)

    def run_concurrently(self, func: Callable, params_list: list[Any]) -> list[Any]:
        """
        Runs given function asynchronously with different parameter-sets.
        """
        from pymig.fs_ops import generate_error
        number_of_tasks = len(params_list)

        if number_of_tasks == 0:
            return []

        if number_of_tasks == 1:
            return [func(*params_list[0])]

        parallel_execution_result = []
        futures = [
            self._thread_pool_executor.submit(func, *params)
            for params in params_list
        ]

        for future in as_completed(futures):
            try:
                data = future.result()
                parallel_execution_result.append(data)
            except Exception as e:
                generate_error(self, repr(e))

        return parallel_execution_result

    def should_migrate_only_data(self) -> bool:
        """
        Checks if there are actions to take other than data migration.
        """
        return self.migrate_only_data
