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

import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from FsOps import FsOps


class ConcurrencyManager:
    @staticmethod
    def run_in_parallel(conversion, func, params_list):
        """
        Runs in parallel given function with different parameter sets.
        :param conversion: Conversion
        :param func: function
        :param params_list: list
        :return: list
        """
        number_of_tasks = len(params_list)

        if number_of_tasks == 0:
            return []

        if number_of_tasks == 1:
            return [func(*params_list[0])]

        number_of_workers = number_of_tasks \
            if number_of_tasks < conversion.max_db_connection_pool_size else conversion.max_db_connection_pool_size

        with ThreadPoolExecutor(max_workers=number_of_workers) as executor:
            func_results = {executor.submit(func, *params): params for params in params_list}

        return ConcurrencyManager._fill_execution_results(conversion, func_results)

    @staticmethod
    def run_data_pipe(conversion, func, params_list):
        """
        Runs the data-pipe.
        :param conversion: Conversion
        :param func: function
        :param params_list: list
        :return: None
        """
        number_of_workers = min(len(conversion.data_pool), multiprocessing.cpu_count())
        func_results = []

        with ProcessPoolExecutor(max_workers=number_of_workers) as executor:
            while len(params_list) != 0:
                params = params_list.pop()
                execution_result = executor.submit(func, *params)
                func_results.append(execution_result)

        return ConcurrencyManager._fill_execution_results(conversion, func_results)

    @staticmethod
    def _fill_execution_results(conversion, func_results):
        """
        Fills parallel_execution_result list.
        :param conversion: Conversion
        :param func_results: list
        :return: list
        """
        parallel_execution_result = []

        for future in as_completed(func_results):
            try:
                data = future.result()
                parallel_execution_result.append(data)
            except Exception as e:
                FsOps.generate_error(conversion, e)

        return parallel_execution_result
