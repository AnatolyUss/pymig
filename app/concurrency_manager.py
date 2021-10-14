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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable

from app.fs_ops import generate_error
from app.conversion import Conversion


def run_concurrently(
    conversion: Conversion,
    func: Callable,
    params_list: list[Any]
) -> list[Any]:
    """
    Runs in parallel given function with different parameter sets.
    """
    number_of_tasks = len(params_list)

    if number_of_tasks == 0:
        return []

    if number_of_tasks == 1:
        return [func(*params_list[0])]

    number_of_workers = min(number_of_tasks, conversion.max_each_db_connection_pool_size)

    with ThreadPoolExecutor(max_workers=number_of_workers) as executor:
        func_results = {executor.submit(func, *params): params for params in params_list}

    parallel_execution_result = []

    for future in as_completed(func_results):
        try:
            data = future.result()
            parallel_execution_result.append(data)
        except Exception as e:
            generate_error(conversion, e)

    return parallel_execution_result
