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
import gc
from typing import Union, Callable, Any

from app.conversion import Conversion


def get_cpu_count() -> int:
    """
    Returns actual number of physical processes that current machine is able to run in parallel.
    """
    return os.cpu_count() or 1


def get_index_of(needle: str, haystack: Union[str, list[str], tuple[str]]) -> int:
    """
    Returns an index of given needle in the haystack.
    The needle can be a variable of any type.
    The haystack is either a list or a string.
    If the needle not found - returns -1.
    """
    try:
        return haystack.index(needle)
    except:
        return -1


def track_memory(func: Callable) -> Callable:
    """
    Decorator, intended to track memory used by the program.
    Notice, memory tracking works only in debug mode.
    """
    def wrap(*args: Any, **kwargs: Any) -> Any:
        conversion = (args[0] if args else None) or kwargs.get('conversion')

        if not isinstance(conversion, Conversion):
            raise ValueError(f'[{func.__name__}] First track_memory.wrap argument must be of type Conversion')

        if not conversion.debug:
            func_result = func(*args, **kwargs)
            gc.collect()
            return func_result

        import math
        import psutil
        from app.fs_ops import log

        def _get_process_memory_stats() -> tuple[int, int]:
            """
            Returns current memory stats: rss, vms.
            """
            process_memory_stats = psutil.Process().memory_info()
            return (math.ceil(process_memory_stats.rss / 1024 / 1024),
                    math.ceil(process_memory_stats.vms / 1024 / 1024))

        rss_before, vms_before = _get_process_memory_stats()
        log(conversion, f'[{func.__name__}] rss_before {rss_before} MB vms_before {vms_before} MB')
        func_result = func(*args, **kwargs)
        unreachable_objects = gc.collect()
        log(conversion, f'[{func.__name__}] gc.collect() found {unreachable_objects} unreachable objects')
        rss_after, vms_after = _get_process_memory_stats()
        log(conversion, f'[{func.__name__}] rss_after {rss_after} MB vms_after {vms_after} MB')
        return func_result
    return wrap
