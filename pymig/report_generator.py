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
import time
import math
from typing import cast

from pymig.conversion import Conversion
from pymig.fs_ops import log


def generate_report(conversion: Conversion, last_message: str) -> None:
    """
    Generates a summary report.
    """
    log_title = generate_report.__name__
    difference_sec = (time.time() - cast(float, conversion.time_begin))
    seconds = math.floor(difference_sec % 60)
    difference_sec /= 60
    minutes = math.floor(difference_sec % 60)
    hours = math.floor(difference_sec / 60)
    formatted_hours = f'0{hours}' if hours < 10 else f'{hours}'
    formatted_minutes = f'0{minutes}' if minutes < 10 else f'{minutes}'
    formatted_seconds = f'0{seconds}' if seconds < 10 else f'{seconds}'
    output = (f'[{log_title}] {last_message}\n\t--[{log_title}] '
              f'Total time: {formatted_hours}:{formatted_minutes}:{formatted_seconds}\n'
              f'\t--[{log_title}] (hours:minutes:seconds)')

    log(conversion, output)
