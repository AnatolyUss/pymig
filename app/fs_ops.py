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
import json
import os
from typing import Optional, cast

from app.conversion import Conversion


def create_logs_directory(conversion: Conversion) -> None:
    """
    Creates logs directory.
    """
    _create_directory(conversion.logs_dir_path, create_logs_directory.__name__)
    _create_directory(conversion.not_created_views_path, create_logs_directory.__name__)


def _create_directory(directory_path: str, log_title: str) -> None:
    """
    Creates a directory at the specified path.
    """
    print(f'\t--[{log_title}] Creating directory {directory_path}...')
    try:
        os.mkdir(directory_path)
    except FileExistsError:
        print(f'\t--[{log_title}] Directory {directory_path} already exists.')
    except Exception as e:
        print(f'\t--[{log_title}] Failed to create directory {directory_path} due to {repr(e)}')


def write_to_file(path: str, mode: str, message: str) -> None:
    """
    Write a message to specified file.
    """
    with open(path, mode) as file:
        file.write(message)


def generate_error(conversion: Conversion, message: str, sql: str = '') -> None:
    """
    Writes a detailed error message to the "/errors-only.log" file.
    """
    message += f'\n\n\tSQL: {sql}\n\n' if sql else ''
    log(conversion, message)
    write_to_file(conversion.error_logs_path, 'a', message)


def log(conversion: Conversion, message: str, table_log_path: Optional[str] = None) -> None:
    """
    Outputs given log.
    Writes given log to the "/all.log" file.
    If necessary, writes given log to the "/{tableName}.log" file.
    """
    print(message)
    write_to_file(conversion.all_logs_path, 'a', f'\n{message}')

    if table_log_path:
        write_to_file(table_log_path, 'a', f'\n{message}')


def read_config(base_dir: str, config_file_name: str = 'config.json') -> dict:
    """
    Reads the main configuration file and returns its contents as a dictionary.
    """
    with open(os.path.join(base_dir, 'config', config_file_name), 'r') as file:
        config_str = file.read()
        config = json.loads(config_str)
        config['logs_dir_path'] = os.path.join(base_dir, 'logs_directory')
        config['data_types_map_addr'] = os.path.join(base_dir, 'config', 'data_types_map.json')
        return cast(dict, config)


def read_extra_config(config: dict, base_dir: str) -> dict:
    """
    Reads the extra configuration file, if necessary.
    """
    if not config['enable_extra_config']:
        config['extra_config'] = None
        return config

    path_to_extra_config = os.path.join(base_dir, 'config', 'extra_config.json')

    with open(path_to_extra_config, 'r') as file:
        extra_config_str = file.read()
        config['extra_config'] = json.loads(extra_config_str)

    return config


def read_data_types_map(conversion: Conversion) -> None:
    """
    Reads "./config/data_types_map.json" and converts its json content to js object.
    """
    with open(conversion.data_types_map_addr, 'r') as file:
        contents = file.read()
        conversion.data_types_map = json.loads(contents)
