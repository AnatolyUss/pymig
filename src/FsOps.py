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

import json
import os


def create_logs_directory(conversion):
    """
    Creates logs directory.
    :param conversion: Conversion, the configuration object.
    :return: None
    """
    logs_title = 'FsOps::create_logs_directory'
    __create_directory(conversion, conversion.logs_dir_path, logs_title)
    __create_directory(conversion, conversion.not_created_views_path, logs_title)


def __create_directory(conversion, directory_path, log_title):
    """
    Creates a directory at the specified path.
    :param conversion: Conversion, the configuration object.
    :param directory_path: string
    :param log_title: string
    :return: None
    """
    print('--[%s] Creating directory %s...' % (log_title, directory_path))
    try:
        os.mkdir(directory_path)
    except FileExistsError:
        print('Directory %s already exists.' % directory_path)


def write_to_file(path, mode, message):
    """
    Write a message to specified file.
    :param path: string, a path to appropriate log file.
    :param mode: string, a mode in which the file will be used.
    :param message: string, a message to be logged.
    :return: None
    """
    with open(path, mode) as file:
        file.write(message)


def generate_error(conversion, message, sql=''):
    """
    Writes a detailed error message to the "/errors-only.log" file.
    :param conversion: Conversion, the configuration object.
    :param message: string, an error message.
    :param sql: string, SQL query that caused an error.
    :return: None
    """
    message += '\n\n\tSQL: %s\n\n' % sql if len(sql) != 0 else ''
    log(conversion, message)
    write_to_file(conversion.error_logs_path, 'a', message)


def log(conversion, message, table_log_path=None):
    """
    Outputs given log.
    Writes given log to the "/all.log" file.
    If necessary, writes given log to the "/{tableName}.log" file.
    :param conversion: Conversion, the configuration object.
    :param message: string, string to be logged.
    :param table_log_path: string,  a path to log file of some particular table.
    :return: None
    """
    print(message)
    write_to_file(conversion.all_logs_path, 'a', message)

    if table_log_path:
        write_to_file(table_log_path, 'a', message)


def read_config(base_dir, config_file_name='config.json'):
    """
    Reads the main configuration file and returns its contents as a dictionary.
    :param base_dir:  string, app's base directory.
    :param config_file_name: string, configuration file name.
    :return: dictionary, configuration as a dictionary.
    """
    config = None
    with open(os.path.join(base_dir, 'config', config_file_name), 'r') as file:
        config_str = file.read()
        config = json.loads(config_str)
        config['logs_dir_path'] = os.path.join(base_dir, 'logs_directory')
        config['data_types_map_addr'] = os.path.join(base_dir, 'config', 'data_types_map.json')

    return config


def read_extra_config(config, base_dir):
    """
    Reads the extra configuration file, if necessary.
    :param config: dictionary, contains the main configuration.
    :param base_dir: string, app's base directory.
    :return: dictionary, configuration, including extra, as a dictionary.
    """
    if not config['enable_extra_config']:
        config['extra_config'] = None
        return config

    path_to_extra_config = os.path.join(base_dir, 'config', 'extra_config.json')
    with open(path_to_extra_config, 'r') as file:
        extra_config_str = file.read()
        config['extra_config'] = json.loads(extra_config_str)

    return config


def read_data_types_map(conversion):
    """
    Reads "./config/data_types_map.json" and converts its json content to js object.
    :param conversion: Conversion, Pymig configuration object.
    :return: None
    """
    with open(conversion.data_types_map_addr, 'r') as file:
        contents = file.read()
        conversion.data_types_map = json.loads(contents)
