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
