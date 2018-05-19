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

from os import path, pardir
import sys
import json
from ConversionSettings import ConversionSettings


class Main():
    def read_file_and_parse_json(self, path_to_file):
        """
        Reads content from specified path and tries to parse received content.
        """
        try:
            with open(path_to_file) as file:
                content = file.read()
            return json.loads(content)
        except IOError as io_error:
            print('--[Main::read_file_and_parse_json] failed to read from %s \n %s' % (path_to_file, io_error))
            sys.exit()
        except json.JSONDecodeError as decode_error:
            print('--[Main::read_file_and_parse_json] failed to parse content of %s \n %s' % (path_to_file, decode_error))
            sys.exit()

    def read_config(self, base_dir, config_file_name='config.json'):
        """
        Reads the configuration file. 
        """
        path_to_config = path.join(base_dir, 'config', config_file_name)
        config = self.read_file_and_parse_json(path_to_config)
        config['logs_dir_path'] = path.join(base_dir, 'logs_directory')
        config['data_types_map_addr'] = path.join(base_dir, 'config', 'data_types_map.json')
        return config
        
    def read_extra_config(self, config, base_dir):
        """
        Reads the extra configuration file, if necessary.
        """
        if not config['enable_extra_config']:
            config['enable_extra_config'] = None
            return config

        path_to_extra_config = path.join(base_dir, 'config', 'extra_config.json')
        extra_config = self.read_file_and_parse_json(path_to_extra_config)
        config['extra_config'] = extra_config
        return config


if __name__ == '__main__':
    base_dir = path.abspath(path.join(path.dirname(path.realpath(__file__)), pardir))
    app = Main()
    config = app.read_config(base_dir)
    config = app.read_extra_config(config, base_dir)
    conversionSettings = ConversionSettings(config)
    print(type(conversionSettings))
