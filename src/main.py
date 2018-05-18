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

class Main():
    def read_config(self, base_dir, config_file_name = 'config.json'):
        """
        Reads the configuration file. 
        """
        path_to_config = path.join(base_dir, 'config', config_file_name)

        try:
            with open(path_to_config, 'r') as config_file:
                config = config_file.read()
            
            dict_config = json.loads(config)
            dict_config['logs_dir_path'] = path.join(base_dir, 'logs_directory')
            dict_config['data_types_map_addr'] = path.join(base_dir, 'config', 'data_types_map.json')
            return dict_config

        except IOError as io_error:
            print('--[Main::read_config] failed to read from %s \n %s' % (path_to_config, io_error))
            sys.exit()
        except json.JSONDecodeError as decode_error:
            print('--[Main::read_config] failed to parse content of %s \n %s' % (path_to_config, decode_error))
            sys.exit()
        

if __name__ == '__main__':
    base_dir = path.abspath(path.join(path.dirname(path.realpath(__file__)), pardir))
    app = Main()
    dict_config = app.read_config(base_dir)
    print(dict_config)
