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
import sys


cwd = os.getcwd()
sys.path.append(cwd)


import app.db_access as DBAccess
from app.fs_ops import read_config, read_extra_config, create_logs_directory, read_data_types_map, read_index_types_map
from app.boot_processor import boot, get_introduction_message
from app.schema_processor import create_schema
from app.conversion import Conversion
from app.binary_data_decoder import decode
from app.report_generator import generate_report
from app.migration_state_manager import create_state_logs_table, create_data_pool_table, read_data_pool
from app.structure_loader import load_structure
from app.constraints_processor import process_constraints
from app.data_loader import send_data


if __name__ == '__main__':
    print(get_introduction_message())
    base_dir = os.getenv('aux_dir', cwd)
    config = read_config(base_dir)
    config = read_extra_config(config, base_dir)
    conversion = Conversion(config)
    create_logs_directory(conversion)
    boot(conversion)
    read_data_types_map(conversion)
    read_index_types_map(conversion)
    create_schema(conversion)
    create_state_logs_table(conversion)
    create_data_pool_table(conversion)
    load_structure(conversion)
    read_data_pool(conversion)
    send_data(conversion)
    decode(conversion)
    process_constraints(conversion)
    DBAccess.close_connection_pools(conversion)
    conversion.shutdown_thread_pool_executor()
    generate_report(conversion, 'Migration is accomplished.')
