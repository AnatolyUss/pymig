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

import os
from FsOps import FsOps
from BootProcessor import BootProcessor
from SchemaProcessor import SchemaProcessor
from Conversion import Conversion
from MigrationStateManager import MigrationStateManager
from StructureLoader import StructureLoader
from ReportGenerator import ReportGenerator
from DataPipeManager import DataPipeManager

if __name__ == '__main__':
    print(BootProcessor.get_introduction_message())
    BASE_DIR = os.getcwd()
    config = FsOps.read_config(BASE_DIR)
    config = FsOps.read_extra_config(config, BASE_DIR)
    conversion = Conversion(config)
    FsOps.create_logs_directory(conversion)
    BootProcessor.boot(conversion)
    FsOps.read_data_types_map(conversion)
    SchemaProcessor.create_schema(conversion)
    MigrationStateManager.create_state_logs_table(conversion)
    MigrationStateManager.create_data_pool_table(conversion)
    StructureLoader.load_structure(conversion)
    MigrationStateManager.read_data_pool(conversion)
    DataPipeManager.send_data(conversion)
    ReportGenerator.generate_report(conversion, 'PYMIG migration is accomplished.')
