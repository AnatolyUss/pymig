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
import DBVendors
from FsOps import FsOps
from Conversion import Conversion
from DBAccess import DBAccess
from ConcurrencyManager import ConcurrencyManager
from MigrationStateManager import MigrationStateManager


class DataPipeManager:
    @staticmethod
    def send_data(conversion):
        """
        Runs the data pipe.
        :param conversion: Conversion
        :return: None
        """
        if DataPipeManager.data_pool_processed(conversion):
            return

        params_list = list(map(lambda meta: [conversion.config, meta], conversion.data_pool))
        ConcurrencyManager.run_data_pipe(conversion, DataPipeManager.load, params_list)

    @staticmethod
    def load(config, data_pool_item):
        """
        Loads the data using separate process.
        :param config: dict
        :param data_pool_item: dict
        :return: None
        """
        log_title = 'DataPipeManager::load'
        conversion = Conversion(config)
        msg = '\t--[%s] Loading the data into "%s"."%s" table...' \
              % (log_title, conversion.schema, data_pool_item['_tableName'])

        FsOps.log(conversion, msg)
        is_recovery_mode = DataPipeManager.data_transferred(conversion, data_pool_item['_id'])

        if not is_recovery_mode:
            # TODO: implement normal migration flow.
            pass

        # TODO: delete data-pool item.

    @staticmethod
    def data_transferred(conversion, data_pool_id):
        """
        Enforces consistency before processing a chunk of data.
        Ensures there are no data duplications.
        In case of normal execution - it is a good practice.
        In case of rerunning Pymig after unexpected failure - it is absolutely mandatory.
        :param conversion: Conversion
        :param data_pool_id: int
        :return: bool
        """
        log_title = 'DataPipeManager::dataTransferred'
        data_pool_table_name = MigrationStateManager.get_data_pool_table_name(conversion)
        sql_get_metadata = 'SELECT metadata AS metadata FROM %s WHERE id = %d;' % (data_pool_table_name, data_pool_id)
        result = DBAccess.query(conversion, log_title, sql_get_metadata, DBVendors.PG, True, True)
        metadata = json.loads(result.data[0]['metadata'])
        target_table_name = '"%s"."%s"' % (conversion.schema, metadata['_tableName'])
        sql_get_first_row = 'SELECT * FROM %s LIMIT 1 OFFSET 0;' % target_table_name
        probe = DBAccess.query(conversion, log_title, sql_get_first_row, DBVendors.PG, True, False, result.client)
        return len(probe.data) != 0

    @staticmethod
    def data_pool_processed(conversion):
        """
        Checks if all data chunks were processed.
        :param conversion: Conversion
        :return: bool
        """
        return len(conversion.data_pool) == 0
