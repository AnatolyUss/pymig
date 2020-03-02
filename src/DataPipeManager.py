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
from DataLoader import DataLoader
from ConcurrencyManager import ConcurrencyManager


class DataPipeManager:
    @staticmethod
    def send_data(conversion):
        """
        Runs the data pipe.
        :param conversion: Conversion
        :return: None
        """
        if len(conversion.data_pool) == 0:
            return

        params_list = [[conversion.config, meta] for meta in conversion.data_pool]
        ConcurrencyManager.run_data_pipe(conversion, DataLoader.load, params_list)
