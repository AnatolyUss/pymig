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
from Utils import Utils
from ExtraConfigProcessor import ExtraConfigProcessor
from ConcurrencyManager import ConcurrencyManager
from DBAccess import DBAccess
from FsOps import FsOps


class VacuumProcessor:
    @staticmethod
    def reclaim_storage(conversion):
        """
        Reclaims storage occupied by dead tuples.
        :param conversion: Conversion
        :return: None
        """
        params = [
            [conversion, table_name]
            for table_name in conversion.tables_to_migrate
            if Utils.get_index_of(
                ExtraConfigProcessor.get_table_name(conversion, table_name, should_get_original=True),
                conversion.no_vacuum
            ) == -1
        ]

        ConcurrencyManager.run_in_parallel(conversion, VacuumProcessor._reclaim_storage_from_table, params)

    @staticmethod
    def _reclaim_storage_from_table(conversion, table_name):
        """
        Reclaims storage from given table.
        :param conversion: Conversion
        :param table_name: str
        :return: None
        """
        log_title = 'VacuumProcessor::_reclaim_storage_from_table'
        full_table_name = '"%s"."%s"' % (conversion.schema, table_name)
        msg = '\t--[%s] Running "VACUUM FULL and ANALYZE" query for table %s...' % (log_title, full_table_name)
        FsOps.log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
        sql = 'VACUUM (FULL, ANALYZE) %s;' % full_table_name
        run_vacuum_result = DBAccess.query_without_transaction(
            conversion=conversion,
            caller=log_title,
            sql=sql
        )

        if not run_vacuum_result.error:
            msg_success = '\t--[%s] Table %s is VACUUMed...' % (log_title, full_table_name)
            FsOps.log(conversion, msg_success, conversion.dic_tables[table_name].table_log_path)
