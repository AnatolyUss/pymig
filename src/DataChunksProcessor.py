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

import DBVendors
from FsOps import FsOps
from DBAccess import DBAccess
from ColumnsDataArranger import ColumnsDataArranger
from ExtraConfigProcessor import ExtraConfigProcessor


class DataChunksProcessor:
    @staticmethod
    def prepare_data_chunks(conversion, table_name, have_data_chunks_processed):
        """
        Prepares a list of tables metadata.
        :param conversion: Conversion
        :param table_name: string
        :param have_data_chunks_processed: bool
        :return: None
        """
        if have_data_chunks_processed:
            return

        log_title = 'DataChunksProcessor::prepare_data_chunks'
        log_path = conversion.dic_tables[table_name].table_log_path
        original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, True)
        select_field_list = ColumnsDataArranger.arrange_columns_data(conversion.dic_tables[table_name].table_columns,
                                                                     conversion.mysql_version)
        sql_rows_cnt = 'SELECT COUNT(1) AS rows_count FROM `%s`;' % original_table_name
        rows_cnt_result = DBAccess.query(conversion, log_title, sql_rows_cnt, DBVendors.MYSQL, True, False)
        rows_cnt = int(rows_cnt_result.data[0]['rows_count'])
        msg = '\t--[%s] Total rows to insert into "%s"."%s": %d' % (log_title, conversion.schema, table_name, rows_cnt)
        FsOps.log(conversion, msg, log_path)
        meta = '{"_tableName":"%s","_selectFieldList":"%s","_rowsCnt":%d}' % (table_name, select_field_list, rows_cnt)

        sql = 'INSERT INTO "{0}"."data_pool_{0}{1}"("metadata") VALUES (%(meta)s);'\
            .format(conversion.schema, conversion.mysql_db_name)

        DBAccess.query(conversion, log_title, sql, DBVendors.PG, True, False, None, {'meta': meta})
