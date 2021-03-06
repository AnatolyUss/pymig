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
import json
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
        :param table_name: str
        :param have_data_chunks_processed: bool
        :return: None
        """
        if have_data_chunks_processed:
            return

        log_title = 'DataChunksProcessor::prepare_data_chunks'
        log_path = conversion.dic_tables[table_name].table_log_path
        original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, True)

        select_field_list = ColumnsDataArranger.arrange_columns_data(
            conversion.dic_tables[table_name].table_columns,
            conversion.mysql_version
        )

        rows_cnt_result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql='SELECT COUNT(1) AS rows_count FROM `%s`;' % original_table_name,
            vendor=DBVendors.MYSQL,
            process_exit_on_error=True,
            should_return_client=False
        )

        rows_cnt = int(rows_cnt_result.data[0]['rows_count'])
        msg = '\t--[%s] Total rows to insert into "%s"."%s": %d' % (log_title, conversion.schema, table_name, rows_cnt)
        FsOps.log(conversion, msg, log_path)

        meta = {
            '_tableName': table_name,
            '_selectFieldList': select_field_list,
            '_rowsCnt': rows_cnt,
        }

        sql = 'INSERT INTO "{0}"."data_pool_{0}{1}"("metadata") VALUES (%(meta)s);' \
            .format(conversion.schema, conversion.mysql_db_name)

        DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql,
            vendor=DBVendors.PG,
            process_exit_on_error=True,
            should_return_client=False,
            client=None,
            bindings={'meta': json.dumps(meta)}
        )
