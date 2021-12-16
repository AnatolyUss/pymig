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
from typing import cast, Any

import app.db_access as DBAccess
import app.extra_config_processor as ExtraConfigProcessor
from app.db_vendor import DBVendor
from app.fs_ops import log
from app.columns_data_arranger import arrange_columns_data
from app.conversion import Conversion


def prepare_data_chunks(
    conversion: Conversion,
    table_name: str,
    have_data_chunks_processed: bool
) -> None:
    """
    Prepares a list of tables metadata.
    """
    if have_data_chunks_processed:
        return

    log_path = conversion.dic_tables[table_name].table_log_path
    original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, True)

    select_field_list = arrange_columns_data(
        table_columns=conversion.dic_tables[table_name].table_columns,
        mysql_version=conversion.mysql_version,
        encoding=conversion.encoding,
    )

    table_data_size = _get_size(conversion=conversion, original_table_name=original_table_name)
    rows_cnt = _get_rows_cnt(conversion=conversion, original_table_name=original_table_name)
    msg = (f'[{prepare_data_chunks.__name__}] Total rows to insert into'
           f' "{conversion.schema}"."{table_name}": {rows_cnt}')

    log(conversion, msg, log_path)
    meta = {
        'table_name': table_name,
        'select_field_list': select_field_list,
        'rows_cnt': rows_cnt,
        'table_data_size': table_data_size,
    }

    sql = (f'INSERT INTO "{conversion.schema}"."data_pool_{conversion.schema}{conversion.mysql_db_name}"("metadata")'
           f' VALUES (%(meta)s);')

    DBAccess.query(
        conversion=conversion,
        caller=prepare_data_chunks.__name__,
        sql=sql,
        vendor=DBVendor.PG,
        process_exit_on_error=True,
        should_return_client=False,
        client=None,
        bindings={
            'meta': json.dumps(meta)
        }
    )


def _get_rows_cnt(conversion: Conversion, original_table_name: str) -> int:
    """
    Returns an amount of records in given MySQL table.
    """
    rows_cnt_result = DBAccess.query(
        conversion=conversion,
        caller=_get_rows_cnt.__name__,
        sql=f'SELECT COUNT(1) AS rows_count FROM `{original_table_name}`;',
        vendor=DBVendor.MYSQL,
        process_exit_on_error=True,
        should_return_client=False
    )

    rows_cnt_result_data = cast(list[dict[str, Any]], rows_cnt_result.data)
    return int(rows_cnt_result_data[0]['rows_count'])


def _get_size(conversion: Conversion, original_table_name: str) -> float:
    sql = f"""
        SELECT round((data_length / 1024 / 1024), 2) AS `size_in_mb` 
        FROM information_schema.TABLES 
        WHERE table_schema = '{conversion.mysql_db_name}' AND table_name = '{original_table_name}';
    """

    result = DBAccess.query(
        conversion=conversion,
        caller=_get_size.__name__,
        sql=sql,
        vendor=DBVendor.MYSQL,
        process_exit_on_error=True,
        should_return_client=False
    )

    rows_cnt_result_data = cast(list[dict[str, Any]], result.data)
    return float(rows_cnt_result_data[0]['size_in_mb'])
