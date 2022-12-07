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
from typing import Union, cast, Any

import app.extra_config_processor as ExtraConfigProcessor
import app.db_access as DBAccess
from app.conversion import Conversion
from app.fs_ops import log
from app.db_vendor import DBVendor


def create_indexes(conversion: Conversion, table_name: str) -> None:
    """
    Creates indexes, including PK, on given table.
    """
    original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, should_get_original=True)
    show_index_result = DBAccess.query(
        conversion=conversion,
        caller=create_indexes.__name__,
        sql=f'SHOW INDEX FROM `{original_table_name}`;',
        vendor=DBVendor.MYSQL,
        process_exit_on_error=False,
        should_return_client=False
    )

    if show_index_result.error:
        return

    pg_indexes: dict[str, dict[str, Union[str, int, list[str]]]] = {}
    show_index_result_data = cast(list[dict[str, Any]], show_index_result.data)

    for index in show_index_result_data:
        pg_column_name = ExtraConfigProcessor.get_column_name(
            conversion=conversion,
            original_table_name=original_table_name,
            current_column_name=index['Column_name'],
            should_get_original=False
        )

        if index['Key_name'] in pg_indexes:
            cast(list[str], pg_indexes[index['Key_name']]['column_name']).append(f'"{pg_column_name}"')
            continue

        pg_indexes[index['Key_name']] = {
            'is_unique': index['Non_unique'] == 0,
            'column_name': [f'"{pg_column_name}"'],
            'index_type': f' USING {_get_index_type(conversion=conversion, index_type=index["Index_type"])}',
        }

    params = [
        [conversion, index_name, table_name, pg_indexes, idx]
        for idx, index_name in enumerate(pg_indexes.keys())
    ]

    conversion.run_concurrently(func=_set_index, params_list=params)
    msg = f'[{create_indexes.__name__}] "{conversion.schema}"."{table_name}": PK/indices are successfully set...'
    log(conversion, msg, conversion.dic_tables[table_name].table_log_path)


def _set_index(
    conversion: Conversion,
    index_name: str,
    table_name: str,
    pg_indexes: dict[str, Union[str, bool]],
    idx: int
) -> None:
    """
    Sets appropriate index.
    """
    sql_add_index = ''

    if index_name.lower() == 'primary':
        column_names_list = cast(dict[str, Union[str, int, list[str]]], pg_indexes[index_name])
        primary_key = ','.join(cast(list[str], column_names_list['column_name']))
        sql_add_index += f'ALTER TABLE "{conversion.schema}"."{table_name}" ADD PRIMARY KEY({primary_key});'
    else:
        pg_index_name = cast(dict[str, Union[str, int, list[str]]], pg_indexes[index_name])
        column_name_list = cast(list[str], pg_index_name['column_name'])
        column_name = f"{column_name_list[0][1:-1]}{str(idx)}"
        index_type = 'UNIQUE' if pg_index_name['is_unique'] else ''
        sql_add_index += f'CREATE {index_type} INDEX'
        sql_add_index += f' "{conversion.schema}_{table_name}_{column_name}_idx"'
        sql_add_index += f' ON "{conversion.schema}"."{table_name}"'
        sql_add_index += f" {pg_index_name['index_type']} "
        sql_add_index += f"({','.join(cast(list[str], pg_index_name['column_name']))});"

    DBAccess.query(
        conversion=conversion,
        caller=_set_index.__name__,
        sql=sql_add_index,
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=False
    )


def _get_index_type(conversion: Conversion, index_type: str) -> str:
    """
    Returns PostgreSQL index type, that correlates to given MySQL index type.
    """
    return conversion.index_types_map[index_type] if index_type in conversion.index_types_map else 'BTREE'
