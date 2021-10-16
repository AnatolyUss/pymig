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
import app.db_access as DBAccess
import app.extra_config_processor as ExtraConfigProcessor
from app.table_processor import map_data_types
from app.conversion import Conversion
from app.concurrency_manager import run_concurrently
from app.utils import get_index_of
from app.db_vendor import DBVendor
from app.fs_ops import log


def process_default(conversion: Conversion, table_name: str) -> None:
    """
    Determines which columns of the given table have default value.
    Sets default values where appropriate.
    """
    msg = f'\t--[{process_default.__name__}] Determines default values for table: "{conversion.schema}"."{table_name}"'
    log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
    original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, should_get_original=True)
    pg_numeric_types = ('money', 'numeric', 'decimal', 'double precision', 'real', 'bigint', 'int', 'smallint')
    sql_reserved_values = {
        'CURRENT_DATE': 'CURRENT_DATE',
        '0000-00-00': "'-INFINITY'",
        'CURRENT_TIME': 'CURRENT_TIME',
        '00:00:00': '00:00:00',
        'CURRENT_TIMESTAMP': 'CURRENT_TIMESTAMP',
        '0000-00-00 00:00:00': "'-INFINITY'",
        'LOCALTIME': 'LOCALTIME',
        'LOCALTIMESTAMP': 'LOCALTIMESTAMP',
        'NULL': 'NULL',
        'null': 'NULL',
        'UTC_DATE': "(CURRENT_DATE AT TIME ZONE 'UTC')",
        'UTC_TIME': "(CURRENT_TIME AT TIME ZONE 'UTC')",
        'UTC_TIMESTAMP': "(NOW() AT TIME ZONE 'UTC')",
    }

    params = [
        [conversion, table_name, original_table_name, column, sql_reserved_values, pg_numeric_types]
        for column in conversion.dic_tables[table_name].table_columns
    ]

    run_concurrently(conversion, _set_default, params)


def _set_default(
    conversion: Conversion,
    table_name: str,
    original_table_name: str,
    column: dict,
    sql_reserved_values: dict[str, str],
    pg_numeric_types: tuple[str]
) -> None:
    """
    Sets default value for given column.
    """
    pg_data_type = map_data_types(conversion.data_types_map, column['Type'])
    column_name = ExtraConfigProcessor.get_column_name(
        conversion=conversion,
        original_table_name=original_table_name,
        current_column_name=column['Field'],
        should_get_original=False
    )

    sql = f'ALTER TABLE "{conversion.schema}"."{table_name}" ALTER COLUMN "{column_name}" SET DEFAULT'

    if column['Default'] in sql_reserved_values:
        sql += f" {sql_reserved_values[column['Default']]};"
    elif get_index_of(pg_data_type, pg_numeric_types) == -1 and column['Default'] is not None:
        sql += f" '{column['Default']}';"
    elif column['Default'] is None:
        sql += ' NULL;'
    else:
        sql += f" {column['Default']};"

    result = DBAccess.query(
        conversion=conversion,
        caller=_set_default.__name__,
        sql=sql,
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=False
    )

    if not result.error:
        msg = (f'\t--[{_set_default.__name__}] Sets default value for'
               f' "{conversion.schema}"."{table_name}"."{column_name}"...')

        log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
