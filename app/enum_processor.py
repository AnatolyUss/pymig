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
from app.conversion import Conversion
from app.db_vendor import DBVendor
from app.utils import get_index_of
from app.fs_ops import log
from app.concurrency_manager import run_concurrently


def process_enum(conversion: Conversion, table_name: str) -> None:
    """
    Defines which columns of the given table are of type "enum".
    Sets an appropriate constraint, if appropriate.
    """
    msg = f'\t--[{process_enum.__name__}] Defines "ENUMs" for table "{conversion.schema}"."{table_name}"'
    log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
    original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, True)
    params = [
        [conversion, table_name, original_table_name, column]
        for column in conversion.dic_tables[table_name].table_columns
        if _is_enum(column)
    ]

    run_concurrently(conversion, _set_enum, params)


def _is_enum(column: dict) -> bool:
    """
    Checks if given column is of type enum.
    """
    if get_index_of('(', column['Type']) != -1:
        list_type = column['Type'].split('(')
        return list_type[0] == 'enum'

    return False


def _set_enum(
    conversion: Conversion,
    table_name: str,
    original_table_name: str,
    column: dict
) -> None:
    """
    Checks if given column is an enum.
    Sets the enum, if appropriate.
    """
    column_name = ExtraConfigProcessor.get_column_name(
        conversion=conversion,
        original_table_name=original_table_name,
        current_column_name=column['Field'],
        should_get_original=False
    )

    enum_values = column['Type'].split('(')[1]  # Exists due to EnumProcessor._is_enum execution result.
    sql = f'ALTER TABLE "{conversion.schema}"."{table_name}" ADD CHECK ("{column_name}" IN ({enum_values});'
    result = DBAccess.query(
        conversion=conversion,
        caller=_set_enum.__name__,
        sql=sql,
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=False
    )

    if not result.error:
        msg = f'\t--[{_set_enum.__name__}] Set "ENUM" for "{conversion.schema}"."{table_name}"."{column_name}"...'
        log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
