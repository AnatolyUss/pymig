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
from app.concurrency_manager import run_concurrently
from app.db_vendor import DBVendor
from app.conversion import Conversion
from app.fs_ops import log


def process_null(conversion: Conversion, table_name: str) -> None:
    """
    Defines which columns of the given table can contain the "NULL" value.
    Sets an appropriate constraint.
    """
    msg = f'[{process_null.__name__}] Sets "NOT NULL" constraints for table: "{conversion.schema}"."{table_name}"'
    log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
    original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, should_get_original=True)
    params = [
        [conversion, table_name, original_table_name, column]
        for column in conversion.dic_tables[table_name].table_columns
        if column['Null'].lower() == 'no'
    ]

    run_concurrently(conversion, _set_not_null, params)


def _set_not_null(
    conversion: Conversion,
    table_name: str,
    original_table_name: str,
    column: dict
) -> None:
    """
    Sets the NOT NULL constraint for given column.
    """
    column_name = ExtraConfigProcessor.get_column_name(
        conversion=conversion,
        original_table_name=original_table_name,
        current_column_name=column['Field'],
        should_get_original=False
    )

    result = DBAccess.query(
        conversion=conversion,
        caller=_set_not_null.__name__,
        sql=f'ALTER TABLE "{conversion.schema}"."{table_name}" ALTER COLUMN "{column_name}" SET NOT NULL;',
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=False
    )

    if not result.error:
        msg = f'[{_set_not_null.__name__}] Set NOT NULL for "{conversion.schema}"."{table_name}"."{column_name}"...'
        log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
