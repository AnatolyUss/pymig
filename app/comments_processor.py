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
from typing import cast, Any

import app.db_access as DBAccess
import app.extra_config_processor as ExtraConfigProcessor
from app.fs_ops import log
from app.conversion import Conversion
from app.db_vendor import DBVendor


def process_comments(conversion: Conversion, table_name: str) -> None:
    """
    Migrates comments.
    """
    msg = f'[{process_comments.__name__}] Creates comments for table "{conversion.schema}"."{table_name}"...'
    log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
    _process_table_comments(conversion, table_name)
    _process_columns_comments(conversion, table_name)


def _process_table_comments(conversion: Conversion, table_name: str) -> None:
    """
    Creates table comments.
    """
    sql_select_comment = (f"SELECT table_comment AS table_comment FROM information_schema.tables "
                          f"WHERE table_schema = '{conversion.mysql_db_name}' AND table_name = "
                          f"'{ExtraConfigProcessor.get_table_name(conversion, table_name, should_get_original=True)}';")

    select_comments_result = DBAccess.query(
        conversion=conversion,
        caller=_process_table_comments.__name__,
        sql=sql_select_comment,
        vendor=DBVendor.MYSQL,
        process_exit_on_error=False,
        should_return_client=False
    )

    if select_comments_result.error:
        return

    select_comments_result_data = cast(list[dict[str, Any]], select_comments_result.data)
    comment = _escape_quotes(select_comments_result_data[0]['table_comment'])
    sql_create_comment = f'COMMENT ON TABLE "{conversion.schema}"."{table_name}" IS \'{comment}\';'
    create_comment_result = DBAccess.query(
        conversion=conversion,
        caller=_process_table_comments.__name__,
        sql=sql_create_comment,
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=False
    )

    if create_comment_result.error:
        return

    msg = (f'[{_process_table_comments.__name__}]'
           f' Successfully set comment for table "{conversion.schema}"."{table_name}"')

    log(conversion, msg, conversion.dic_tables[table_name].table_log_path)


def _process_columns_comments(conversion: Conversion, table_name: str) -> None:
    """
    Creates columns comments.
    """
    original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, should_get_original=True)
    params = [
        [conversion, table_name, original_table_name, column]
        for column in conversion.dic_tables[table_name].table_columns
        if column['Comment'] != ''
    ]

    conversion.run_concurrently(func=_set_column_comment, params_list=params)


def _set_column_comment(
    conversion: Conversion,
    table_name: str,
    original_table_name: str,
    column: dict
) -> None:
    """
    Creates comment on specified column.
    """
    column_name = ExtraConfigProcessor.get_column_name(
        conversion=conversion,
        original_table_name=original_table_name,
        current_column_name=column['Field'],
        should_get_original=False
    )

    comment = _escape_quotes(column['Comment'])
    create_comment_result = DBAccess.query(
        conversion=conversion,
        caller=_set_column_comment.__name__,
        sql=f'COMMENT ON COLUMN "{conversion.schema}"."{table_name}"."{column_name}" IS \'{comment}\';',
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=False
    )

    if create_comment_result.error:
        return

    msg = (f'[{_set_column_comment.__name__}] Set comment for'
           f' "{conversion.schema}"."{table_name}" column: "{column_name}"...')

    log(conversion, msg, conversion.dic_tables[table_name].table_log_path)


def _escape_quotes(string: str) -> str:
    """
    Escapes quotes inside given string.
    """
    return string.replace("'", "''")
