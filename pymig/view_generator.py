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
import os
from typing import cast, Any

import pymig.migration_state_manager as MigrationStateManager
import pymig.db_access as DBAccess
from pymig.db_vendor import DBVendor
from pymig.utils import get_index_of
from pymig.fs_ops import write_to_file, log
from pymig.conversion import Conversion


def generate_views(conversion: Conversion) -> None:
    """
    Attempts to convert MySQL views to PostgreSQL views.
    """
    views_loaded = MigrationStateManager.get(conversion, 'views_loaded')

    if views_loaded:
        return

    params = [[conversion, view_name] for view_name in conversion.views_to_migrate]
    conversion.run_concurrently(func=_generate_single_view, params_list=params)


def _generate_single_view(conversion: Conversion, view_name: str) -> None:
    """
    Attempts to convert given view from MySQL to PostgreSQL.
    """
    show_create_view_result = DBAccess.query(
        conversion=conversion,
        caller=_generate_single_view.__name__,
        vendor=DBVendor.MYSQL,
        process_exit_on_error=False,
        should_return_client=False,
        sql=f'SHOW CREATE VIEW `{view_name}`;'
    )

    if show_create_view_result.error:
        return

    show_create_view_result_data = cast(list[dict[str, Any]], show_create_view_result.data)
    create_pg_view_sql = _generate_view_code(
        schema=conversion.schema,
        view_name=view_name,
        mysql_view_code=show_create_view_result_data[0]['Create View']
    )

    create_pg_view_result = DBAccess.query(
        conversion=conversion,
        caller=_generate_single_view.__name__,
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=False,
        should_return_programming_error=True,
        sql=create_pg_view_sql
    )

    if create_pg_view_result.error:
        _log_not_created_view(conversion, view_name, create_pg_view_sql)
        return

    log(conversion, f'[{_generate_single_view.__name__}] View "{conversion.schema}"."{view_name}" is created...')


def _log_not_created_view(
    conversion: Conversion,
    view_name: str,
    sql: str
) -> None:
    """
    Writes a log, containing a code of the view FromMySqlToPostgreSql has just failed to create.
    """
    view_file_path = os.path.join(conversion.not_created_views_path, f'{view_name}.sql')
    write_to_file(view_file_path, 'w', sql)


def _generate_view_code(
    schema: str,
    view_name: str,
    mysql_view_code: str
) -> str:
    """
    Attempts to convert MySQL view to PostgreSQL view.
    Attempts to generate a PostgreSQL equivalent of MySQL view.
    """
    mysql_view_code = '"'.join(mysql_view_code.split('`'))
    query_start_position = get_index_of('AS', mysql_view_code)
    mysql_view_code = mysql_view_code[query_start_position:]
    mysql_view_code_list = mysql_view_code.split(' ')
    mysql_view_code_list_length = len(mysql_view_code_list)

    for position, key_word in enumerate(mysql_view_code_list):
        key_word_lower = key_word.lower()
        next_position = position + 1

        if key_word_lower == 'from' or key_word_lower == 'join' and next_position < mysql_view_code_list_length:
            mysql_view_code_list[next_position] = f'"{schema}".{mysql_view_code_list[next_position]}'

    return f'CREATE OR REPLACE VIEW "{schema}"."{view_name}" {" ".join(mysql_view_code_list)};'
