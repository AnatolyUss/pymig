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
from app.db_vendors import DBVendors
from app.fs_ops import log
from app.conversion import Conversion
from app.concurrency_manager import run_concurrently


def decode(conversion: Conversion) -> None:
    """
    Decodes binary data from from textual representation.
    """
    log(conversion, f'\t--[{decode.__name__}] Decoding binary data from textual representation')
    sql = ("SELECT table_name, column_name FROM information_schema.columns"
           f" WHERE table_catalog = '{conversion.target_con_string['database']}'"
           f" AND table_schema = '{conversion.schema}' AND data_type IN ('bytea', 'geometry');")

    result = DBAccess.query(
        conversion=conversion,
        caller=decode.__name__,
        sql=sql,
        vendor=DBVendors.PG.value,
        process_exit_on_error=False,
        should_return_client=False
    )

    if result.error:
        # No need to continue if no 'bytea' or 'geometry' columns found.
        return

    params = [[conversion, record['table_name'], record['column_name']] for record in result.data]
    run_concurrently(conversion, _decode, params)


def _decode(
    conversion: Conversion,
    table_name: str,
    column_name: str
) -> None:
    """
    Performs the actual binary data decoding.
    """
    quoted_escape = "'escape'"
    quoted_hex = "'hex'"
    sql = (f'UPDATE {conversion.schema}."{table_name}" '
           f'SET "{column_name}" = DECODE(ENCODE("{column_name}", {quoted_escape}), {quoted_hex});')

    result = DBAccess.query(
        conversion=conversion,
        caller=_decode.__name__,
        sql=sql,
        vendor=DBVendors.PG.value,
        process_exit_on_error=False,
        should_return_client=False
    )

    if not result.error:
        msg = (f'\t--[{_decode.__name__}] Decoded binary data from textual representation'
               f' for table "{conversion.schema}"."{table_name}"')

        log(conversion, msg)
