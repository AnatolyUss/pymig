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
from app.db_vendor import DBVendor
from app.conversion import Conversion


def create_schema(conversion: Conversion) -> None:
    """
    Creates a new PostgreSQL schema if it does not exist yet.
    """
    result = DBAccess.query(
        conversion=conversion,
        caller=create_schema.__name__,
        sql=f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{conversion.schema}';",
        vendor=DBVendor.PG,
        process_exit_on_error=True,
        should_return_client=True
    )

    if len(result.data) == 0:
        DBAccess.query(
            conversion=conversion,
            caller=create_schema.__name__,
            sql=f'CREATE SCHEMA "{conversion.schema}";',
            vendor=DBVendor.PG,
            process_exit_on_error=True,
            should_return_client=False,
            client=result.client
        )
