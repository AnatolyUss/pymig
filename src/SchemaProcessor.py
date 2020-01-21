__author__ = "Anatoly Khaytovich <anatolyuss@gmail.com>"
__copyright__ = "Copyright (C) 2018 - present, Anatoly Khaytovich <anatolyuss@gmail.com>"
__license__ = """
    This file is a part of "PYMIG" - the database migration tool.
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

from DBAccess import DBAccess
import DBVendors


class SchemaProcessor:
    @staticmethod
    def create_schema(conversion):
        """
        Creates a new PostgreSQL schema if it does not exist yet.
        :param conversion: Conversion, Pymig configuration object.
        :return: None
        """
        log_title = 'SchemaProcessor::create_schema'
        result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql='SELECT schema_name FROM information_schema.schemata WHERE schema_name = \'%s\';' % conversion.schema,
            vendor=DBVendors.PG,
            process_exit_on_error=True,
            should_return_client=True
        )

        if len(result.data) == 0:
            DBAccess.query(
                conversion=conversion,
                caller=log_title,
                sql='CREATE SCHEMA "%s";' % conversion.schema,
                vendor=DBVendors.PG,
                process_exit_on_error=True,
                should_return_client=False,
                client=result.client
            )
