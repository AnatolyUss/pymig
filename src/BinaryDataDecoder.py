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
import DBVendors
from FsOps import FsOps
from DBAccess import DBAccess
from ConcurrencyManager import ConcurrencyManager


class BinaryDataDecoder:
    @staticmethod
    def decode(conversion):
        """
        Decodes binary data from from textual representation.
        :param conversion: Conversion
        :return: None
        """
        log_title = 'BinaryDataDecoder::decode'
        FsOps.log(conversion, '\t--[%s] Decoding binary data from textual representation.' % log_title)

        sql = '''
        SELECT table_name, column_name FROM information_schema.columns
        WHERE table_catalog = \'%s\' AND table_schema = \'%s\' AND data_type IN (\'bytea\', \'geometry\');
        ''' % (conversion.target_con_string['database'], conversion.schema)

        result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=False
        )

        if result.error:
            # No need to continue if no 'bytea' or 'geometry' columns found.
            return

        params = [[conversion, record['table_name'], record['column_name']] for record in result.data]
        ConcurrencyManager.run_in_parallel(conversion, BinaryDataDecoder._decode, params)

    @staticmethod
    def _decode(conversion, table_name, column_name):
        log_title = 'BinaryDataDecoder::_decode'
        sql = 'UPDATE {0}."{1}" SET "{2}" = DECODE(ENCODE("{2}", {3}}), {4});' \
            .format(conversion.schema, table_name, column_name, "'escape'", "'hex'")

        result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=False
        )

        if not result.error:
            msg = '\t--[%s] Decoded binary data from textual representation for table "%s"."%s".' \
                  % (log_title, conversion.schema, table_name)

            FsOps.log(conversion, msg)
