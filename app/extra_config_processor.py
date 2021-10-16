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
from typing import cast

from app.conversion import Conversion


def get_column_name(
    conversion: Conversion,
    original_table_name: str,
    current_column_name: str,
    should_get_original: bool
) -> str:
    """
    Retrieves appropriate column name.
    """
    if 'tables' in conversion.extra_config:
        for table_dict in conversion.extra_config['tables']:
            if table_dict['name']['original'] == original_table_name and 'columns' in table_dict:
                for column_dict in table_dict['columns']:
                    if column_dict['original'] == current_column_name:
                        return cast(str, column_dict['original'] if should_get_original else column_dict['new'])

    return current_column_name


def get_table_name(
    conversion: Conversion,
    current_table_name: str,
    should_get_original: bool
) -> str:
    """
    Retrieves appropriate table name.
    """
    if 'tables' in conversion.extra_config:
        for table_dict in conversion.extra_config['tables']:
            table_name = table_dict['name']['new'] if should_get_original else table_dict['name']['original']
            if table_name == current_table_name:
                return cast(str, table_dict['name']['original'] if should_get_original else table_dict['name']['new'])

    return current_table_name


def parse_foreign_keys(
    conversion: Conversion,
    table_name: str
) -> list[dict[str, str]]:
    """
    Parses the extra_config foreign_keys attributes and generate an output array
    required by ForeignKeyProcessor.process_foreign_key_worker.
    """
    ret_val = []
    if 'foreign_keys' in conversion.extra_config:
        for row in conversion.extra_config['foreign_keys']:
            if row['table_name'] == table_name:
                # There may be several FKs in a single table.
                fk = {attr.upper(): row[attr] for attr in row}
                ret_val.append(fk)

    return ret_val
