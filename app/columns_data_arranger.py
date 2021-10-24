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
from app.utils import get_index_of


def arrange_columns_data(table_columns: list[dict], mysql_version: str) -> str:
    """
    TODO: must pass encoding.
    Arranges columns data before loading.
    Notice, the "inline" columns encoding conversion cannot be implemented,
    since MySQL's UTF-8 implementation isn't the same as PostgreSQL's one.
    """
    select_fields_list = []
    wkb_func = 'ST_AsWKB' if float(mysql_version) >= 5.76 else 'AsWKB'

    for column in table_columns:
        col_field, col_type = column['Field'], column['Type']

        if is_spacial(col_type):
            # Apply HEX(ST_AsWKB(...)) due to the issue, described at https://bugs.mysql.com/bug.php?id=69798
            select_fields_list.append(f'HEX({wkb_func}(`{col_field}`)) AS `{col_field}`')
        elif is_binary(col_type):
            select_fields_list.append(f'HEX(`{col_field}`) AS `{col_field}`')
        elif is_bit(col_type):
            select_fields_list.append('BIN(`{0}`) AS `{0}`'.format(col_field))
        elif is_date_time(col_type):
            select_fields_list.append(f"IF(`{col_field}` IN('0000-00-00', '0000-00-00 00:00:00'),"
                                      f" '-INFINITY', CAST(`{col_field}` AS CHAR)) AS `{col_field}`")
        elif is_numeric(col_type):
            select_fields_list.append(f"`{col_field}` AS `{col_field}`")
        else:
            select_fields_list.append(f"`{col_field}` AS `{col_field}`")

    return ','.join(select_fields_list)


def is_numeric(data_type: str) -> bool:
    """
    Defines if given type is one of MySQL numeric types.
    """
    return (get_index_of('decimal', data_type) != -1
            or get_index_of('numeric', data_type) != -1
            or get_index_of('double', data_type) != -1
            or get_index_of('float', data_type) != -1
            or (get_index_of('int', data_type) != -1 and data_type != 'point'))


def is_spacial(data_type: str) -> bool:
    """
    Defines if given type is one of MySQL spacial types.
    """
    return (get_index_of('geometry', data_type) != -1
            or get_index_of('point', data_type) != -1
            or get_index_of('linestring', data_type) != -1
            or get_index_of('polygon', data_type) != -1)


def is_date_time(data_type: str) -> bool:
    """
    Defines if given type is one of MySQL date-time types.
    """
    return get_index_of('timestamp', data_type) != -1 or get_index_of('date', data_type) != -1


def is_binary(data_type: str) -> bool:
    """
    Defines if given type is one of MySQL binary types.
    """
    return get_index_of('blob', data_type) != -1 or get_index_of('binary', data_type) != -1


def is_bit(data_type: str) -> bool:
    """
    Defines if given type is one of MySQL bit type.
    """
    return get_index_of('bit', data_type) != -1
