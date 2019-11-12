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

from Utils import Utils


class ColumnsDataArranger:
    @staticmethod
    def arrange_columns_data(table_columns, mysql_version):
        """
        Arranges columns data before loading.
        :param table_columns: list
        :param mysql_version: string
        :return: string
        """
        ret_val = ''
        wkb_func = 'ST_AsWKB' if float(mysql_version) >= 5.76 else 'AsWKB'

        for column in table_columns:
            col_field, col_type = column['Field'], column['Type']

            if ColumnsDataArranger.is_spacial(col_type):
                # Apply HEX(ST_AsWKB(...)) due to the issue, described at https://bugs.mysql.com/bug.php?id=69798
                ret_val += 'HEX({0}(`{1}`)) AS `{1}`,'.format(wkb_func, col_field)
            elif ColumnsDataArranger.is_binary(col_type):
                ret_val += 'HEX(`{0}`) AS `{0}`,'.format(col_field)
            elif ColumnsDataArranger.is_bit(col_type):
                ret_val += 'BIN(`{0}`) AS `{0}`,'.format(col_field)
            elif ColumnsDataArranger.is_date_time(col_type):
                ret_val += "IF(`{0}` IN('0000-00-00', '0000-00-00 00:00:00'),".format(col_field) \
                        + " '-INFINITY', CAST(`{0}` AS CHAR)) AS `{0}`,".format(col_field)
            else:
                ret_val += '`{0}` AS `{0}`,'.format(col_field)

        return ret_val[0:-1]

    @staticmethod
    def is_spacial(data_type):
        """
        Defines if given type is one of MySQL spacial types.
        :param data_type: string
        :return: bool
        """
        return Utils.get_index_of('geometry', data_type) != -1 \
            or Utils.get_index_of('point', data_type) != -1 \
            or Utils.get_index_of('linestring', data_type) != -1 \
            or Utils.get_index_of('polygon', data_type) != -1

    @staticmethod
    def is_date_time(data_type):
        """
        Defines if given type is one of MySQL date-time types.
        :param data_type: string
        :return: bool
        """
        return Utils.get_index_of('timestamp', data_type) != -1 or Utils.get_index_of('date', data_type) != -1

    @staticmethod
    def is_binary(data_type):
        """
        Defines if given type is one of MySQL binary types.
        :param data_type: string
        :return: bool
        """
        return Utils.get_index_of('blob', data_type) != -1 or Utils.get_index_of('binary', data_type) != -1

    @staticmethod
    def is_bit(data_type):
        """
        Defines if given type is one of MySQL bit type.
        :param data_type: string
        :return: bool
        """
        return Utils.get_index_of('bit', data_type) != -1
