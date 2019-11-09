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


class ExtraConfigProcessor:
    @staticmethod
    def get_column_name(conversion, original_table_name, current_column_name, should_get_original):
        """
        Retrieves appropriate column name.
        :param conversion: Conversion
        :param original_table_name: string
        :param current_column_name: string
        :param should_get_original: bool
        :return: string
        """
        if conversion.extra_config is not None and 'tables' in conversion.extra_config:
            for table_dict in conversion.extra_config['tables']:
                if table_dict['name']['original'] == original_table_name and 'columns' in table_dict:
                    for column_dict in table_dict['columns']:
                        if column_dict['original'] == current_column_name:
                            return column_dict['original'] if should_get_original else column_dict['new']

        return current_column_name

    @staticmethod
    def get_table_name(conversion, current_table_name, should_get_original):
        """
        Retrieves appropriate table name.
        :param conversion: Conversion
        :param current_table_name: string
        :param should_get_original: bool
        :return: string
        """
        if conversion.extra_config is not None and 'tables' in conversion.extra_config:
            for table_dict in conversion.extra_config['tables']:
                table_name = table_dict['name']['new'] if should_get_original else table_dict['name']['original']
                if table_name == current_table_name:
                    return table_dict['name']['original'] if should_get_original else table_dict['name']['new']

        return current_table_name
