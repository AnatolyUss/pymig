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

import DBVendors
from FsOps import FsOps
from Utils import Utils
from DBAccess import DBAccess
from ExtraConfigProcessor import ExtraConfigProcessor


class TableProcessor:
    @staticmethod
    def create_table(conversion, table_name):
        """
        Migrates structure of a single table to the target server.
        :param conversion: Conversion, Pymig configuration object.
        :param table_name: string, A table name.
        :return: None
        """
        log_title = 'TableProcessor::create_table'
        log_path = conversion.dic_tables[table_name].table_log_path
        FsOps.log(conversion, '\t--[%s] Currently creating table: `%s`' % (log_title, table_name), log_path)
        original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, True)
        sql_show_columns = 'SHOW FULL COLUMNS FROM `%s`;' % original_table_name
        show_columns_result = DBAccess.query(conversion, log_title, sql_show_columns, DBVendors.MYSQL, False, False)

        if show_columns_result.error:
            return

        conversion.dic_tables[table_name].table_columns = show_columns_result.data

        if conversion.should_migrate_only_data():
            return

        def __get_column_definition(input_dict):
            col_name = ExtraConfigProcessor.get_column_name(conversion, original_table_name, input_dict['Field'], False)
            col_type = TableProcessor.map_data_types(conversion.data_types_map, input_dict['Type'])
            return '"%s" %s' % (col_name, col_type)

        sql_columns = ','.join(list(map(__get_column_definition, show_columns_result.data)))
        sql_create_table = 'CREATE TABLE IF NOT EXISTS "%s"."%s"(%s);' % (conversion.schema, table_name, sql_columns)
        create_table_result = DBAccess.query(conversion, log_title, sql_create_table, DBVendors.PG, True, False)

        if not create_table_result.error:
            success_message = '\t--[%s] Table "%s"."%s" is created.' % (log_title, conversion.schema, table_name)
            FsOps.log(conversion, success_message, log_path)

    @staticmethod
    def map_data_types(data_types_map, mysql_data_type):
        """
        Converts MySQL data types to corresponding PostgreSQL data types.
        This conversion performs in accordance to mapping rules in './config/data_types_map.json'.
        './config/data_types_map.json' can be customized.
        :param data_types_map: dict
        :param mysql_data_type: string
        :return: string
        """
        ret_val = ''
        data_type_details = mysql_data_type.split(' ')
        mysql_data_type = data_type_details[0].lower()
        increase_original_size = Utils.get_index_of('unsigned', data_type_details) != -1 \
            or Utils.get_index_of('zerofill', data_type_details) != -1

        if Utils.get_index_of('(', mysql_data_type) == -1:
            # No parentheses detected.
            ret_val = data_types_map[mysql_data_type]['increased_size'] \
                if increase_original_size else data_types_map[mysql_data_type]['type']
        else:
            # Parentheses detected.
            list_data_type = mysql_data_type.split('(')
            str_data_type = list_data_type[0].lower()
            type_display_width = list_data_type[1]

            if 'enum' == str_data_type or 'set' == str_data_type:
                ret_val = 'character varying(255)'
            elif 'decimal' == str_data_type or 'numeric' == str_data_type:
                ret_val = '%s(%s' % (data_types_map[str_data_type]['type'], type_display_width)
            elif 'decimal(19,2)' == mysql_data_type or data_types_map[str_data_type]['mySqlVarLenPgSqlFixedLen']:
                # Should be converted without a length definition.
                ret_val = data_types_map[str_data_type]['increased_size'] \
                    if increase_original_size else data_types_map[str_data_type]['type']

            else:
                # Should be converted with a length definition.
                ret_val = '%s(%s' % (data_types_map[str_data_type]['increased_size'], type_display_width) \
                    if increase_original_size else '%s(%s' % (data_types_map[str_data_type]['type'], type_display_width)

        # Prevent incompatible length (CHARACTER(0) or CHARACTER VARYING(0)).
        if ret_val == 'character(0)':
            ret_val = 'character(1)'
        elif ret_val == 'character varying(0)':
            ret_val = 'character varying(1)'

        return ret_val
