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
import app.db_Access as DBAccess
import app.extra_config_processor as ExtraConfigProcessor
from app.db_vendors import DBVendors
from app.fs_ops import log
from app.utils import get_index_of
from app.conversion import Conversion


def create_table(conversion: Conversion, table_name: str) -> None:
    """
    Migrates structure of a single table to the target server.
    """
    log_path = conversion.dic_tables[table_name].table_log_path
    log(conversion, f'\t--[{create_table.__name__}] Currently creating table: `{table_name}`', log_path)
    original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, True)
    show_columns_result = DBAccess.query(
        conversion=conversion,
        caller=create_table.__name__,
        sql=f'SHOW FULL COLUMNS FROM `{original_table_name}`;',
        vendor=DBVendors.MYSQL.value,
        process_exit_on_error=False,
        should_return_client=False
    )

    if show_columns_result.error:
        return

    conversion.dic_tables[table_name].table_columns = show_columns_result.data

    if conversion.should_migrate_only_data():
        return

    def _get_column_definition(input_dict: dict[str, str]) -> str:
        """
        Returns a column definition.
        """
        col_name = ExtraConfigProcessor.get_column_name(conversion, original_table_name, input_dict['Field'], False)
        col_type = map_data_types(conversion.data_types_map, input_dict['Type'])
        return f'"{col_name}" {col_type}'

    sql_columns = ','.join([_get_column_definition(input_dict) for input_dict in show_columns_result.data])
    create_table_result = DBAccess.query(
        conversion=conversion,
        caller=create_table.__name__,
        sql=f'CREATE TABLE IF NOT EXISTS "{conversion.schema}"."{table_name}"({sql_columns});',
        vendor=DBVendors.PG.value,
        process_exit_on_error=True,
        should_return_client=False
    )

    if not create_table_result.error:
        success_message = f'\t--[{create_table.__name__}] Table "{conversion.schema}"."{table_name}" is created'
        log(conversion, success_message, log_path)


def map_data_types(data_types_map: dict, mysql_data_type: str) -> str:
    """
    Converts MySQL data types to corresponding PostgreSQL data types.
    This conversion performs in accordance to mapping rules in './config/data_types_map.json'.
    './config/data_types_map.json' can be customized.
    """
    ret_val = ''
    data_type_details = mysql_data_type.split(' ')
    mysql_data_type = data_type_details[0].lower()
    increase_original_size = (get_index_of('unsigned', data_type_details) != -1
                              or get_index_of('zerofill', data_type_details) != -1)

    if get_index_of('(', mysql_data_type) == -1:
        # No parentheses detected.
        ret_val = (data_types_map[mysql_data_type]['increased_size']
                   if increase_original_size
                   else data_types_map[mysql_data_type]['type'])
    else:
        # Parentheses detected.
        list_data_type = mysql_data_type.split('(')
        str_data_type = list_data_type[0].lower()
        type_display_width = list_data_type[1]

        if 'enum' == str_data_type or 'set' == str_data_type:
            ret_val = 'character varying(255)'
        elif 'decimal' == str_data_type or 'numeric' == str_data_type:
            ret_val = f'{data_types_map[str_data_type]["type"]}({type_display_width}'
        elif 'decimal(19,2)' == mysql_data_type or data_types_map[str_data_type]['mySqlVarLenPgSqlFixedLen']:
            # Should be converted without a length definition.
            ret_val = (data_types_map[str_data_type]['increased_size']
                       if increase_original_size
                       else data_types_map[str_data_type]['type'])
        else:
            # Should be converted with a length definition.
            ret_val = (f'{data_types_map[str_data_type]["increased_size"]}({type_display_width}'
                       if increase_original_size
                       else f'{data_types_map[str_data_type]["type"]}({type_display_width}')

    # Prevent incompatible length (CHARACTER(0) or CHARACTER VARYING(0)).
    if ret_val == 'character(0)':
        ret_val = 'character(1)'
    elif ret_val == 'character varying(0)':
        ret_val = 'character varying(1)'

    return ret_val
