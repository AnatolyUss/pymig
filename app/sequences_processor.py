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
import app.extra_config_processor as ExtraConfigProcessor
import app.db_access as DBAccess
from app.fs_ops import log
from app.db_vendor import DBVendor
from app.conversion import Conversion


def create_sequence(conversion: Conversion, table_name: str) -> None:
    """
    Defines which column in given table has the "auto_increment" attribute.
    Creates an appropriate sequence.
    """
    original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, True)
    table_columns_list = conversion.dic_tables[table_name].table_columns
    auto_increment_columns = [column for column in table_columns_list if column['Extra'] == 'auto_increment']

    if len(auto_increment_columns) == 0:
        return  # No auto-incremented column found.

    auto_increment_column = auto_increment_columns[0]['Field']
    column_name = ExtraConfigProcessor.get_column_name(
        conversion=conversion,
        original_table_name=original_table_name,
        current_column_name=auto_increment_column,
        should_get_original=False
    )

    seq_name = f'{table_name}_{column_name}_seq'
    create_sequence_result = DBAccess.query(
        conversion=conversion,
        caller=create_sequence.__name__,
        sql=f'CREATE SEQUENCE "{conversion.schema}"."{seq_name}";',
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=True
    )

    if create_sequence_result.error:
        DBAccess.release_db_client(conversion, create_sequence_result.client)
        return

    sql_set_next_val = (f'ALTER TABLE "{conversion.schema}"."{table_name}" ALTER COLUMN "{column_name}"'
                        f" SET DEFAULT NEXTVAL('{conversion.schema}.{seq_name}');")

    set_next_val_result = DBAccess.query(
        conversion=conversion,
        caller=create_sequence.__name__,
        sql=sql_set_next_val,
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=True,
        client=create_sequence_result.client
    )

    if set_next_val_result.error:
        DBAccess.release_db_client(conversion, set_next_val_result.client)
        return

    sql_set_sequence_owner = (f'ALTER SEQUENCE "{conversion.schema}"."{seq_name}"'
                              f' OWNED BY "{conversion.schema}"."{table_name}"."{column_name}";')

    set_sequence_owner_result = DBAccess.query(
        conversion=conversion,
        caller=create_sequence.__name__,
        sql=sql_set_sequence_owner,
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=True,
        client=set_next_val_result.client
    )

    if set_sequence_owner_result.error:
        DBAccess.release_db_client(conversion, set_sequence_owner_result.client)
        return

    sql_set_sequence_value = (f'SELECT SETVAL(\'"{conversion.schema}"."{seq_name}"\','
                              f' (SELECT MAX("{column_name}") FROM "{conversion.schema}"."{table_name}"));')

    set_sequence_value_result = DBAccess.query(
        conversion=conversion,
        caller=create_sequence.__name__,
        sql=sql_set_sequence_value,
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=False,
        client=set_sequence_owner_result.client
    )

    if not set_sequence_value_result.error:
        msg = f'[{create_sequence.__name__}] Sequence "{conversion.schema}"."{seq_name}" is created...'
        log(conversion, msg, conversion.dic_tables[table_name].table_log_path)


def set_sequence_value(conversion: Conversion, table_name: str) -> None:
    """
    Sets sequence value.
    """
    original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, True)
    table_columns_list = conversion.dic_tables[table_name].table_columns
    auto_increment_columns = [column for column in table_columns_list if column['Extra'] == 'auto_increment']

    if len(auto_increment_columns) == 0:
        return  # No auto-incremented column found.

    auto_increment_column = auto_increment_columns[0]['Field']
    column_name = ExtraConfigProcessor.get_column_name(
        conversion=conversion,
        original_table_name=original_table_name,
        current_column_name=auto_increment_column,
        should_get_original=False
    )

    seq_name = f'{table_name}_{column_name}_seq'
    sql = (f'SELECT SETVAL("{conversion.schema}"."{seq_name}",'
           f' (SELECT MAX("{column_name}") FROM "{conversion.schema}"."{table_name}"));')

    result = DBAccess.query(
        conversion=conversion,
        caller=set_sequence_value.__name__,
        sql=sql,
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=False
    )

    if not result.error:
        msg = f'[{set_sequence_value.__name__}] Sequence "{conversion.schema}"."{seq_name}" is created...'
        log(conversion, msg, conversion.dic_tables[table_name].table_log_path)
