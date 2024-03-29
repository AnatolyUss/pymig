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
from typing import cast, Any

import pymig.db_access as DBAccess
import pymig.migration_state_manager as MigrationStateManager
import pymig.extra_config_processor as ExtraConfigProcessor
from pymig.db_vendor import DBVendor
from pymig.utils import get_index_of
from pymig.table import Table
from pymig.fs_ops import log
from pymig.conversion import Conversion
from pymig.table_processor import create_table
from pymig.data_chunks_processor import prepare_data_chunks


def load_structure(conversion: Conversion) -> None:
    """
    Loads source tables and views, that need to be migrated.
    """
    _get_mysql_version(conversion)
    have_tables_loaded = MigrationStateManager.get(conversion, 'tables_loaded')
    sql = f'SHOW FULL TABLES IN `{conversion.mysql_db_name}` WHERE 1 = 1'

    if conversion.include_tables:
        include_tables = ','.join([f'"{table_name}"' for table_name in conversion.include_tables])
        sql += f' AND Tables_in_{conversion.mysql_db_name} IN({include_tables})'

    if conversion.exclude_tables:
        exclude_tables = ','.join([f'"{table_name}"' for table_name in conversion.exclude_tables])
        sql += f' AND Tables_in_{conversion.mysql_db_name} NOT IN({exclude_tables})'

    result = DBAccess.query(
        conversion=conversion,
        caller=load_structure.__name__,
        sql=f'{sql};',
        vendor=DBVendor.MYSQL,
        process_exit_on_error=True,
        should_return_client=False
    )

    thread_pool_params, tables_cnt, views_cnt = [], 0, 0
    result_data = cast(list[dict[str, Any]], result.data)

    for row in result_data:
        relation_name = row[f'Tables_in_{conversion.mysql_db_name}']

        if row['Table_type'] == 'BASE TABLE' and get_index_of(relation_name, conversion.exclude_tables) == -1:
            relation_name = ExtraConfigProcessor.get_table_name(conversion, relation_name, False)
            conversion.tables_to_migrate.append(relation_name)
            conversion.dic_tables[relation_name] = Table(f'{conversion.logs_dir_path}/{relation_name}.log')
            thread_pool_params.append([conversion, relation_name, have_tables_loaded])
            tables_cnt += 1
        elif row['Table_type'] == 'VIEW':
            conversion.views_to_migrate.append(relation_name)
            views_cnt += 1

    conversion.run_concurrently(func=process_table_before_data_loading, params_list=thread_pool_params)
    msg = (f'[{load_structure.__name__}] Source DB structure is loaded...\n'
           f'\t--[{load_structure.__name__}] Tables to migrate: {tables_cnt}\n'
           f'\t--[{load_structure.__name__}] Views to migrate: {views_cnt}')

    log(conversion, msg)
    MigrationStateManager.set(conversion, 'tables_loaded')


def process_table_before_data_loading(
    conversion: Conversion,
    table_name: str,
    have_data_chunks_processed: bool
) -> None:
    """
    Processes current table before data loading.
    """
    create_table(conversion, table_name)
    prepare_data_chunks(conversion, table_name, have_data_chunks_processed)


def _get_mysql_version(conversion: Conversion) -> None:
    """
    Retrieves the source db version.
    """
    result = DBAccess.query(
        conversion=conversion,
        caller=_get_mysql_version.__name__,
        sql='SELECT VERSION() AS mysql_version;',
        vendor=DBVendor.MYSQL,
        process_exit_on_error=False,
        should_return_client=False
    )

    if not result.error:
        result_data = cast(list[dict[str, Any]], result.data)
        split_version = result_data[0]['mysql_version'].split('.')
        major_version = split_version[0]
        minor_version_with_postfix = ''.join(split_version[1:])
        minor_version = minor_version_with_postfix.split('-')[0]
        conversion.mysql_version = f'{major_version}.{minor_version}'
