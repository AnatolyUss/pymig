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
from typing import cast, Union

import app.db_access as DBAccess
import app.migration_state_manager as MigrationStateManager
import app.extra_config_processor as ExtraConfigProcessor
from app.fs_ops import log
from app.db_vendor import DBVendor
from app.conversion import Conversion


def set_foreign_keys(conversion: Conversion) -> None:
    """
    Starts a process of foreign keys migration.
    """
    foreign_keys_processed = MigrationStateManager.get(conversion, 'foreign_keys_loaded')

    if foreign_keys_processed:
        return

    params = [[conversion, table_name] for table_name in conversion.tables_to_migrate]
    conversion.run_concurrently(func=_get_foreign_keys_metadata, params_list=params)


def _get_foreign_keys_metadata(conversion: Conversion, table_name: str) -> None:
    """
    Retrieves foreign keys metadata.
    """
    msg = (f'[{_get_foreign_keys_metadata.__name__}]'
           f' Search foreign keys for table "{conversion.schema}"."{table_name}"...')

    log(conversion, msg)
    sql = f"""
        SELECT 
            cols.COLUMN_NAME, refs.REFERENCED_TABLE_NAME, refs.REFERENCED_COLUMN_NAME,
            cRefs.UPDATE_RULE, cRefs.DELETE_RULE, cRefs.CONSTRAINT_NAME 
        FROM INFORMATION_SCHEMA.`COLUMNS` AS cols 
        INNER JOIN INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` AS refs 
            ON refs.TABLE_SCHEMA = cols.TABLE_SCHEMA 
                AND refs.REFERENCED_TABLE_SCHEMA = cols.TABLE_SCHEMA 
                AND refs.TABLE_NAME = cols.TABLE_NAME 
                AND refs.COLUMN_NAME = cols.COLUMN_NAME 
        LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS cRefs 
            ON cRefs.CONSTRAINT_SCHEMA = cols.TABLE_SCHEMA 
                AND cRefs.CONSTRAINT_NAME = refs.CONSTRAINT_NAME 
        LEFT JOIN INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` AS links 
            ON links.TABLE_SCHEMA = cols.TABLE_SCHEMA 
                AND links.REFERENCED_TABLE_SCHEMA = cols.TABLE_SCHEMA 
                AND links.REFERENCED_TABLE_NAME = cols.TABLE_NAME 
                AND links.REFERENCED_COLUMN_NAME = cols.COLUMN_NAME 
        LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS cLinks 
            ON cLinks.CONSTRAINT_SCHEMA = cols.TABLE_SCHEMA 
                AND cLinks.CONSTRAINT_NAME = links.CONSTRAINT_NAME 
        WHERE cols.TABLE_SCHEMA = '{conversion.mysql_db_name}' AND cols.TABLE_NAME
         = '{ExtraConfigProcessor.get_table_name(conversion, table_name, should_get_original=True)}';
    """

    result = DBAccess.query(
        conversion=conversion,
        caller=_get_foreign_keys_metadata.__name__,
        sql=sql,
        vendor=DBVendor.MYSQL,
        process_exit_on_error=False,
        should_return_client=False
    )

    if result.error:
        return

    extra_rows = ExtraConfigProcessor.parse_foreign_keys(conversion, table_name)
    full_rows = (result.data or []) + extra_rows
    _set_foreign_keys_for_given_table(conversion, table_name, full_rows)
    msg = (f'[{_get_foreign_keys_metadata.__name__}]'
           f' Foreign keys for table "{conversion.schema}"."{table_name}" are set...')

    log(conversion, msg)


def _set_foreign_keys_for_given_table(
    conversion: Conversion,
    table_name: str,
    rows: list[dict]
) -> None:
    """
    Sets foreign keys for given table.
    """
    constraints: dict[str, dict[str, Union[str, list[str]]]] = {}
    original_table_name = ExtraConfigProcessor.get_table_name(conversion, table_name, should_get_original=True)

    for row in rows:
        current_column_name = ExtraConfigProcessor.get_column_name(
            conversion=conversion,
            original_table_name=original_table_name,
            current_column_name=row['COLUMN_NAME'],
            should_get_original=False
        )

        current_referenced_table_name = ExtraConfigProcessor.get_table_name(
            conversion=conversion,
            current_table_name=row['REFERENCED_TABLE_NAME'],
            should_get_original=False
        )

        original_referenced_table_name = ExtraConfigProcessor.get_table_name(
            conversion=conversion,
            current_table_name=row['REFERENCED_TABLE_NAME'],
            should_get_original=True
        )

        current_referenced_column_name = ExtraConfigProcessor.get_column_name(
            conversion=conversion,
            original_table_name=original_referenced_table_name,
            current_column_name=row['REFERENCED_COLUMN_NAME'],
            should_get_original=False
        )

        if row['CONSTRAINT_NAME'] in constraints:
            constraint_column_name = cast(list[str], constraints[row['CONSTRAINT_NAME']]['column_name'])
            constraint_referenced_column_name = cast(
                list[str],
                constraints[row['CONSTRAINT_NAME']]['referenced_column_name']
            )

            constraint_column_name.append(f'"{current_column_name}"')
            constraint_referenced_column_name.append(f'"{current_referenced_column_name}"')
            return

        constraints[row['CONSTRAINT_NAME']] = {}
        constraints[row['CONSTRAINT_NAME']]['column_name'] = [f'"{current_column_name}"']
        constraints[row['CONSTRAINT_NAME']]['referenced_column_name'] = [f'"{current_referenced_column_name}"']
        constraints[row['CONSTRAINT_NAME']]['referenced_table_name'] = current_referenced_table_name
        constraints[row['CONSTRAINT_NAME']]['update_rule'] = row['UPDATE_RULE']
        constraints[row['CONSTRAINT_NAME']]['delete_rule'] = row['DELETE_RULE']

    params = [[conversion, constraints, foreign_key, table_name] for foreign_key in constraints.keys()]
    conversion.run_concurrently(func=_set_single_foreign_key, params_list=params)


def _set_single_foreign_key(
    conversion: Conversion,
    constraints: dict,
    foreign_key: dict,
    table_name: str
) -> None:
    """
    Creates a single foreign key.
    """
    foreign_key_column_names = ','.join(constraints[foreign_key]['column_name'])
    referenced_table_name = constraints[foreign_key]['referenced_table_name']
    referenced_column_names = ','.join(constraints[foreign_key]['referenced_column_name'])
    update_rule = constraints[foreign_key]['update_rule']
    delete_rule = constraints[foreign_key]['delete_rule']
    sql = (f'ALTER TABLE "{conversion.schema}"."{table_name}" ADD FOREIGN KEY ({foreign_key_column_names})'
           f' REFERENCES "{conversion.schema}"."{referenced_table_name}"({referenced_column_names})'
           f' ON UPDATE {update_rule} ON DELETE {delete_rule};')

    DBAccess.query(
        conversion=conversion,
        caller=_set_single_foreign_key.__name__,
        sql=sql,
        vendor=DBVendor.PG,
        process_exit_on_error=False,
        should_return_client=False
    )
