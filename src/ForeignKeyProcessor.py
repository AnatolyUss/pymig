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
from MigrationStateManager import MigrationStateManager
from ExtraConfigProcessor import ExtraConfigProcessor
from ConcurrencyManager import ConcurrencyManager
from DBAccess import DBAccess
from FsOps import FsOps
import DBVendors


class ForeignKeyProcessor:
    @staticmethod
    def set_foreign_keys(conversion):
        """
        TODO: add description.
        :param conversion: Conversion
        :return: None
        """
        foreign_keys_processed = MigrationStateManager.get(conversion, 'foreign_keys_loaded')

        if foreign_keys_processed:
            return

        params = [[conversion, table_name] for table_name in conversion.tables_to_migrate]
        ConcurrencyManager.run_in_parallel(conversion, ForeignKeyProcessor._set_foreign_key_for_table, params)

    @staticmethod
    def _set_foreign_key_for_table(conversion, table_name):
        """
        TODO: add description.
        :param conversion: Conversion
        :param table_name: str
        :return: None
        """
        log_title = 'ForeignKeyProcessor::_set_foreign_key_for_table'
        msg = '\t--[%s] Search foreign keys for table "%s"."%s"...' % (log_title, conversion.schema, table_name)
        FsOps.log(conversion, msg)
        sql = """
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
            WHERE cols.TABLE_SCHEMA = '%s' AND cols.TABLE_NAME = '%s';
        """ % (
            conversion.mysql_db_name,
            ExtraConfigProcessor.get_table_name(conversion, table_name, should_get_original=True)
        )

        result = DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql,
            vendor=DBVendors.MYSQL,
            process_exit_on_error=False,
            should_return_client=False
        )

        if result.error:
            return

        extra_rows = ExtraConfigProcessor.parse_foreign_keys(conversion, table_name)
        full_rows = (result.data or []) + extra_rows
        # await processForeignKeyWorker(conversion, tableName, fullRows);
        msg = '\t--[%s] Foreign keys for table "%s"."%s" are set...' % (log_title, conversion.schema, table_name)
        FsOps.log(conversion, msg)

    @staticmethod
    def _set_foreign_key(conversion, table_name, rows):
        """
        TODO: add description.
        :param conversion: Conversion
        :param table_name: str
        :param rows: list
        :return: None
        """
        constraints = {}
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
                constraints[row['CONSTRAINT_NAME']]['column_name'].append('"%s"' % current_column_name)
                constraints[row['CONSTRAINT_NAME']]['referenced_column_name'].append(
                    '"%s"' % current_referenced_column_name
                )

                return

            constraints[row['CONSTRAINT_NAME']] = {}
            constraints[row['CONSTRAINT_NAME']]['column_name'] = ['"%s"' % current_column_name]
            constraints[row['CONSTRAINT_NAME']]['referenced_column_name'] = ['"%s"' % current_referenced_column_name]
            constraints[row['CONSTRAINT_NAME']]['referenced_table_name'] = current_referenced_table_name
            constraints[row['CONSTRAINT_NAME']]['update_rule'] = row['UPDATE_RULE']
            constraints[row['CONSTRAINT_NAME']]['delete_rule'] = row['DELETE_RULE']

        params = [[conversion, constraints, foreign_key, table_name] for foreign_key in constraints.keys()]
        ConcurrencyManager.run_in_parallel(conversion, ForeignKeyProcessor._set_single_foreign_key, params)

    @staticmethod
    def _set_single_foreign_key(conversion, constraints, foreign_key, table_name):
        """
        TODO: add description.
        :param conversion: Conversion
        :param constraints: dict
        :param foreign_key: dict
        :param table_name: str
        :return: None
        """
        log_title = 'ForeignKeyProcessor::_set_single_foreign_key'
        sql = '''
            ALTER TABLE "{0}"."{1}" ADD FOREIGN KEY ({2}})
            REFERENCES "{0}"."{3}"({4})
            ON UPDATE {5}
            ON DELETE {6};
        '''.format(
            conversion.schema,
            table_name,
            ','.join(constraints[foreign_key]['column_name']),
            constraints[foreign_key]['referenced_table_name'],
            ','.join(constraints[foreign_key]['referenced_column_name']),
            constraints[foreign_key]['update_rule'],
            constraints[foreign_key]['delete_rule']
        )

        DBAccess.query(
            conversion=conversion,
            caller=log_title,
            sql=sql,
            vendor=DBVendors.PG,
            process_exit_on_error=False,
            should_return_client=False
        )
