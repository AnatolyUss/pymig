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
from ConcurrencyManager import ConcurrencyManager


class ConstraintsProcessor:
    @staticmethod
    def processConstraints(conversion):
        """
        Continues migration process after data loading.
        :param conversion: Conversion
        :return: None
        """
        is_table_constraints_loaded = MigrationStateManager.get(conversion, 'per_table_constraints_loaded')

        if not is_table_constraints_loaded:
            params = [[conversion, table_name] for table_name in conversion.tables_to_migrate]
            ConcurrencyManager.run_in_parallel(conversion, ConstraintsProcessor._processConstraintsPerTable, params)

        # TODO: proceed to foreign keys setting.

    @staticmethod
    def _processConstraintsPerTable(conversion, table_name):
        """
        Processes given table's constraints.
        :param conversion: Conversion
        :param table_name: string
        :return: None
        """
        if conversion.should_migrate_only_data():
            # return sequencesProcessor.setSequenceValue(conversion, tableName);

        # await processEnum(conversion, tableName);
        # await processNull(conversion, tableName);
        # await processDefault(conversion, tableName);
        # await sequencesProcessor.createSequence(conversion, tableName);
        # await processIndexAndKey(conversion, tableName);
        # await processComments(conversion, tableName);
