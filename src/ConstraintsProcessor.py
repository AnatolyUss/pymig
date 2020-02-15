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
from IndexesProcessor import IndexesProcessor
from MigrationStateManager import MigrationStateManager
from ConcurrencyManager import ConcurrencyManager
from SequencesProcessor import SequencesProcessor
from EnumProcessor import EnumProcessor
from NullProcessor import NullProcessor
from DefaultProcessor import DefaultProcessor
from CommentsProcessor import CommentsProcessor
from ReportGenerator import ReportGenerator


class ConstraintsProcessor:
    @staticmethod
    def process_constraints(conversion):
        """
        Continues migration process after data loading.
        :param conversion: Conversion
        :return: None
        """
        is_table_constraints_loaded = MigrationStateManager.get(conversion, 'per_table_constraints_loaded')

        if not is_table_constraints_loaded:
            params = [[conversion, table_name] for table_name in conversion.tables_to_migrate]
            ConcurrencyManager.run_in_parallel(conversion, ConstraintsProcessor._process_constraints_per_table, params)

        if conversion.should_migrate_only_data():
            MigrationStateManager.set(conversion, 'per_table_constraints_loaded', 'foreign_keys_loaded', 'views_loaded')
        else:
            MigrationStateManager.set(conversion, 'per_table_constraints_loaded')
            # await processForeignKey(conversion);  # TODO: implement.
            MigrationStateManager.set(conversion, 'foreign_keys_loaded')
            # await processViews(conversion);  # TODO: implement.
            MigrationStateManager.set(conversion, 'views_loaded')

        # Reclaim storage occupied by dead tuples.
        # await runVacuumFullAndAnalyze(conversion);  # TODO: implement.

        # !!!Note, dropping of data - pool and state - logs tables MUST be the last step of migration process.
        # await dataPoolManager.dropDataPoolTable(conversion);  # TODO: implement.
        # await migrationStateManager.dropStateLogsTable(conversion);  # TODO: implement.
        ReportGenerator.generate_report(conversion, 'Migration is accomplished.')

    @staticmethod
    def _process_constraints_per_table(conversion, table_name):
        """
        Processes given table's constraints.
        :param conversion: Conversion
        :param table_name: str
        :return: None
        """
        if conversion.should_migrate_only_data():
            return SequencesProcessor.set_sequence_value(conversion, table_name)

        EnumProcessor.process_enum(conversion, table_name)
        NullProcessor.process_null(conversion, table_name)
        DefaultProcessor.process_default(conversion, table_name)
        SequencesProcessor.create_sequence(conversion, table_name)
        IndexesProcessor.create_indexes(conversion, table_name)
        CommentsProcessor.process_comments(conversion, table_name)
