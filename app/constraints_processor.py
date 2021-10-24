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
import app.migration_state_manager as MigrationStateManager
from app.conversion import Conversion
from app.indexes_processor import create_indexes
from app.concurrency_manager import run_concurrently
from app.enum_processor import process_enum
from app.sequences_processor import set_sequence_value, create_sequence
from app.null_processor import process_null
from app.default_processor import process_default
from app.comments_processor import process_comments
from app.view_generator import generate_views
from app.foreign_key_processor import set_foreign_keys


def process_constraints(conversion: Conversion) -> None:
    """
    Continues migration process after data loading.
    """
    are_table_constraints_loaded = MigrationStateManager.get(conversion, 'per_table_constraints_loaded')

    if not are_table_constraints_loaded:
        params = [[conversion, table_name] for table_name in conversion.tables_to_migrate]
        run_concurrently(conversion, process_constraints_per_table, params)

    if conversion.should_migrate_only_data():
        MigrationStateManager.set(conversion, 'per_table_constraints_loaded', 'foreign_keys_loaded', 'views_loaded')
    else:
        MigrationStateManager.set(conversion, 'per_table_constraints_loaded')
        set_foreign_keys(conversion)
        MigrationStateManager.set(conversion, 'foreign_keys_loaded')
        generate_views(conversion)
        MigrationStateManager.set(conversion, 'views_loaded')

    # !!!Note, dropping of data - pool and state - logs tables MUST be the last step of migration process.
    MigrationStateManager.drop_data_pool_table(conversion)
    MigrationStateManager.drop_state_logs_table(conversion)


def process_constraints_per_table(conversion: Conversion, table_name: str) -> None:
    """
    Processes given table's constraints.
    """
    if conversion.should_migrate_only_data():
        return set_sequence_value(conversion, table_name)

    process_enum(conversion, table_name)
    process_null(conversion, table_name)
    process_default(conversion, table_name)
    create_sequence(conversion, table_name)
    create_indexes(conversion, table_name)
    process_comments(conversion, table_name)
