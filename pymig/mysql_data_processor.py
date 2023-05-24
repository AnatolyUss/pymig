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


def process_mysql_data(batch: tuple[tuple[str, ...], ...]) -> str:
    """
    Accepts a batch of records from ``MySQLdb``,
    and returns a string in TSV format (TSV uses tab instead of comma).
    """
    # Note, the list comprehension below wrapped in square brackets on purpose.
    # DO NOT strip the brackets, since it will work slower.
    return '\n'.join(['\t'.join(record) for record in batch])
