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


class Utils:
    @staticmethod
    def get_index_of(needle, haystack):
        """
        Returns an index of given needle in the haystack.
        The needle can be a variable of any type.
        The haystack is either a list or a string.
        If the needle not found - returns -1.
        :param needle: variable of any type
        :param haystack: list or string
        :return: int
        """
        try:
            return haystack.index(needle)
        except ValueError:
            return -1
