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

import time
import math
from FsOps import FsOps


class ReportGenerator:
    @staticmethod
    def generate_report(conversion, last_message):
        """
        Generates a summary report.
        :param conversion: Conversion
        :param last_message: string
        :return: None
        """
        log_title = 'ReportGenerator::generateReport'
        difference_sec = (time.time() - conversion.time_begin)
        seconds = math.floor(difference_sec % 60)
        difference_sec /= 60
        minutes = math.floor(difference_sec % 60)
        hours = math.floor(difference_sec / 60)
        formatted_hours = '0%d' % hours if hours < 10 else '%d' % hours
        formatted_minutes = '0%d' % minutes if minutes < 10 else '%d' % minutes
        formatted_seconds = '0%d' % seconds if seconds < 10 else '%d' % seconds
        output = '\t--[{0}] {1}\n\t--[{0}] Total time: {2}:{3}:{4}\n\t--[{0}] (hours:minutes:seconds)'\
            .format(log_title, last_message, formatted_hours, formatted_minutes, formatted_seconds)

        FsOps.log(conversion, output)
