# Copyright 2014 Max Sharples
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Class to inherit your HiveJobs from. See README for more info
"""
import logging
import re

from apiarist.launch import HiveJobLauncher
from apiarist.script import HiveQuery
from apiarist.conf import _READ_ARGS_FROM_SYS_ARGV

log = logging.getLogger(__name__)


class HiveJob(HiveJobLauncher):

    def __init__(self, args=None):
        super(HiveJob, self).__init__(self._job_name(), args)

    def hive_query(self):
        """Get the Hive script object based on provided params
        """
        return HiveQuery(self)

    def plain_query(self):
        """Condense spaces"""
        return re.sub(r"\s+", " ", self.query()).strip()

    def _job_name(self):
        return self.__class__.__name__

    @classmethod
    def run(cls):
        """Entry point for running job from the command-line.
        """
        hive_job = cls(args=_READ_ARGS_FROM_SYS_ARGV)
        hive_job.execute()

    #  methods which define the query logic and I/O

    def input_columns(self):
        """Create this in your HiveJob subclass"""
        raise NotImplementedError

    def output_columns(self):
        """Create this in your HiveJob subclass"""
        raise NotImplementedError

    def table(self):
        """Create this in your HiveJob subclass"""
        raise NotImplementedError

    def query(self):
        """Create this in your HiveJob subclass"""
        raise NotImplementedError
