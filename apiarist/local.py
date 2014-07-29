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
"""Class to manage local job execution
"""
import os
import subprocess
import hashlib
import time
import shutil
import logging

logger = logging.getLogger(__name__)


class LocalRunner():
    """Handles running the Hive script on
    a local Hive installation.
    """

    def __init__(self, job_name=None, input_path=None,
                 hive_query=None, output_dir=None):

        #  TODO test for Hive installation

        self.job_name = job_name
        self.job_id = self._generate_job_id()
        self.start_time = time.time()

        # I/O for job data
        tmp_path = os.environ['APIARIST_TMP_DIR'] + self.job_id
        self.data_path = tmp_path + '.data'
        self.table_path = tmp_path + '-table'
        self.input_path = os.path.abspath(input_path)
        if output_dir:
            self.output_dir = os.path.abspath(output_dir) + '/' + self.job_id
        else:
            self.output_dir = tmp_path + '-output'

        # the Hive script object
        self.hive_query = hive_query
        self.local_script_file = tmp_path + '.hql'

    def run(self):
        """Run the hive query against a local hive installation (*nix only)
        """
        # prepare files
        self._copy_input_data()
        self._generate_hive_script()
        # execute against hive server
        cmd = ["hive -f {}".format(self.local_script_file)]
        logger.info("running HIVE script with: {}".format(cmd))
        hql = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        stdout = hql.communicate()
        if stdout[1] is not None:
            logger.info(stdout)
        # observe and report
        self._wait_for_job_to_complete()

    def _copy_input_data(self):
        shutil.copyfile(self.input_path, self.data_path)

    def _wait_for_job_to_complete(self):
        # TODO - wait until there are files in this dir
        cmd = ["cat {}/*".format(self.output_dir)]
        cat = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, shell=True)
        stdout, stderr = cat.communicate()
        logger.info("\nQuery output ------->\n")
        print(stdout)  # query results to STDOUT

    def _generate_hive_script(self):
        """Write the HQL to a local (temp) file
        """
        hq = self.hive_query.local_hive_script(self.data_path,
                                               self.output_dir,
                                               self.table_path)
        f = open(self.local_script_file, 'w')
        f.writelines(hq)
        f.close()

    def _generate_job_id(self):
        """Create a unique job run identifier
        """
        run_id = self.job_name + str(time.time())
        digest = hashlib.md5(run_id).hexdigest()
        return 'hj-' + digest

    #  hooks for the with statement ###

    def __enter__(self):
        """Don't do anything special at start of with block"""
        s = self
        return s

    def __exit__(self, type, value, traceback):
        """Call self.cleanup() at end of with block."""
        self.cleanup()

    def cleanup(self):
        # TODO _ remove scratch dirs?
        pass
