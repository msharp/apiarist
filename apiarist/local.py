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
import sys
import subprocess
import hashlib
import time
import logging

logger = logging.getLogger(__name__)


class LocalRunner():

    def __init__(self, job_name=None, input_path=None, 
            hive_query=None, output_dir=None):

        self.job_name = job_name
        self.job_id = self._generate_job_id()
        self.start_time = time.time()

        # I/O for job data
        self.input_path = os.path.abspath(input_path)
        self.output_dir = output_dir + self.job_id +'.csv'
        self.table_path  = os.environ['APIARIST_TMP_DIR'] + self.job_id +'.table'

        # the Hive script object
        self.hive_query = hive_query
        print self.hive_query.local_hive_script(self.input_path, self.output_dir, self.table_path)
        self.local_script_file = os.environ['APIARIST_TMP_DIR'] + self.job_id +'.hql'

    def run(self):
        """Run the hive query against a local hive installation (*nix only)
        """
        query = self.hive_query.local_hive_script(self.input_path, self.output_dir, self.table_path)

        self._generate_hive_script()
        cmd = ["hive -f {}".format(self.local_script_file)]
        print "running HIVE script with: {}".format(cmd)
        hql = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        stdout = hql.communicate()

        self._wait_for_job_to_complete()

    def _wait_for_job_to_complete(self):
        # TODO - wait until there are files in this dir
        cmd = ["cat {}/*".format(self.output_dir)]
        cat = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        stdout, stderr = cat.communicate()
        print stdout
 
    def _generate_hive_script(self):
        """Write the HQL to a local (temp) file
        """
        hq = self.hive_query.local_hive_script(self.input_path, self.output_dir, self.table_path)
        f = open(self.local_script_file,'w')
        f.writelines(hq)
        f.close()
 
    def _generate_job_id(self):
        """Create a unique job run identifier
        """
        run_id = self.job_name + str(time.time())
        digest = hashlib.md5(run_id).hexdigest()
        return 'hj-' + digest
   
    ### hooks for the with statement ###

    def __enter__(self):
        """Don't do anything special at start of with block"""
        s = self
        print s.job_id
        return s

    def __exit__(self, type, value, traceback):
        """Call self.cleanup() at end of with block."""
        self.cleanup()

    def cleanup(self):
        # TODO _ remove scratch dirs?
        print "cleaning up ... "

