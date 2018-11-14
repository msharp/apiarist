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
"""
Class to manage EMR config and job running
"""
import os
import getpass
import hashlib
import re
import time
import datetime
import logging
import six

import boto
from boto.emr.step import HiveStep
from boto.emr.step import InstallHiveStep
from boto.emr.connection import EmrConnection
from apiarist.s3 import copy_s3_file, is_dir, upload_file_to_s3
from apiarist.script import generate_hive_script_file, get_script_file_location

logger = logging.getLogger(__name__)


class EMRRunner():

    def __init__(self, job_name=None, input_path=None, hive_query=None,
                 output_dir=None, scratch_uri=None, log_path=None,
                 ami_version=None, hive_version=None, num_instances=None,
                 master_instance_type=None, slave_instance_type=None,
                 iam_instance_profile=None, iam_service_role=None,
                 aws_access_key_id=None, aws_secret_access_key=None,
                 visible_to_all_users=None,
                 s3_sync_wait_time=5, check_emr_status_every=30,
                 label=None, owner=None, temp_dir=None):

        self.job_name = job_name
        self.job_id = self._generate_job_id()
        self.start_time = time.time()

        # AWS credentials can come from arguments or environment
        # set AWS credentials if supplied (override ENV)
        if aws_access_key_id:
            os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
            self.aws_access_key_id = aws_access_key_id
        else:
            logger.debug("Getting AWS access key from ENV")
            self.aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']

        if aws_secret_access_key:
            os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key
            self.aws_secret_access_key = aws_secret_access_key
        else:
            logger.debug("Getting AWS secret key from ENV")
            self.aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

        # Set visibility of job flow to all users if visible_to_all_users is None
        if visible_to_all_users is None:
            self.visible_to_all_users = True
        else:
            self.visible_to_all_users = visible_to_all_users

        self.s3_sync_wait_time = s3_sync_wait_time
        self.check_emr_status_every = check_emr_status_every

        # I/O for job data
        self.input_path = input_path
        self.output_dir = output_dir

        # is the input multiple files in a 'directory'?
        try:
            self.input_is_dir = is_dir(input_path)
        except TypeError:
            self.input_is_dir = False

        # the Hive script object
        self.hive_query = hive_query

        #  EMR options
        self.master_instance_type = master_instance_type
        self.slave_instance_type = slave_instance_type
        self.ami_version = ami_version
        self.hive_version = hive_version
        self.num_instances = num_instances
        self.iam_instance_profile = iam_instance_profile
        self.iam_service_role = iam_service_role
        self.label = label
        self.owner = owner

        # give this job a unique name
        self.job_key = self._make_unique_job_key(
            label=self.label, owner=self.owner)

        # S3 'scratch' directory
        if scratch_uri:
            self.base_path = scratch_uri
            os.environ['S3_SCRATCH_URI'] = scratch_uri
        else:
            self.base_path = os.environ['S3_SCRATCH_URI']

        # allow alternate logging path
        self.log_path = log_path or self.base_path + 'logs/'
        # other temp files live in a jobID bucket
        self.job_files = self.base_path + self.job_id + '/'
        self.data_path = self.job_files + 'data'
        if self.input_is_dir:
            self.data_path += '/'
        self.table_path = self.job_files + 'tables/'
        self.script_path = self.job_files + 'script.hql'
        self.output_path = self.output_dir or self.job_files + 'output/'

        # a local temp dir is used to write the script
        self.local_script_file = get_script_file_location(self.job_id,
                                                          temp_dir)

        logger.info("JobID {0}, started at {1}".format(self.job_id,
                                                       self.start_time))

    def _generate_hive_script(self, data_source):
        """Write the HQL to a local (temp) file
        """
        hq = self.hive_query.emr_hive_script(data_source, self.output_path,
                                             self.table_path)
        generate_hive_script_file(hq, self.local_script_file)

    def _generate_job_id(self):
        """Create a unique job run identifier
        """
        run_id = self.job_name + str(time.time())
        digest = hashlib.md5(six.b(run_id)).hexdigest()
        return 'hj-' + digest

    def _make_unique_job_key(self, label=None, owner=None):
        """Come up with a useful unique ID for this job
        """
        # use the name of the job as the label if one wasn't explicitly
        # specified
        if not label:
            label = self.job_name

        if not owner:
            try:
                owner = getpass.getuser()
            except:
                owner = 'no_user'

        now = datetime.datetime.utcnow()
        return '%s.%s.%s.%06d' % (
            label, owner,
            now.strftime('%Y%m%d.%H%M%S'), now.microsecond)

    def _generate_and_upload_hive_script(self):
        self._generate_hive_script(self.data_path)
        upload_file_to_s3(self.local_script_file, self.script_path)

    #  hooks for the with statement ###

    def __enter__(self):
        """Don't do anything special at start of with block"""
        return self

    def __exit__(self, type, value, traceback):
        """Call self.cleanup() at end of with block."""
        self.cleanup()

    def run(self):
        """Run the Hive job on EMR cluster
        """
        #  copy the data source to a new object
        #  (Hive deletes/moves the original)
        copy_s3_file(self.input_path, self.data_path)

        # and create the hive script
        self._generate_and_upload_hive_script()

        logger.info("Waiting {} seconds for S3 eventual consistency".format(
                    self.s3_sync_wait_time))
        time.sleep(self.s3_sync_wait_time)

        # TODO more options like setting aws region
        conn = EmrConnection(self.aws_access_key_id,
                             self.aws_secret_access_key)

        setup_step = InstallHiveStep(self.hive_version)
        run_step = HiveStep(self.job_name, self.script_path)

        cluster_id = conn.run_jobflow(
            self.job_key,
            self.log_path,
            action_on_failure='CANCEL_AND_WAIT',
            master_instance_type=self.master_instance_type,
            slave_instance_type=self.slave_instance_type,
            ami_version=self.ami_version,
            num_instances=self.num_instances,
            job_flow_role=self.iam_instance_profile,
            service_role=self.iam_service_role,
            visible_to_all_users=self.visible_to_all_users)

        conn.add_jobflow_steps(cluster_id, [setup_step, run_step])

        logger.info("Job started on cluster {0}".format(cluster_id))

        self._wait_for_job_to_complete(conn, cluster_id)

        logger.info("Output file is in: {0}".format(self.output_path))

    def cleanup(self):
        # TODO _ remove scratch dirs?
        logger.info("cleaning up ... ")

    # wait for job and log status
    # this method extracted from mrjob.job
    def _wait_for_job_to_complete(self, conn, cluster_id):
        """
        Wait for the job to complete, and raise an exception if
        the job failed.

        Also grab log URI from the job status (since we may not know it)
        """
        success = False
        chk_status_freq = self.check_emr_status_every
        # opts = {'check_emr_status_every': 30}
        # s3_logs = self.log_path
        emr_job_start = self.start_time

        while True:
            # don't antagonize EMR's throttling
            logger.debug('Waiting {0} seconds'.format(chk_status_freq))
            time.sleep(chk_status_freq)

            cluster = conn.describe_cluster(cluster_id)

            job_state = cluster.status.state
            reason = getattr(
                getattr(cluster.status, 'statechangereason', None),
                'message', '')

            # find all steps belonging to us, and get their state
            step_states = []
            running_step_name = ''
            total_step_time = 0.0
            step_nums = []  # step numbers belonging to us. 1-indexed

            steps = conn.list_steps(cluster.id).steps or []
            for i, step in enumerate(steps):

                # ignore steps belonging to other jobs
                if not step.name.startswith(self.job_name):
                    continue

                step_nums.append(i + 1)

                step_states.append(step.status.state)
                if step.status.state == 'RUNNING':
                    running_step_name = step.name

                if hasattr(step, 'startdatetime') and \
                   hasattr(step, 'enddatetime'):

                    start_time = iso8601_to_timestamp(step.startdatetime)
                    end_time = iso8601_to_timestamp(step.enddatetime)
                    total_step_time += end_time - start_time

            if not step_states:
                raise AssertionError("Can't find our steps in the job flow!")

            # if all our steps have completed, we're done!
            if all(state == 'COMPLETED' for state in step_states):
                success = True
                break

            # if any step fails, give up
            if any(state in ('FAILED', 'CANCELLED') for state in step_states):
                break

            # (the other step states are PENDING and RUNNING)

            # keep track of how long we've been waiting
            running_time = time.time() - emr_job_start

            # logging message values
            loginfo = (int(running_time), job_state, reason, running_step_name)

            # otherwise, we can print a status message
            if running_step_name:
                logger.info("Job launched {0} ago. "
                            " Status {1}: {2} ({3})".
                            format(*loginfo))

            # other states include STARTING and SHUTTING_DOWN
            elif reason:
                logger.info("Job launched {0} ago. "
                            "Status {1}: {2}".format(*loginfo))
            else:
                logger.info("Job launched {0} ago. "
                            "Status {1}".format(*loginfo))

        if success:
            logger.info("Job completed on cluster {}.".format(cluster.id))
            logger.info("Running time was {0} "
                        "(excludes time spent waiting for the EC2 instances)".
                        format(total_step_time))
        else:
            msg = "Job on cluster {0} failed with status {1}: {2}".format(
                  cluster.id, job_state, reason)
            logger.info(msg)

            raise Exception(msg)


#  AWS Date-time parsing

# sometimes AWS gives us seconds as a decimal, which we can't parse
# with boto.utils.ISO8601
SUBSECOND_RE = re.compile('\.[0-9]+')

# Thu, 29 Mar 2012 04:55:44 GMT
RFC1123 = '%a, %d %b %Y %H:%M:%S %Z'


def iso8601_to_timestamp(iso8601_time):
    iso8601_time = SUBSECOND_RE.sub('', iso8601_time)
    try:
        return time.mktime(time.strptime(iso8601_time, boto.utils.ISO8601))
    except ValueError:
        return time.mktime(time.strptime(iso8601_time, RFC1123))


def iso8601_to_datetime(iso8601_time):
    iso8601_time = SUBSECOND_RE.sub('', iso8601_time)
    try:
        return datetime.strptime(iso8601_time, boto.utils.ISO8601)
    except ValueError:
        return datetime.strptime(iso8601_time, RFC1123)
