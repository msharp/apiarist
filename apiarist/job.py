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
import sys
import os
import re
import hashlib
import time
import boto
from boto.emr.step import HiveStep
from boto.emr.step import InstallHiveStep
from boto.emr.connection import EmrConnection
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from script import HiveQuery

log = logging.getLogger(__name__)


class HiveJob(object):

    def __init__(self):
        self.job_id = self._generate_job_id()
        self.start_time = time.time()
        self._job_name = self.job_name()

        # S3 - until we can run locally
        self.base_path      = os.environ['S3_BASE_PATH']
        self.output_path    = self.base_path + self.job_id + '/output/' 
        self.data_path      = self.base_path + self.job_id + '/data/'
        self.table_path     = self.base_path + self.job_id + '/tables/' 
        self.script_path    = self.base_path + self.job_id + '/script.hql' 
        self.log_path       = self.base_path + 'logs/' # TODO allow alternate logging path

        self.local_script_file = os.environ['APIARIST_TMP_DIR'] + self.job_id +'.hql'

        self.hive_query = HiveQuery(self.table(), self.input_columns(), self.output_columns(), self.query())
        
        print("Initialising job {0} with jobID {1} at {2}".format(self.job_name(), self.job_id, self.start_time))

    def generate_hive_script(self, data_source):
        """Write the HQL to a local (temp) file
        """
        hq = self.hive_query.emr_hive_script(data_source, self.output_path, self.table_path)
        f = open(self.local_script_file,'w')
        f.writelines(hq)
        f.close()

    def job_name(self):
        return self.__class__.__name__

    def _generate_job_id(self):
        """Create a unique job run identifier
        """
        run_id = self.job_name() + str(time.time())
        digest = hashlib.md5(run_id).hexdigest()
        return 'hj-' + digest
   
    def _generate_and_upload_hive_script(self, data_source):
        self.generate_hive_script(data_source)
        self._upload_file_to_s3(self.local_script_file, self.script_path)

    def run(self, data_source):

        # TODO _ copy the data source to a nuw object (Hive deletes/moves the original)

        self._generate_and_upload_hive_script(data_source)

        # TODO wait 5 secs for S3 consistency 
        print("Waiting 5s for S3 eventual consistency")
        time.sleep(5)

        conn = EmrConnection(os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])

        setup_step = InstallHiveStep()
        run_step = HiveStep(self.job_name(), self.script_path)

        jobid = conn.run_jobflow(
                            self.job_name(),
                            self.log_path,
                            action_on_failure='CANCEL_AND_WAIT',
                            master_instance_type='m3.xlarge',
                            slave_instance_type='m3.xlarge',
                            ami_version='2.0',
                            num_instances=2)

        conn.add_jobflow_steps(jobid, [setup_step, run_step])

        self._wait_for_job_to_complete(conn, jobid)
 
    ############################

    # S3/storage utility methods

    def _upload_file_to_s3(self, file_path, s3_path):
        s3_bucket, s3_key = self._parse_s3_uri(s3_path)
        conn = S3Connection(os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])
        bkt = conn.get_bucket(s3_bucket)
        k = Key(bkt)
        k.key = s3_key
        k.set_contents_from_filename(file_path)       
    
    def _parse_s3_uri(self,uri):
        """Parse the uri from the for: s3://bucketname/some/other/path/info/
        to: 
            bucket = bucketname
            key = some/other/path/info
        """
        m = re.search(r'(s3://)([A-Za-z0-9_-]+)/(\S*)', uri)
        if m:
            return (m.group(2),m.group(3))
        else:
            return None
  

    # ############################
    # cribbed from mrjob
    # ###########################
        
    # wait for job and log status (from mrjob)
    def _wait_for_job_to_complete(self, conn, jobid):
        """Wait for the job to complete, and raise an exception if
        the job failed.

        Also grab log URI from the job status (since we may not know it)
        """
        success = False
        opts = {'check_emr_status_every': 30}
        s3_logs = self.log_path
        emr_job_start = self.start_time

        while True:
            # don't antagonize EMR's throttling
            print('Waiting %.1f seconds...' % opts['check_emr_status_every'])
            time.sleep(opts['check_emr_status_every'])

            job_flow = conn.describe_jobflow(jobid)

            job_state = job_flow.state
            reason = getattr(job_flow, 'laststatechangereason', '')

            # find all steps belonging to us, and get their state
            step_states = []
            running_step_name = ''
            total_step_time = 0.0
            step_nums = []  # step numbers belonging to us. 1-indexed
            lg_step_num_mapping = {}

            steps = job_flow.steps or []
            latest_lg_step_num = 0
            for i, step in enumerate(steps):
                #if LOG_GENERATING_STEP_NAME_RE.match(posixpath.basename(getattr(step, 'jar', ''))):
                #    latest_lg_step_num += 1

                # ignore steps belonging to other jobs
                if not step.name.startswith(self._job_name):
                    continue

                step_nums.append(i + 1)
                #if LOG_GENERATING_STEP_NAME_RE.match(posixpath.basename(getattr(step, 'jar', ''))):
                #    lg_step_num_mapping[i + 1] = latest_lg_step_num

                step.state = step.state
                step_states.append(step.state)
                if step.state == 'RUNNING':
                    running_step_name = step.name

                if (hasattr(step, 'startdatetime') and hasattr(step, 'enddatetime')):
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

            # otherwise, we can print a status message
            if running_step_name:
                print("Job launched {0} ago, status {1}: {2} ({3})".format(running_time, job_state, reason, running_step_name))

                #if self._show_tracker_progress:
                #    try:
                #        tracker_handle = urllib2.urlopen(self._tracker_url)
                #        tracker_page = ''.join(tracker_handle.readlines())
                #        tracker_handle.close()
                #        # first two formatted percentages, map then reduce
                #        map_complete, reduce_complete = [
                #            float(complete) for complete
                #            in JOB_TRACKER_RE.findall(tracker_page)[:2]]
                #        print(' map %3d%% reduce %3d%%' % (
                #                 map_complete, reduce_complete))
                #    except:
                #        print('Unable to load progress from job tracker')
                #        # turn off progress for rest of job
                #        self._show_tracker_progress = False
                # once a step is running, it's safe to set up the ssh tunnel to
                # the job tracker
                #job_host = getattr(job_flow, 'masterpublicdnsname', None)
                #if job_host and opts['ssh_tunnel_to_job_tracker']:
                #    self.setup_ssh_tunnel_to_job_tracker(job_host)

            # other states include STARTING and SHUTTING_DOWN
            elif reason:
                print("Job launched {0} ago, status {1}: {2}".format(running_time, job_state, reason))
            else:
                print("Job launched {0} ago, status {1}".format(running_time, job_state))

        if success:
            print('Job completed.')
            print("Running time was {0} (not counting time spent waiting for the EC2 instances)".format(total_step_time))
            #self._fetch_counters(step_nums, lg_step_num_mapping)
            #self.print_counters(range(1, len(step_nums) + 1))
        else:
            msg = 'Job on job flow %s failed with status %s: %s' % (
                job_flow.jobflowid, job_state, reason)
            print(msg)
            #if self._s3_job_log_uri:
            #    print('Logs are in %s' % self._s3_job_log_uri)
            # look for a Python traceback
            #cause = self._find_probable_cause_of_failure(
            cause = False
            #    step_nums, sorted(lg_step_num_mapping.values()))
            if cause:
                # log cause, and put it in exception
                cause_msg = []  # lines to log and put in exception
                cause_msg.append('Probable cause of failure (from %s):' %
                                 cause['log_file_uri'])
                cause_msg.extend(line.strip('\n') for line in cause['lines'])
                if cause['input_uri']:
                    cause_msg.append('(while reading from %s)' %
                                     cause['input_uri'])

                for line in cause_msg:
                    print(line)

                # add cause_msg to exception message
                msg += '\n' + '\n'.join(cause_msg) + '\n'

            raise Exception(msg)

### AWS Date-time parsing ###

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
