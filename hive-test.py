import sys
import os
import re
import hashlib
import time
import boto
from boto.emr.step import InstallHiveStep, HiveStep
from boto.emr.connection import EmrConnection

# temp dir ??
os.environ['APIARIST_TMP_DIR'] = '/Users/max/Code/apiarist/temp/'
   

class HiveJob(object):

    def __init__(self):
        self.job_id = self._generate_job_id()
        # S3 - until we can run locally
        self.base_path = 's3://sauropod-test/hive/'
        self.output_path = self.base_path + 'output/' + self.job_id +'/'
        self.table_path = self.base_path + 'tables/' + self.job_id +'/'
        self.log_path = self.base_path + 'logs/'

        # args = ['--base-path', s3_output]
        self.hive_query = HiveQuery(self.table(), self.input_columns(), self.output_columns(), self.query())
        

    def generate_hive_script(self, source_data):
        hq = self.hive_query.emr_hive_script(source_data, self.output_path, self.table_path)
        return hq

    def job_name(self):
        return self.__class__.__name__

    def _generate_job_id(self):
        run_id = self.job_name() + str(time.time())
        digest = hashlib.md5(run_id).hexdigest()
        return 'hj-' + digest
    
    def _generate_and_upload_hive_script(self):
        hive_script = self.generate_hive_script(source_data)
        # TODO 
        # write to a file
        # tranfer to S3 location
        # return S3 location
        return 's3://sauropod-test/hive/scripts/emr-example7.hql'


    def run(self, source_data):

        hive_script = self._generate_and_upload_hive_script()

        # TODO wait 5 secs for S3 consistency 

        conn = EmrConnection(os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])

        setup_step = InstallHiveStep()
        run_step = HiveStep(self.job_name(), hive_script)

        jobid = conn.run_jobflow(
                            self.job_name(),
                            s3_logs,
                            # steps=[setup, run],
                            # keep_alive=False, 
                            action_on_failure='CANCEL_AND_WAIT',
                            master_instance_type='m3.xlarge',
                            slave_instance_type='m3.xlarge',
                            ami_version='2.0',
                            num_instances=2)


        conn.add_jobflow_steps(jobid, [setup_step, run_step])

        self._wait_for_job_to_complete(conn, jobid)



    # ############################
    # cribbed from mrjob
    # ###########################

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

    # wait for job and log status (from mrjob)
    def _wait_for_job_to_complete(conn, jobid):
        """Wait for the job to complete, and raise an exception if
        the job failed.

        Also grab log URI from the job status (since we may not know it)
        """
        success = False
        opts = {'check_emr_status_every': 30}
        s3_logs = 's3://sauropod-test/hive/logs/'
        emr_job_start = time.time()

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
                #if LOG_GENERATING_STEP_NAME_RE.match(
                #        posixpath.basename(getattr(step, 'jar', ''))):
                #    latest_lg_step_num += 1

                # ignore steps belonging to other jobs
                #if not step.name.startswith(self._job_name):
                #    continue

                #step_nums.append(i + 1)
                #if LOG_GENERATING_STEP_NAME_RE.match(
                #        posixpath.basename(getattr(step, 'jar', ''))):
                #    lg_step_num_mapping[i + 1] = latest_lg_step_num

                step.state = step.state
                step_states.append(step.state)
                if step.state == 'RUNNING':
                    running_step_name = step.name

                if (hasattr(step, 'startdatetime') and
                        hasattr(step, 'enddatetime')):
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
                print('Job launched %.1fs ago, status %s: %s (%s)' %
                         (running_time, job_state, reason, running_step_name))

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
                print('Job launched %.1fs ago, status %s: %s' %
                         (running_time, job_state, reason))
            else:
                print('Job launched %.1fs ago, status %s' %
                         (running_time, job_state,))

        if success:
            print('Job completed.')
            print('Running time was %.1fs (not counting time spent waiting'
                     ' for the EC2 instances)' % total_step_time)
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
       


class HiveQuery(object):
    """
    Object to manage the contruction of the Hive query including the boilerplate to load data

    Can return a Hive query for local or EMR execution.
    """
    HIVE_TYPES = [
            'TINYINT',  # 1 byte
            'SMALLINT'  # 2 byte
            'INT'       # 4 byte
            'BIGINT'    # 8 byte
            'FLOAT'     # 4 byte (single precision floating point numbers)
            'DOUBLE'    # 8 byte (double precision floating point numbers)
            'BOOLEAN'   # you know what these are
            'STRING',   # up to 2GB 
            ]
            # NOTE: There are no date data types; dates are treated as strings.
            # There are several date functions which operate on strings.

    def __init__(self, table_name, input_columns, output_columns, query):
        self.uid = 'foo'
        self.table_name = table_name
        self.results_table_name = table_name + "_results"
        self.query = query
        self.input_columns = input_columns
        self.output_columns = output_columns
        if query[-1:] != ';':
            raise ValueError, "query must terminate with a semi-colon"
        if table_name not in query:
            raise ValueError, "query does not contain a reference to the table"
        # TODO validate the input/output columns for proper data types and reserved keywords

    def _csv_serde_jar(self):
        """Using a JAR for serialisation/deserialisation in the Hive tables
        """
        # if it is known on S3
        try:
            serde = os.environ["CSV_SERDE_JAR_S3"]
        except KeyError:
            # TODO - ensure the jar is up on S3
            os.environ['CSV_SERDE_JAR_S3'] = "s3://sauropod-test/hive/jars/csv-serde-1.1.2-0.11.0-all.jar"
            serde = os.environ["CSV_SERDE_JAR_S3"]
        return serde

    def emr_hive_script(self, source_data, output_dir, temp_table_dir):
        """Generate the complete Hive script for EMR
        outputs a comma-delimited file via a hive textfile table
        """
        parts = [
            "ADD JAR {0};".format(self._csv_serde_jar()), # s3://sauropod-test/hive/jars/csv-serde-1.1.2-0.11.0-all.jar;",
            "SET hive.exec.compress.output=false;"
            ]
        parts += self._create_table_ddl(self.table_name, self.input_columns, temp_table_dir)
        parts.append("LOAD DATA INPATH '{0}' INTO TABLE {1};".format(source_data, self.table_name)) # s3://sauropod-test/hive/data/emails-sent-by-date.csv
        parts += self._create_table_ddl(self.results_table_name, self.output_columns, output_dir)
        parts.append("INSERT INTO TABLE {0} {1}".format(self.results_table_name, self.query))
        parts.append(self.query)
        return "\n".join(parts)

    def _create_table_ddl(self, name, columns, location):
        return [
            "CREATE EXTERNAL TABLE {0} ({1})".format(name, self._column_ddl(columns)), 
            "ROW FORMAT serde 'com.bizo.hive.serde.csv.CSVSerde'",
            "STORED AS TEXTFILE", 
            "LOCATION '{0}';".format(location)
            ]

    def _column_ddl(self, columns):
        """Get column defintions for Hive table
        """
        s = []
        for col in columns:
            s.append("{0} {1}".format(col[0], col[1])) 
        return ",".join(s)


class EmailRecipientsSummary(HiveJob):

    def table(self):
        return 'emails_sent'

    def input_columns(self):
        return [
                ('day', 'STRING'),
                ('weekday', 'INT'),
                ('sent', 'BIGINT')
                ]

    def output_columns(self):
        return [
                ('year', 'INT'),
                ('weekday', 'INT'),
                ('sent', 'BIGINT')
                ]

    def query(self):
        return "SELECT YEAR(day), weekday, SUM(sent) FROM emails_sent GROUP BY YEAR(day), weekday;"

 

if __name__ == "__main__":
    EmailRecipientsSummary().run('s3://sauropod-test/hive/data/emails-sent-by-date.csv')

        
