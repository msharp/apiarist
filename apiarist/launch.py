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
"""Class to manage set up and running of Hivejobs
"""
import sys
import logging

from optparse import Option
# from optparse import OptionError
# from optparse import OptionGroup
from optparse import OptionParser
from apiarist.emr import EMRRunner
from apiarist.local import LocalRunner
from apiarist.util import log_to_null
from apiarist.util import log_to_stream
from apiarist.conf import process_args

logger = logging.getLogger(__name__)

# sentinel value; used when running HiveJob as a script
_READ_ARGS_FROM_SYS_ARGV = '_READ_ARGS_FROM_SYS_ARGV'


class ArgumentMissingError(Exception):
    pass


class HiveJobLauncher(object):

    OPTION_CLASS = Option

    def __init__(self, job_name, args=None):

        self.job_name = job_name

        # allow argument to be passed in to be used
        # to compose the script (variables/parameters)
        self._passthrough_options = []

        self.option_parser = OptionParser(usage=self._usage(),
                                          option_class=self.OPTION_CLASS,
                                          add_help_option=False)
        self.configure_options()

        # build options from args provided
        # this may load the yaml config also
        self.options, rem_args = process_args(self.option_parser, args)

        #  after named args have been removed by OptionParser
        #  only remaining argument is the location of input data
        try:
            self.input_data = rem_args[0]
        except IndexError:
            raise ArgumentMissingError("must provide path to source data")

        # Make it possible to redirect stdin, stdout, and stderr, for testing
        # TODO create sandbox()
        self.stdin = sys.stdin
        self.stdout = sys.stdout
        self.stderr = sys.stderr

    def execute(self):
        self.run_job()

    @classmethod
    def set_up_logging(cls, quiet=False, verbose=False, stream=None):
        """Set up logging when running from the command line.
        This will also set up a null log handler for boto, so we don't get
        warnings if boto tries to log about throttling and whatnot.
        """
        if quiet:
            log_to_null(name='apiarist')
            log_to_null(name='__main__')
        else:
            log_to_stream(name='apiarist', debug=verbose, stream=stream)
            log_to_stream(name='__main__', debug=verbose, stream=stream)
        log_to_null(name='boto')

    def run_job(self):
        """Run the job
        """
        self.set_up_logging(quiet=self.options.quiet,
                            verbose=self.options.verbose,
                            stream=self.stderr)
        #  log the options being used
        logger.info("Launching job {0}".format(self.job_name))
        with self.make_runner() as runner:
            runner.run()

    def make_runner(self):
        """Make a runner based on arguments provided
        """
        if self.options.runner == 'emr':
            kwargs = self.emr_job_runner_kwargs()
            logger.info("Initating EMR runner: {}".format(kwargs))
            return EMRRunner(**kwargs)
        else:
            kwargs = self.local_job_runner_kwargs()
            logger.info("Initiating local runner: {}".format(kwargs))
            return LocalRunner(**kwargs)

    def local_job_runner_kwargs(self):
        return {
            'input_path': self.input_data,
            'output_dir': self.options.output_dir,
            'hive_query': self.hive_query(),
            'job_name': self.job_name,
            }

    def emr_job_runner_kwargs(self):
        slave_instance_type = self.options.slave_instance_type
        master_instance_type = (self.options.master_instance_type or
                                slave_instance_type)
        return {
            'input_path': self.input_data,
            'output_dir': self.options.output_dir,
            'hive_query': self.hive_query(),
            'scratch_dir': self.options.scratch_dir,
            'job_name': self.job_name,
            'master_instance_type': master_instance_type,
            'slave_instance_type': slave_instance_type,
            'num_instances': self.options.num_instances,
            'ami_version': self.options.ami_version,
            'hive_version': self.options.hive_version,
            }

    def hive_query(self):
        # implemented in subclass HiveJob
        raise NotImplementedError

    def configure_options(self):
        """Define the arguments for this script
        """
        # the running mode - local or EMR
        self.option_parser.add_option(
            '-r', dest='runner', action='store', default='local'
            )
        # allow YAML config file
        self.option_parser.add_option(
            '--conf-path', dest='config_path', action='store', default=None
            )
        self.option_parser.add_option(
            '--output-dir', dest='output_dir', action='store', default=False
            )
        self.option_parser.add_option(
            '--s3-scratch-uri', dest='scratch_dir',
            action='store', default=False
            )
        self.option_parser.add_option(
            '--ec2-master-instance-type', dest='master_instance_type',
            action='store', default=False
            )
        self.option_parser.add_option(
            '--ec2-instance-type', dest='slave_instance_type',
            action='store', default='m3.xlarge'
            )
        self.option_parser.add_option(
            '--num-ec2-instances', dest='num_instances',
            action='store', default=2
            )
        self.option_parser.add_option(
            '--ami-version', dest='ami_version',
            action='store', default='latest'
            )
        self.option_parser.add_option(
            '--hive-version', dest='hive_version',
            action='store', default='latest'
            )
        # logging options
        self.option_parser.add_option(
            '--quiet', dest='quiet',
            action='store_true', default=False
            )
        self.option_parser.add_option(
            '--verbose', dest='verbose',
            action='store_true', default=False
            )

    def add_passthrough_option(self, *args, **kwargs):
        """Add a section in the Job to specify options passed to
        the job to compile the query.

        Override the `configure_options` method in your job.

            def configure_options(self):
                super(MRYourJob, self).configure_options()
                self.add_passthrough_option('--start-date',
                                            default='2013-1-1',
                                            dest='startdate')

        Your job will now be able to access `self.options.startdate`
        """
        pass_opt = self.option_parser.add_option(*args, **kwargs)
        self._passthrough_options.append(pass_opt)

    @classmethod
    def _usage(cls):
        """Command line usage string for this class"""
        return ("usage: python [script path|executable path|--help]")
