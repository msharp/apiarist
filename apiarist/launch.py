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
#  from optparse import OptionError
#  from optparse import OptionGroup
from optparse import OptionParser

from apiarist.emr import EMRRunner
from apiarist.local import LocalRunner
from apiarist.util import log_to_null
from apiarist.util import log_to_stream

logger = logging.getLogger(__name__)

# sentinel value; used when running HiveJob as a script
_READ_ARGS_FROM_SYS_ARGV = '_READ_ARGS_FROM_SYS_ARGV'


class ArgumentMissingError(Exception):
    pass


class HiveJobLauncher(object):

    OPTION_CLASS = Option

    def __init__(self, job_name, args=None):

        self.job_name = job_name

        #  TODO _ allow argument to be passed in
        #  to be used to compose the script (variables/parameters)
        self.passthrough_options = []

        self.option_parser = OptionParser(usage=self._usage(),
                                          option_class=self.OPTION_CLASS,
                                          add_help_option=False)
        self.configure_options()

        # arguments provided on command line
        if args == _READ_ARGS_FROM_SYS_ARGV:
            self._cl_args = sys.argv[1:]
            self.options, args = self.option_parser.parse_args(self._cl_args)
        else:
            self.options, args = self.option_parser.parse_args(args)

        #  after named args are removed,
        #  only remaining arg is the source of data
        try:
            self.input_data = args[0]
        except IndexError:
            raise ArgumentMissingError("must provide path to source data")

        # Make it possible to redirect stdin, stdout, and stderr, for testing
        # See sandbox(), below.
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
        master_instance_type = self.options.master_instance_type or \
            slave_instance_type
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

        # TODO allow YAML config file
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

    @classmethod
    def _usage(cls):
        """Command line usage string for this class"""
        return ("usage: python [script path|executable path|--help]")
