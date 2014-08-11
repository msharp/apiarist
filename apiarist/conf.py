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
"""Management of job options
"""
import sys
import yaml

# sentinel value; used when running HiveJob as a script
_READ_ARGS_FROM_SYS_ARGV = '_READ_ARGS_FROM_SYS_ARGV'


def process_args(parser, args):
    """Process the comman-line or program arguments
    and check for a config file with extra options
    """
    # arguments can be provided on command line
    if args == _READ_ARGS_FROM_SYS_ARGV:
        opt_args = sys.argv[1:]
    else:
        opt_args = args

    # first pass at the arg list
    options, rem_args = parser.parse_args(opt_args)

    # if a config file was supplied, then modify args list and reprocess
    if options.config_path is not None:
        cf = YamlConfig(options.config_path)
        merged_args = cf.merge_config_file_args(opt_args, options.runner)
        options, rem_args = parser.parse_args(merged_args)

    return (options, rem_args)


class YamlConfig():
    """Use a yaml file to provide options to your jobs
    """

    # list of allowed config file options
    # FIXME not all these options are implemented
    OPTION_MAP = {
        'aws_access_key_id': '--aws-access-key-id',
        'aws_secret_access_key': '--aws-secret-access-key',

        'aws_availability_zone': '--aws-availability-zone',  # not implemented
        'aws_region': '--aws-region ',  # not implemented
        's3_endpoint': '--s3-endpoint',  # not implemented
        'emr_endpoint': '--emr-endpoint ',  # not implemented

        'hive_version': '--hive-version',
        'ami_version': '--ami-version',

        'ec2_master_instance_type': '--ec2-master-instance-type',
        'ec2_instance_type': '--ec2-instance-type',
        'num_ec2_instances': '--num-ec2-instances',

        's3_log_uri': '--s3-log-uri',
        's3_scratch_uri': '--s3-scratch-uri',
        's3_sync_wait_time': '--s3-sync-wait-time',
        'check_emr_status_every': '--check-emr-status-every',
        }

    def __init__(self, path):
        """Initialise the object with a file path
        """
        f = open(path, "r")
        self.conf = yaml.load(f)

    def merge_config_file_args(self, args, runner):
        """Merge options from the config file with supplied arguments.
        Options from command line or program take precedence over
        options provided in the config file
        """
        try:
            if runner == 'emr':
                opts = self.conf['runners']['emr']
            elif runner == 'local':
                opts = self.conf['runners']['local']
        except KeyError:
            return args  # no extra options available

        append_opts = []
        for k, v in self.OPTION_MAP.iteritems():
            if k in opts and v not in args:
                append_opts.append(v)
                append_opts.append(opts[k])
        return args + append_opts
