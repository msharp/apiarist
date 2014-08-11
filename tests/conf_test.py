#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import unittest
from apiarist.conf import YamlConfig

# FIXME make the config yaml file a StringIO object
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'test-config.conf')


class ConfigFileTest(unittest.TestCase):

    def setUp(self):
        self.conf = YamlConfig(CONFIG_PATH)
        self.args = self.conf.merge_config_file_args([], 'emr')

    def assert_contains_args(self, args, arg_pair):
        """Test to see if an two-element list is
        found in a larger list (of program arguments)
        """
        arg = arg_pair[0]
        val = arg_pair[1]
        msg = "{0} = {1} is not in the arg list".format(arg, val)
        found = False
        if arg in args:
            found = (val == args[args.index(arg)+1])
        return self.assertTrue(found, msg)

    def aws_credentials_test(self):
        self.assert_contains_args(self.args,
                                  ['--aws-access-key-id', 'foo'])
        self.assert_contains_args(self.args,
                                  ['--aws-secret-access-key', 'bar'])

    def master_instance_type_test(self):
        self.assert_contains_args(self.args,
                                  ['--ec2-master-instance-type', 'q3.foo'])

    def instance_type_test(self):
        self.assert_contains_args(self.args,
                                  ['--ec2-instance-type', 'r6.4xbaz'])

    def num_instances_test(self):
        self.assert_contains_args(self.args,
                                  ['--num-ec2-instances', 5])

    def hive_version_test(self):
        self.assert_contains_args(self.args,
                                  ['--hive-version', '2.0.0'])

    def ami_version_test(self):
        self.assert_contains_args(self.args,
                                  ['--ami-version', '1.0.0'])

    def s3_log_uri_test(self):
        self.assert_contains_args(self.args,
                                  ['--s3-log-uri', 's3://foo/bar/'])

    def s3_scratch_uri_test(self):
        self.assert_contains_args(self.args,
                                  ['--s3-scratch-uri', 's3://foo/baz/'])

    def sync_wait_time_test(self):
        self.assert_contains_args(self.args,
                                  ['--s3-sync-wait-time', 7])

    def check_emr_status_every_test(self):
        self.assert_contains_args(self.args,
                                  ['--check-emr-status-every', 29])

    # test the precendence of arguments
    #
    # options supplied on command line or by a program
    # take precedence over those found in a config file

    def override_credentials_test(self):
        u = ['--aws-access-key-id', 'MYACCESSKEY']
        p = ['--aws-secret-access-key', 'MYSECRETKEY']
        args = self.conf.merge_config_file_args(u + p, 'emr')
        self.assert_contains_args(args, u)
        self.assert_contains_args(args, p)
