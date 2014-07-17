#!/usr/bin/python
# -*- coding: utf-8 -*-

import unittest
from apiarist.launch import HiveJobLauncher

class HiveJobLauncherTest(unittest.TestCase):

    DATA_PATH =  's3://path/to/data/'

    def supply_path_to_data_test(self):
        j = HiveJobLauncher('TestJob', [self.DATA_PATH])
        self.assertEqual(self.DATA_PATH, j.input_data)

    def supply_output_dir_test(self):
        d = 's3://path/to/ouput-data/'
        j = HiveJobLauncher('TestJob', [self.DATA_PATH, '--output-dir', d])
        self.assertEqual(d, j.options.output_dir)

    def supply_scratch_uri_test(self):
        d = 's3://path/to/scratch/'
        j = HiveJobLauncher('TestJob', [self.DATA_PATH, '--s3-scratch-uri', d])
        self.assertEqual(d, j.options.scratch_dir)

    def supply_ec2_instance_type_test(self):
        t = 'j3.2xlarge'
        j = HiveJobLauncher('TestJob', [self.DATA_PATH, '--ec2-instance-type', t])
        self.assertEqual(t, j.options.slave_instance_type)

    def supply_ec2_master_instance_type_test(self):
        m = 'm3.xlarge'
        s = 'j3.2xlarge'
        j = HiveJobLauncher('TestJob', [self.DATA_PATH, '--ec2-master-instance-type', m, '--ec2-instance-type', s])
        self.assertEqual(m, j.options.master_instance_type)
        self.assertEqual(s, j.options.slave_instance_type)

    def supply_num_ec2_instances_test(self):
        n = '7'
        j = HiveJobLauncher('TestJob', [self.DATA_PATH, '--num-ec2-instances', n])
        self.assertEqual(n, j.options.num_instances)

    def supply_ami_version_test(self):
        v = '2.0.0'
        j = HiveJobLauncher('TestJob', [self.DATA_PATH, '--ami-version', v])
        self.assertEqual(v, j.options.ami_version)

    def supply_hive_version_test(self):
        v = '2.0'
        j = HiveJobLauncher('TestJob', [self.DATA_PATH, '--hive-version', v])
        self.assertEqual(v, j.options.hive_version)


    def this_class_has_no_query_test(self):
        j = HiveJobLauncher('TestJob', ['s3://path/to/data/'])
        self.assertRaises(NotImplementedError, j.hive_query)
