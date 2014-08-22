#!/usr/bin/python
# -*- coding: utf-8 -*-

import unittest
import os
from apiarist.emr import EMRRunner


class EmrTest(unittest.TestCase):
    # not many tests here
    # need to find a way to mock AWS

    def has_a_job_name_test(self):
        r = EMRRunner('TestJob')
        self.assertEqual(r.job_name, 'TestJob')

    def set_aws_credentials_test(self):
        del os.environ['AWS_ACCESS_KEY_ID']
        del os.environ['AWS_SECRET_ACCESS_KEY']
        k, s = 'foo', 'bar'
        r = EMRRunner('TestJob',
                      aws_access_key_id=k, aws_secret_access_key=s)
        self.assertEqual(r.aws_access_key_id, k)
        self.assertEqual(r.aws_secret_access_key, s)
        self.assertEqual(os.environ['AWS_ACCESS_KEY_ID'], k)
        self.assertEqual(os.environ['AWS_SECRET_ACCESS_KEY'], s)
