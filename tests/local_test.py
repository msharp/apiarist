#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
import os
from apiarist.local import LocalRunner


class LocalTest(unittest.TestCase):

    def setUp(self):
        if 'APIARIST_TMP_DIR' in os.environ:
            del os.environ['APIARIST_TMP_DIR']

    def as_a_job_name_test(self):
        r = LocalRunner('TestJob', input_path='/foo/bar')
        self.assertEqual(r.job_name, 'TestJob')

    def get_local_scratch_dir_test(self):
        r = LocalRunner('TestJob', input_path='/foo/bar')
        self.assertEqual(r.scratch_dir, '~/.apiarist/' + r.job_id + '/')
        r = LocalRunner('TestJob', input_path='/foo/bar',
                        temp_dir='/bar/baz/')
        self.assertEqual(r.scratch_dir, '/bar/baz/' + r.job_id + '/')
        os.environ['APIARIST_TMP_DIR'] = '/baz/foo/'
        r = LocalRunner('TestJob', input_path='/foo/bar')
        self.assertEqual(r.scratch_dir, '/baz/foo/' + r.job_id + '/')