#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
import os
from apiarist.local import LocalRunner


class LocalTest(unittest.TestCase):

    def setUp(self):
        if 'APIARIST_TMP_DIR' in os.environ:
            del os.environ['APIARIST_TMP_DIR']

    def has_a_job_name_test(self):
        r = LocalRunner('TestJob', input_path='/foo/bar')
        self.assertEqual(r.job_name, 'TestJob')

    def set_no_output_test(self):
        r = LocalRunner('TestJob', input_path='/foo/bar',
                        no_output=True)
        self.assertFalse(r.stream_output)
        r = LocalRunner('TestJob', input_path='/foo/bar')
        self.assertTrue(r.stream_output)

    def get_local_scratch_dir_test(self):
        r = LocalRunner('TestJob', input_path='/foo/bar')
        self.assertEqual(r.scratch_dir,
                         '{0}/.apiarist/{1}/'.format(os.environ['HOME'],
                                                     r.job_id))
        r = LocalRunner('TestJob', input_path='/foo/bar',
                        temp_dir='/bar/baz/')
        self.assertEqual(r.scratch_dir, '/bar/baz/' + r.job_id + '/')
        os.environ['APIARIST_TMP_DIR'] = '/baz/foo/'
        r = LocalRunner('TestJob', input_path='/foo/bar')
        self.assertEqual(r.scratch_dir, '/baz/foo/' + r.job_id + '/')

    def test_set_retain_hive_table(self):
        r = LocalRunner('TestJob', input_path='/foo/bar')
        self.assertFalse(r.retain_hive_table)
        r = LocalRunner('TestJob', input_path='/foo/bar',
                        retain_hive_table=True)
        self.assertTrue(r.retain_hive_table)
