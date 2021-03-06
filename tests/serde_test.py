#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import unittest
from apiarist.serde import Serde
from apiarist.serde import UnknownSerdeError


class SerdeTest(unittest.TestCase):

    def serde_jar_test(self):
        s = Serde('csv')
        self.assertEqual(s.type, 'CSV')
        self.assertEqual('.jar', s.jar[-4:])

    def unknown_serde_error_test(self):
        self.assertRaises(UnknownSerdeError, Serde, 'foo')

    def s3_base_path_test(self):
        # by supplying path
        s3 = 's3://foo/bar/baz/'
        s = Serde('csv', s3_base_path=s3)
        self.assertEqual(s3, s._s3_base_path)
        # by getting from env
        s3 = 's3://foo/baz/bar/'
        os.environ['S3_SCRATCH_URI'] = s3
        s = Serde('csv')
        self.assertEqual(s3, s._s3_base_path)

    def s3_base_path_error_test(self):
        # will raise error if neither scratch path
        # nor serde jar URI available
        if 'S3_SCRATCH_URI' in os.environ:
            del os.environ['S3_SCRATCH_URI']
        if 'CSV_SERDE_JAR_S3' in os.environ:
            del os.environ['CSV_SERDE_JAR_S3']
        s = Serde()
        self.assertRaises(ValueError, s.s3_path)
