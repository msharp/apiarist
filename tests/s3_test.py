#!/usr/bin/python
# -*- coding: utf-8 -*-

import unittest
from apiarist.s3 import parse_s3_uri
from apiarist.s3 import obj_type
from apiarist.s3 import is_dir


class SerdeTest(unittest.TestCase):

    def parse_s3_uri_test(self):
        s = 's3://foo/bar/baz.csv'
        b, k = parse_s3_uri(s)
        self.assertEqual(b, 'foo')
        self.assertEqual(k, 'bar/baz.csv')

    def obj_type_test(self):
        s = 's3://foo/bar/baz.csv'
        self.assertEqual(obj_type(s), 'file')
        s = 's3://foo/bar/baz/'
        self.assertEqual(obj_type(s), 'directory')

    def is_dir_test(self):
        s = 's3://foo/bar/baz.csv'
        self.assertFalse(is_dir(s))
        s = 's3://foo/bar/baz/'
        self.assertTrue(is_dir(s))
