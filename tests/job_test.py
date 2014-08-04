#!/usr/bin/python
# -*- coding: utf-8 -*-

import unittest
from apiarist.job import HiveJob
from apiarist.job import HiveQuery


class MockJob(HiveJob):
    def input_columns(self):
        return []
    def output_columns(self):
        return []
    def query(self):
        return "foo;"
    def table(self):
        return "foo"

    def configure_options(self):
        super(MockJob, self).configure_options()
        self.add_passthrough_option('--passthrough-option',
                                    dest='popt')


class HiveJobTest(unittest.TestCase):

    def this_class_is_a_placeholder_test(self):
        # This class is a shell for creating the actual job logic
        # The methods below must be implemented in a subclass
        # and should always throw NotImplemented for this class
        j = HiveJob(['dummy/arg'])
        self.assertRaises(NotImplementedError, j.input_columns)
        self.assertRaises(NotImplementedError, j.output_columns)
        self.assertRaises(NotImplementedError, j.table)
        self.assertRaises(NotImplementedError, j.query)

    def job_class_has_a_name_tests(self):
        j = MockJob(['foo'])
        self.assertEqual('MockJob', j.job_name)

    def job_class_has_a_query_test(self):
        j = MockJob(['foo'])
        self.assertEqual(type(j.hive_query()), HiveQuery)

    def allows_passthrough_options_test(self):
        j = MockJob(['foo','--passthrough-option','foo'])
        self.assertEqual(j.options.popt, 'foo')
