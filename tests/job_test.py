#!/usr/bin/python
# -*- coding: utf-8 -*-

import unittest
from apiarist.job import HiveJob, InvalidHiveJobException
from apiarist.job import HiveQuery


class MockJob(HiveJob):
    def input_columns(self):
        return []

    def output_columns(self):
        return []

    def query(self):
        return """SELECT foo, bar, baz
               FROM foo
               WHERE x = y
               """

    def table(self):
        return "foo"

    def configure_options(self):
        super(MockJob, self).configure_options()
        self.add_passthrough_option('--passthrough-option',
                                    dest='popt')


class BrokenMockJob(MockJob):
    def query(self):
        return None


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
        j = MockJob(['foo', '--passthrough-option', 'foo'])
        self.assertEqual(j.options.popt, 'foo')

    def plain_query_test(self):
        j = MockJob(['foo'])
        q = "SELECT foo, bar, baz FROM foo WHERE x = y"
        self.assertEqual(q, j.plain_query())

    def has_delimter_char_test(self):
        j = MockJob(['foo'])
        self.assertEqual(j.INFILE_DELIMITER_CHAR, ",")
        self.assertEqual(j.OUTFILE_DELIMITER_CHAR, ",")

    def has_quote_char_test(self):
        j = MockJob(['foo'])
        self.assertEqual(j.INFILE_QUOTE_CHAR, r'\"')
        self.assertEqual(j.OUTFILE_QUOTE_CHAR, r'\"')

    def has_escape_char_test(self):
        j = MockJob(['foo'])
        self.assertEqual(j.INFILE_ESCAPE_CHAR, "\\\\")
        self.assertEqual(j.OUTFILE_ESCAPE_CHAR, "\\\\")

    def broken_query_raises_invalid_job_error_test(self):
        j = BrokenMockJob(['foo'])
        self.assertRaises(InvalidHiveJobException, j.plain_query)
