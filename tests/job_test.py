#!/usr/bin/python
# -*- coding: utf-8 -*-

import unittest
from apiarist.job import HiveJob

class HiveJobTest(unittest.TestCase):
    
    def this_class_is_a_placeholder_test(self):
        """This class is a shell for creating the actual job logic
        The methods below must be implemented in a subclass
        and should always throw NotImplemented for this class"""
        j = HiveJob(['dummy/arg'])
        self.assertRaises(NotImplementedError, j.input_columns)
        self.assertRaises(NotImplementedError, j.output_columns)
        self.assertRaises(NotImplementedError, j.table)
        self.assertRaises(NotImplementedError, j.query)
