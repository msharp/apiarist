#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import unittest
from apiarist.script import HiveQuery
from apiarist.job import HiveJob
from apiarist.serde import Serde
from apiarist.script import get_script_file_location


class DummyJob(HiveJob):
    def __init__(self, q='', tn='', ic='', oc=''):
        self.q = q
        self.tn = tn
        self.ic = ic
        self.oc = oc

    def table(self):
        return self.tn

    def query(self):
        return self.q

    def input_columns(self):
        return self.ic

    def output_columns(self):
        return self.oc


class HiveQueryTest(unittest.TestCase):

    def setUp(self):
        self.hq = self._dummy_query()

    def _dummy_query(self,
                     q=""" SELECT  foo, bar
                            FROM some_table
                            WHERE zero = 0;
                        """,
                     tn='some_table',
                     ic=[('foo', 'STRING'), ('bar', 'STRING')],
                     oc=[('foo', 'STRING'), ('bar', 'STRING')]
                     ):
        job = DummyJob(q, tn, ic, oc)
        return HiveQuery(job)

    def must_pass_hivejob_object_test(self):
        self.assertRaises(TypeError, HiveQuery, 'foo')

    def raise_table_name_error_test(self):
        self.assertRaises(ValueError,
                          self._dummy_query,
                          tn='nonexistent-table')

    def missing_semi_colon_is_added_test(self):
        q = self._dummy_query(q=""" SELECT foo, bar
                                    FROM some_table
                                    WHERE 0 = 1
                                 """)

        self.assertEqual(q.query[-1:], ';')

    def parsed_query_test(self):
        parsed_query = "SELECT foo, bar FROM some_table WHERE zero = 0;"
        self.assertEqual(parsed_query, self.hq.query)

    def emr_hive_script_test(self):
        data_source = 's3://foo/bar/baz/data/'
        output_dir = 's3://foo/bar/baz/temp/'
        temp_table_dir = 's3://foo/bar/baz/table/'
        serde = os.environ["CSV_SERDE_JAR_S3"] = 's3://path/to/serde.jar'
        os.environ['S3_SCRATCH_URI'] = 's3://foo/bar/baz/'
        s = "ADD JAR {};\n".format(serde)
        s += "SET hive.exec.compress.output=false;\n"
        s += "CREATE EXTERNAL TABLE some_table (`foo` STRING, `bar` STRING)\n"
        s += "ROW FORMAT serde 'com.bizo.hive.serde.csv.CSVSerde'\n"
        s += "STORED AS TEXTFILE\nLOCATION '{}';\n".format(temp_table_dir)
        s += "LOAD DATA INPATH '{}' ".format(data_source)
        s += "INTO TABLE some_table;\n"
        s += "CREATE EXTERNAL TABLE some_table_results "
        s += "(`foo` STRING, `bar` STRING)\n"
        s += "ROW FORMAT serde 'com.bizo.hive.serde.csv.CSVSerde'\n"
        s += "STORED AS TEXTFILE\nLOCATION '{}';\n".format(output_dir)
        s += "INSERT INTO TABLE some_table_results\n"
        s += "SELECT foo, bar FROM some_table WHERE zero = 0;"
        self.assertEqual(s, self.hq.emr_hive_script(data_source,
                                                    output_dir,
                                                    temp_table_dir))

    def local_hive_script_test(self):
        data_source = '/tmp/data'
        output_dir = '/tmp/out'
        temp_table_dir = '/tmp/table'
        serde = Serde('csv').jar
        s = "ADD JAR {};\n".format(serde)
        s += "DROP TABLE some_table;\nDROP TABLE some_table_results;\n"
        s += "CREATE EXTERNAL TABLE some_table (`foo` STRING, `bar` STRING)\n"
        s += "ROW FORMAT serde 'com.bizo.hive.serde.csv.CSVSerde'\n"
        s += "STORED AS TEXTFILE\nLOCATION '{}';\n".format(temp_table_dir)
        s += "LOAD DATA LOCAL INPATH '{}' ".format(data_source)
        s += "INTO TABLE some_table;\n"
        s += "CREATE EXTERNAL TABLE some_table_results "
        s += "(`foo` STRING, `bar` STRING)\n"
        s += "ROW FORMAT serde 'com.bizo.hive.serde.csv.CSVSerde'\n"
        s += "STORED AS TEXTFILE\nLOCATION '{}';\n".format(output_dir)
        s += "INSERT INTO TABLE some_table_results\n"
        s += "SELECT foo, bar FROM some_table WHERE zero = 0;"
        self.assertEqual(s, self.hq.local_hive_script(data_source,
                                                      output_dir,
                                                      temp_table_dir))

    def column_ddl_test(self):
        cols = [('foo', 'INT'), ('bar', 'STRING')]
        ddl = "`foo` INT, `bar` STRING"
        self.assertEqual(ddl, self.hq._column_ddl(cols))

    def get_script_file_location_test(self):
        if 'APIARIST_TMP_DIR' in os.environ:
            del os.environ['APIARIST_TMP_DIR']
        l, j = "/foo/bar", "abcdef1234567890"
        self.assertEqual(get_script_file_location(j),
                         '/tmp/' + j + '.hql')
        self.assertEqual(get_script_file_location(j, l),
                         l + j + '.hql')
        # test with ENV var
        os.environ['APIARIST_TMP_DIR'] = "/bar/baz/"
        self.assertEqual(get_script_file_location(j),
                         '/bar/baz/' + j + '.hql')
