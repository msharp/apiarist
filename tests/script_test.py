#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import unittest
from apiarist.script import HiveQuery


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
        return HiveQuery(tn, ic, oc, q)

    def raise_semi_colon_error(self):
        pass

    def raise_table_name_error(self):
        pass

    def parsed_query_test(self):
        parsed_query = "SELECT foo, bar FROM some_table WHERE zero = 0;"
        self.assertEqual(parsed_query, self.hq.query)

    def emr_hive_script_test(self):
        data_source = 's3://foo/bar/baz/data/'
        output_dir = 's3://foo/bar/baz/temp/'
        temp_table_dir = 's3://foo/bar/baz/table/'
        os.environ["CSV_SERDE_JAR_S3"] = 's3://path/to/serde.jar'
        s = "ADD JAR s3://path/to/serde.jar;\n"
        s += "SET hive.exec.compress.output=false;\n"
        s += "CREATE EXTERNAL TABLE some_table (`foo` STRING, `bar` STRING)\n"
        s += "ROW FORMAT serde 'com.bizo.hive.serde.csv.CSVSerde'\n"
        s += "STORED AS TEXTFILE\nLOCATION 's3://foo/bar/baz/table/';\n"
        s += "LOAD DATA INPATH 's3://foo/bar/baz/data/' INTO TABLE some_table;"
        s += "\nCREATE EXTERNAL TABLE some_table_results "
        s += "(`foo` STRING, `bar` STRING)\n"
        s += "ROW FORMAT serde 'com.bizo.hive.serde.csv.CSVSerde'\n"
        s += "STORED AS TEXTFILE\nLOCATION 's3://foo/bar/baz/temp/';\n"
        s += "INSERT INTO TABLE some_table_results SELECT foo, bar "
        s += "FROM some_table WHERE zero = 0;\n"
        s += "SELECT foo, bar FROM some_table WHERE zero = 0;"
        self.assertEqual(s, self.hq.emr_hive_script(data_source,
                                                    output_dir,
                                                    temp_table_dir))

    def column_ddl_test(self):
        cols = [('foo', 'INT'), ('bar', 'STRING')]
        ddl = "`foo` INT, `bar` STRING"
        self.assertEqual(ddl, self.hq._column_ddl(cols))
