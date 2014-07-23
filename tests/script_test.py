#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import unittest
from apiarist.script import HiveQuery
from apiarist.serde import Serde


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
        serde = os.environ["CSV_SERDE_JAR_S3"] = 's3://path/to/serde.jar'
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
