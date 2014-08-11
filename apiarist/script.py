# Copyright 2014 Max Sharples
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import re
from apiarist.serde import Serde


class HiveQuery(object):
    """Class to manage the contruction of the Hive query
    including the boilerplate to load data
    Can return a Hive query for local or EMR execution.
    """

    #  NOTE: There are no date data types; dates are treated as strings.
    #  There are several date functions which operate on strings.
    HIVE_TYPES = [
        'TINYINT',  # 1 byte
        'SMALLINT'  # 2 byte
        'INT'       # 4 byte
        'BIGINT'    # 8 byte
        'FLOAT'     # 4 byte (single precision floating point numbers)
        'DOUBLE'    # 8 byte (double precision floating point numbers)
        'BOOLEAN'   # you know what these are
        'STRING',   # up to 2GB
        ]

    def __init__(self, table_name, input_columns, output_columns, query):
        """Initalise the query with all the variable properties
        defined for a HiveJob """
        self.table_name = table_name
        self.results_table_name = table_name + "_results"
        self.query = self._parse_query(query.strip())
        self.input_columns = input_columns
        self.output_columns = output_columns
        if self.query[-1:] != ';':
            self.query += ';'
        if self.table_name not in self.query:
            raise ValueError("query does not contain a reference to the table")
        #  TODO validate the input/output columns for
        #  proper data types and reserved keywords

    def __repr__(self):
        return "HiveQuery:{}...".format(self.query[:80])

    def _csv_serde_jar(self, s3_scratch_uri):
        """Using a JAR for serialisation/deserialisation in the Hive tables
        """
        serde = Serde('csv', s3_scratch_uri)
        return serde.s3_path()

    def _parse_query(self, query):
        """Condense spaces"""
        return re.sub(r"\s+", " ", query)

    def local_hive_script(self, data_source, output_dir, temp_table_dir):
        """generate a hive script to execute on the local hive server
        generates a CSV file via a hive textfile table
        """
        # boilerplate
        parts = [
            #  serde required before attempting drop tables
            "ADD JAR {0};".format(Serde('csv').jar),
            "DROP TABLE {0};".format(self.table_name),
            "DROP TABLE {0};".format(self.results_table_name),
            ]
        #  add the table in which we'll load the source data
        parts += self._create_table_ddl(self.table_name,
                                        self.input_columns,
                                        temp_table_dir)
        #  add statement to load the source data into this table
        parts.append("LOAD DATA LOCAL INPATH '{0}' INTO TABLE {1};".format(
            data_source, self.table_name))
        #  add a table to select the results into (for CSV formatting)
        parts += self._create_table_ddl(self.results_table_name,
                                        self.output_columns,
                                        output_dir)
        #  insert the results of the supplied query into this table
        parts.append("INSERT INTO TABLE {0}".format(
            self.results_table_name, self.query))
        #  and finally, the query
        parts.append(self.query)
        #  return a string that can be written to a file and run on Hive
        return "\n".join(parts)

    def emr_hive_script(self, data_source, output_dir, temp_table_dir,
                        s3_scratch_uri=None):
        """Generate the complete Hive script for EMR
        igenerates a set of comma-delimited files via a hive textfile table
        """
        # boilerplate
        parts = [
            "ADD JAR {0};".format(self._csv_serde_jar(s3_scratch_uri)),
            "SET hive.exec.compress.output=false;"
            ]
        # add the table in which we'll load the source data
        parts += self._create_table_ddl(self.table_name,
                                        self.input_columns,
                                        temp_table_dir)
        # add statement to load the source data into this table
        parts.append("LOAD DATA INPATH '{0}' INTO TABLE {1};".format(
            data_source, self.table_name))
        # add a table to select the results into (for CSV formatting)
        parts += self._create_table_ddl(self.results_table_name,
                                        self.output_columns,
                                        output_dir)
        # insert the results of the supplied query into this table
        parts.append("INSERT INTO TABLE {0}".format(self.results_table_name))
        # and finally, the query
        parts.append(self.query)
        # return a string that can be written to a file and run on Hive
        return "\n".join(parts)

    def _create_table_ddl(self, name, columns, location):
        """Create a Hive table to store CSV data
        """
        cols = self._column_ddl(columns)
        return [
            "CREATE EXTERNAL TABLE {0} ({1})".format(name, cols),
            "ROW FORMAT serde 'com.bizo.hive.serde.csv.CSVSerde'",
            "STORED AS TEXTFILE",
            "LOCATION '{0}';".format(location)
            ]

    def _column_ddl(self, columns):
        """Get column defintions for Hive table
        """
        cols = ["`{0}` {1}".format(col[0], col[1]) for col in columns]
        return ", ".join(cols)
