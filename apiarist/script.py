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
import os

class HiveQuery(object):
    """
    Class to manage the contruction of the Hive query including the boilerplate to load data
    Can return a Hive query for local or EMR execution.
    """
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
            # NOTE: There are no date data types; dates are treated as strings.
            # There are several date functions which operate on strings.

    def __init__(self, table_name, input_columns, output_columns, query):
        self.table_name         = table_name
        self.results_table_name = table_name + "_results"
        self.query              = query
        self.input_columns      = input_columns
        self.output_columns     = output_columns
        if query[-1:] != ';':
            raise ValueError, "query must terminate with a semi-colon"
        if table_name not in query:
            raise ValueError, "query does not contain a reference to the table"
        # TODO validate the input/output columns for proper data types and reserved keywords

    def _csv_serde_jar(self):
        """Using a JAR for serialisation/deserialisation in the Hive tables
        """
        # if it is known on S3
        try:
            serde = os.environ["CSV_SERDE_JAR_S3"]
        except KeyError:
            # TODO - ensure the jar is up on S3
            print("The serde jar in ./jars dir must be put onto S3")
            raise KeyError
        return serde

    def emr_hive_script(self, data_source, output_dir, temp_table_dir):
        """Generate the complete Hive script for EMR
        outputs a comma-delimited file via a hive textfile table
        """
        # boilerplate
        parts = [
            "ADD JAR {0};".format(self._csv_serde_jar()), # 
            "SET hive.exec.compress.output=false;"
            ]
        # add the table in which we'll load the source data
        parts += self._create_table_ddl(self.table_name, self.input_columns, temp_table_dir)
        # add statement to load the source data into this table
        parts.append("LOAD DATA INPATH '{0}' INTO TABLE {1};".format(data_source, self.table_name)) 
        # add a table to select the results into (for CSV formatting)
        parts += self._create_table_ddl(self.results_table_name, self.output_columns, output_dir)
        # insert the results of the supplied query into this table
        parts.append("INSERT INTO TABLE {0} {1}".format(self.results_table_name, self.query))
        # and finally, the query
        parts.append(self.query)
        # return a string that can be written to a file and run on Hive
        return "\n".join(parts)

    def _create_table_ddl(self, name, columns, location):
        """Create a Hive table to store CSV data
        """
        return [
            "CREATE EXTERNAL TABLE {0} ({1})".format(name, self._column_ddl(columns)), 
            "ROW FORMAT serde 'com.bizo.hive.serde.csv.CSVSerde'",
            "STORED AS TEXTFILE", 
            "LOCATION '{0}';".format(location)
            ]

    def _column_ddl(self, columns):
        """Get column defintions for Hive table
        """
        s = []
        for col in columns:
            s.append("{0} {1}".format(col[0], col[1])) 
        return ",".join(s)
 
