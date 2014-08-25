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
from apiarist.s3 import upload_file_to_s3


class UnknownSerdeError(Exception):
    pass


class Serde(object):
    """Class to manage the Hive table serde jars
    """
    JARS_DIR = os.path.join(os.path.dirname(__file__), 'jars')
    CSV_JAR = 'csv-serde-1.1.2-0.11.0-all.jar'

    def __init__(self, serde='csv', s3_base_path=None):
        if serde == 'csv':
            self.type = 'CSV'
            self.jar = os.path.abspath(os.path.join(self.JARS_DIR,
                                                    self.CSV_JAR))
        else:
            raise UnknownSerdeError

        # base path for s3 files
        if s3_base_path is None:
            s3_base_path = os.environ.get('S3_SCRATCH_URI')
        self._s3_base_path = s3_base_path

    def s3_path(self):
        """get or create a location on S3 for the serde jar"""
        if 'CSV_SERDE_JAR_S3' in os.environ:
            # known location on S3
            serde = os.environ['CSV_SERDE_JAR_S3']
        else:
            if self._s3_base_path is None:
                raise ValueError("must specify the S3 scratch URI")
            # ensure the jar is up on S3
            jar_path = self._s3_base_path + 'jars/csv-serde.jar'
            upload_file_to_s3(self.jar, jar_path)
            os.environ['CSV_SERDE_JAR_S3'] = serde = jar_path
        return serde
