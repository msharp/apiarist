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
""" S3/storage utility methods """

import os
import re
from boto.s3.connection import S3Connection
from boto.s3.key import Key


def copy_s3_file(source, destination):
    """ Copy an S3 object from one location to another
    """
    dest_bucket, dest_key = parse_s3_uri(destination)
    source_bucket, source_key = parse_s3_uri(source)
    conn = S3Connection(os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])
    bkt = conn.get_bucket(dest_bucket)
    return bkt.copy_key(dest_key, source_bucket, source_key)

def upload_file_to_s3(file_path, s3_path):
    """Create an S3 object from the contents of a local file
    """
    s3_bucket, s3_key = parse_s3_uri(s3_path)
    conn = S3Connection(os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])
    bkt = conn.get_bucket(s3_bucket)
    k = Key(bkt)
    k.key = s3_key
    return k.set_contents_from_filename(file_path)       

def parse_s3_uri(uri):
    """Parse an S3 uri from: s3://bucketname/some/other/path/info/
    to: 
        bucket = bucketname
        key = some/other/path/info
    """
    m = re.search(r'(s3://)([A-Za-z0-9_-]+)/(\S*)', uri)
    if m:
        return (m.group(2),m.group(3))
    else:
        return None



