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


def get_conn():
    return S3Connection(os.environ['AWS_ACCESS_KEY_ID'],
                        os.environ['AWS_SECRET_ACCESS_KEY'])


def copy_s3_file(source, destination):
    """ Copy an S3 object from one location to another
    """
    dest_bucket, dest_key = parse_s3_uri(destination)
    source_bucket, source_key = parse_s3_uri(source)
    conn = get_conn()
    if is_dir(source):
        s_bkt = conn.get_bucket(source_bucket)
        d_bkt = conn.get_bucket(dest_bucket)
        for i, k in enumerate(get_bucket_list(s_bkt, source_key)):
            new_key = dest_key + str(i)
            #  logging.info "copying {0}{1} to {2}{3}".format(source_bucket,
            #  k.key, dest_bucket, new_key)
            d_bkt.copy_key(new_key, source_bucket, k.key)
        return destination + '/'
    else:
        bkt = conn.get_bucket(dest_bucket)
        #  logging.info "copying {0}{1} to {2}{3}".format(source_bucket,
        #  source_key, dest_bucket, dest_key)
        return bkt.copy_key(dest_key, source_bucket, source_key)


def upload_file_to_s3(file_path, s3_path):
    """Create an S3 object from the contents of a local file
    """
    s3_bucket, s3_key = parse_s3_uri(s3_path)
    conn = get_conn()
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
        return (m.group(2), m.group(3))
    else:
        return None


def obj_type(key):
    """If it is a 'dir' it will end with a slash
    otherwise it is a 'file'
    """
    if key[-1:] == '/':
        return 'directory'
    else:
        return 'file'


def is_dir(key):
    return obj_type(key) == 'directory'


def concatenate_keys(source_dir, destination_key):
    """Concatenate all the files in a bucket
    using multipart upload feature of S3 API.
    NOTE: this only works when all files are above 5MB
    """
    s_bucket, s_key = parse_s3_uri(source_dir)
    d_bucket, d_key = parse_s3_uri(destination_key)
    conn = get_conn()
    s_bk = conn.get_bucket(s_bucket)
    d_bk = conn.get_bucket(d_bucket)
    mp = d_bk.initiate_multipart_upload(d_key)
    for i, k in enumerate(get_bucket_list(s_bk, s_key)):
        mp.copy_part_from_key(s_bucket, k.key, i+1)
    mp.complete_upload()


def get_bucket_list(bucket, key):
    """ list items in a bucket that match given key """
    # ignore key if zero bytes
    return [k for k in bucket.list(key) if k.size > 0]
