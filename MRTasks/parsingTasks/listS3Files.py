import sys

__author__ = 'arul'

from boto.s3.connection import S3Connection


if __name__ == '__main__':
    access_key = sys.argv[1]
    access_secret = sys.argv[2]
    conn = S3Connection(access_key,access_secret)
    bucket = conn.get_bucket('cs6240_msd')
    for key in bucket.list(prefix='cs6240_msd/'):
        print key
        # print key.name.encode('utf-8')