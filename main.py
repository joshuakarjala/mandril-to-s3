import os
import sys
import datetime
import json

from bottle import get, post, run, request
from bottle import jinja2_template as template

import boto
import boto.s3.connection
from boto import config as botoconfig
from boto.s3.key import Key

botoconfig.add_section('s3')
botoconfig.set('s3', 'use-sigv4', 'True')

# make sure we have AWS credentials and a S3 Bucket
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
if not AWS_ACCESS_KEY_ID:
    print >> sys.stderr, 'Missing environment variable AWS_ACCESS_KEY_ID'
    exit(1)


AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
if not AWS_SECRET_ACCESS_KEY:
    print >> sys.stderr, 'Missing environment variable AWS_SECRET_ACCESS_KEY'
    exit(1)


# make sure we have AWS credentials and a S3 Bucket
S3_REGION = os.environ.get('S3_REGION')
if not S3_REGION:
    print >> sys.stderr, 'Missing environment variable S3_REGION'
    exit(1)


S3_BUCKET = os.environ.get('S3_BUCKET')
if not S3_BUCKET:
    print >> sys.stderr, 'Missing environment variable S3_BUCKET'
    exit(1)


def upload_to_s3(data):
    today = datetime.datetime.now()

    # establish connection to S3
    conn = boto.s3.connect_to_region(S3_REGION,
                                     aws_access_key_id=AWS_ACCESS_KEY_ID,
                                     aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                     is_secure=True
                                     )
    bucket = conn.get_bucket(S3_BUCKET)
    k = Key(bucket)

    k.key = '%d/%d/%d/%d.json' % (today.year, today.month, today.day, data.get('ts'))
    k.set_contents_from_string(json.dumps(data, indent=2))


@post('/inbound_mail')
def inbound_mail():
    post_data = request.POST
    event_list = json.loads(post_data.get('mandrill_events'))

    for data in event_list:
        upload_to_s3(data)

    return 'OK'


@get('/setup')
def setup():
    url = request.url.replace('/setup', '/inbound_mail')
    return template('This is your hook url, copy it:<h3>{{url}}</h3>', url=url)

run(host='0.0.0.0', port=int(os.environ.get('PORT', 8010)))
