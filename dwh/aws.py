import json
import re
import urllib.parse as url

from pathlib import Path


def get_receipt_and_body(message):
    return message.receipt_handle, json.loads(message.body).get('Records', {})


def is_s3_event(record):
    return record.get('eventSource') == 'aws:s3'


def is_new_file_event(record):
    return re.match('ObjectCreated:.*', record.get('eventName'))


def get_bucket_and_key(record):
    return record['s3']['bucket']['name'], url.unquote_plus(record['s3']['object']['key'])


def parse_message(message):
    receipt_handle, records = get_receipt_and_body(message)
    return receipt_handle, [get_bucket_and_key(record) for record in records if is_s3_event(record) and is_new_file_event(record)]


def poll_queue(queue, **kwargs):
    while True:
        yield from (parse_message(message) for message in queue.receive_messages(**kwargs))


def download_file(s3_resource, bucket, key):
    path = Path(Path(key).name)
    if not path.exists():
        s3_resource.Bucket(bucket).download_file(key, path.name)
    return path

def read_file(s3_resource, bucket, key):
    f = s3_resource.meta.client.get_object(Bucket=bucket,Key=key)
    return f.get('Body')

# CHANGE
def list_files(s3_resource, bucket, path):
    files = 'F:\SelfService\DataFile\\'#s3_resource.meta.client.list_objects(Bucket=bucket,Prefix=path)
    for item in files.get('Contents'):
        yield ( item.get('Key'), item.get('LastModified') )