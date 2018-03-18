import boto3
import dataset
import dwh
import logging
import os
import cx_Oracle

from pathlib import Path

# Setup logging
log_format = '%(asctime)s:%(filename)s:%(levelname)s:%(message)s'
logging.basicConfig(format=log_format, level=logging.INFO)

# Get environment variables
config_dir = 'F:\SelfService\Spec' #Path(os.getenv('CONFIG_DIR'))
config_bucket = 'DataFile'   #os.getenv('CONFIG_BUCKET')
config_prefix = 'F:\SelfService' #os.getenv('CONFIG_PREFIX')

#rds_url = os.getenv('RDS_DSN')
#sqs_url = os.getenv('SQS_DSN')
#region = os.getenv('AWS_REGION')

# Get AWS clients
s3 = boto3.resource('s3', region_name='Ireland')#region)
sqs = boto3.resource('sqs', region_name='Ireland')#region)
queue = '' #sqs.Queue(sqs_url)

# Connect to the database
db = cx_Oracle.connect(user='HEROSOBJ', password='HEROSOBJ', dsn='(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.1.161)(PORT = 1521))(CONNECT_DATA = (SERVICE_NAME = ORCL)))')
# dataset.connect(rds_url, ensure_schema=False, engine_kwargs={'case_sensitive': False})
config = dwh.Configurations(config_dir, s3, config_bucket, config_prefix)

# Consume messages
for receipt_handler, records in dwh.poll_queue(queue, WaitTimeSeconds=15):
    if len(records) == 0:
        continue

    for bucket, key in records:
        try:
            logging.info(f'processing {key} in bucket {bucket}')
            with dwh.PersistentBuffer(key, db) as pb:
                dwh.persist(pb, config, dwh.download_file(s3, bucket, key))
            logging.info(f'{key} in bucket {bucket} processed successfully')
            #sqs.Message(sqs_url, receipt_handler).delete()
        except dwh.PersistentBuffer.AlreadyProcessing:
            logging.info(f'{key} in bucket {bucket} already processing')
            # don't remove sqs message as the process might still fail!
        except dwh.PersistentBuffer.AlreadyProcessed:
            logging.info(f'{key} in bucket {bucket} already processed')
            #sqs.Message(sqs_url, receipt_handler).delete()
        except Exception as e:
            logging.exception(f'error while processing {key} in bucket {bucket}')
