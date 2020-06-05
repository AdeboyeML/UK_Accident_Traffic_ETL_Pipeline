from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from boto3.session import Session
import boto3
import logging
import os

class UploadToS3Operator(BaseOperator):
    ui_color = '#358140'
    

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 s3_bucket = "",
                 *args, **kwargs):

        super(UploadToS3Operator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.s3bucket = s3_bucket
        
        
    def execute(self, context):
        """
        redshift_conn_id: redshift cluster connection info.
        aws_credentials_id: necessary info needed to make AWS connection
        s3_bucket: source data in S3 bucket that has the files we want to copy from.
        """
        self.log.info('StageToRedshiftOperator not implemented yet')
        hook = S3Hook(self.aws_credentials_id)
        bucket = self.s3bucket
        keys = hook.list_keys(bucket)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        session = Session(aws_access_key_id=credentials.access_key,
                      aws_secret_access_key=credentials.secret_key)
        keys = os.listdir('/home/workspace/uk-traffic')
        for key in keys:
            session.resource('s3').Bucket(bucket).upload_file('/home/workspace/uk-traffic/' + key, key, 
                                                              ExtraArgs={'ACL': 'public-read'})
        keys2 = os.listdir('/home/workspace/uk-accident')
        for key in keys2:
            session.resource('s3').Bucket(bucket).upload_file('/home/workspace/uk-accident/' + key, key, 
                                                              ExtraArgs={'ACL': 'public-read'})