from zipfile import ZipFile
from os import walk,path
from mypy_boto3_s3.client import S3Client
from boto3.s3.transfer import TransferConfig
import boto3
from os.path import relpath, join, abspath, dirname
import aioboto3
import pathlib
import io
import asyncio
from typing import IO


AWS_S3_BUCKET= ''
AIRFLOW_FOLDER= './'
AWS_S3_DEST_DIR= 'cdk_airflow'



def execut(PLUGINS_DIR='./plugins',AWS_S3_PLUGINS_DIR=None, AWS_S3_BUCKET=None):
    plugins_whitelist = ['.py']
    KB = 1024
    MB = KB ** 2
    GB = KB ** 3        
    config = TransferConfig(multipart_threshold= 5 * GB)
    s3: S3Client
    s3=boto3.client("s3")
    plugins(s3, PLUGINS_DIR, AWS_S3_PLUGINS_DIR, plugins_whitelist, config, AWS_S3_BUCKET)


def plugins(s3, PLUGINS_DIR, AWS_S3_PLUGINS_DIR, plugins_whitelist, config, AWS_S3_BUCKET):
        zip_stream:IO[bytes]=io.BytesIO()
        with ZipFile(zip_stream, 'w') as zip:
            for source, dirs, files in walk(PLUGINS_DIR):
                for filename in files:
                    if(pathlib.Path(filename).suffix not in plugins_whitelist): continue
                    filepath =  path.join(source, filename)
                    zip.write(filename=filepath, arcname=path.join(relpath(source,PLUGINS_DIR),filename))
        zip_stream.seek(0)
        s3.upload_fileobj(Fileobj=zip_stream, Bucket=AWS_S3_BUCKET, Key=AWS_S3_PLUGINS_DIR, Config=config)
        return  s3.head_object(Bucket=AWS_S3_BUCKET, Key=AWS_S3_PLUGINS_DIR)

