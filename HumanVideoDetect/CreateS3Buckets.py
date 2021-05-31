import boto3
import json
import logging
from botocore.exceptions import ClientError

s3 = boto3.client('s3')

def s3CreateBuckets(region):
    InputVideoBucket = s3.create_bucket(Bucket='inputvideobucket',CreateBucketConfiguration={
        'LocationConstraint': region,
    })
    OutputDataBucket = s3.create_bucket(Bucket='outputdatabucket', CreateBucketConfiguration={
        'LocationConstraint': region,
    })
    response = s3.list_buckets()
#print('Existing buckets:')
#for bucket in response['Buckets']:
 #   print(f'  {bucket["Name"]}')
    print(OutputDataBucket)
    print(InputVideoBucket)
    return InputVideoBucket, OutputDataBucket
    
try: 
    newbuckets = s3CreateBuckets('us-east-2')
except ClientError as ex:
    print('bucket already exists')
print (newbuckets)