import boto3 
import logging
import json
from botocore.exceptions import ClientError

def LambdaStartLabelDetection(InputVideoBucket, InputVideoKey, SNSTopicArn):
    client = boto3.client('rekognition')
    s3bucket = InputVideoBucket
    s3key = InputVideoKey
    sns = SNSTopicArn
    
    response = client.start_label_detection(
    Video={
        'S3Object': {
            'Bucket': InputVideoBucket,
            'Name': InputVideoKey
        }
    },
    MinConfidence=80,
    NotificationChannel={
        'SNSTopicArn': SNSTopicArn,
        'RoleArn': 'arn:aws:iam::256069468632:role/RekognitionServiceRole'
    },
    JobTag='TestJob01'
)
    return response
    
    
def S3Exist(InputVideoBucket, InputVideoKey):
    s3 = boto3.client('s3')
    try:
        response = s3.head_object(Bucket=InputVideoBucket, Key=InputVideoKey)
        return response
    except ClientError:
        #print('key not found ...')
        return ('False')

response = S3Exist('inputvideobucket', 'test.mp4')
if response != 'False':
  print('Key exists, continue ...')
  LambdaStartLabelDetection('inputvideobucket', 'test.mp4', 'arn:aws:sns:us-east-2:256069468632:RekognitionTest01')
  
  
  
else:
  print('Failed to locate input key in S3 ... ')
 

# Rekognition Service role: arn:aws:iam::256069468632:role/RekognitionServiceRole
# SNS Topic Arn: arn:aws:sns:us-east-2:256069468632:RekognitionTest01

    