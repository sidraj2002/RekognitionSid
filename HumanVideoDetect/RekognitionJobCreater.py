import boto3 
import logging
import json
import time
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
    MinConfidence=91,
    NotificationChannel={
        'SNSTopicArn': SNSTopicArn,
        'RoleArn': 'arn:aws:iam::256069468632:role/RekognitionServiceRole'
    },
    JobTag='TestJob01')
    return response
    
    
def JobSuccessChecker2(RekognitionJobID):
    client = boto3.client('rekognition')
    JobSucess = False
    while not JobSucess :
     response = client.get_label_detection(
      JobId=RekognitionJobID,
      MaxResults=123
      )
     print(response['JobStatus'])
     if response['JobStatus'] == "SUCCEEDED":
        JobSucess = True
        return response
     else:
        time.sleep(5)
     continue
    
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
  
  RekognitionStartResponse = LambdaStartLabelDetection('inputvideobucket', 'test.mp4', 'arn:aws:sns:us-east-2:256069468632:RekognitionTest01')
  print(RekognitionStartResponse)
  
  JobStatusCheck = JobSuccessChecker2( RekognitionStartResponse['JobId'])
  with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(JobStatusCheck, f, ensure_ascii=False, indent=4)
  
else:
  print('Failed to locate input key in S3 ... ')
 

# Rekognition Service role: arn:aws:iam::256069468632:role/RekognitionServiceRole
# SNS Topic Arn: arn:aws:sns:us-east-2:256069468632:RekognitionTest01

# Version 1 - Using SQS:

#def JobSuccessChecker(SQSEndpoint, RekognitionJobID):
#    client = boto3.client('sqs')
#    JobSucess = False
#    while JobSucess == False:
#     response = client.receive_message(
#     QueueUrl=SQSEndpoint,
#     AttributeNames=['All'],
#     MessageAttributeNames=[ 'All',],
#     MaxNumberOfMessages=10,
#     )
#     print(response)
#     time.sleep(5)
#     continue
#      return True
     
