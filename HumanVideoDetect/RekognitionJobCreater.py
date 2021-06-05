import boto3 
import logging
import json
import time
from botocore.exceptions import ClientError

def StartLabelDetection(InputVideoBucket, InputVideoKey, SNSTopicArn):
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
    MinConfidence=90,
    NotificationChannel={
        'SNSTopicArn': SNSTopicArn,
        'RoleArn': 'arn:aws:iam::256069468632:role/RekognitionServiceRole'
    },
    #JobTag='TestJob01')
    )
    return response
    
    
def JobSuccessChecker2(RekognitionJobID, RekognitionNextToken):
    client = boto3.client('rekognition')
    JobSucess = False
    while not JobSucess :
     response = client.get_label_detection(
      JobId=RekognitionJobID,
      MaxResults=10
      )
     print(response['JobStatus'])
     if response['JobStatus'] == "SUCCEEDED":
        JobSucess = True
        return response
     else:
        time.sleep(5)
     continue
 
 
def JobResultsFetcher(RekognitionJobID, RekognitionNextToken):
    client = boto3.client('rekognition')
    response = client.get_label_detection(
      JobId=RekognitionJobID,
      MaxResults=50,
      NextToken=RekognitionNextToken
      )
    return response
    
def S3Exist(InputBucket, InputKey):
    s3 = boto3.client('s3')
    try:
        response = s3.head_object(Bucket=InputBucket, Key=InputKey)
        return response
    except ClientError:
        #print('key not found ...')
        return ('False')

# Work on Below to combine below function to work with FrameExtracter and omit JSON file output and reads. 
def RekognitionResultsPublisher(RekognitionJobID, RekognitionNextToken, SqsUrl):
  sqs = boto3.client('sqs')
  Done = False
  count = 1
  while Done == False: 
      JobStatusCheck = JobResultsFetcher(RekognitionJobID, RekognitionNextToken)
      SendMessage = sqs.send_message(
        QueueUrl=SqsUrl,
        MessageBody=str(JobStatusCheck),
        MessageAttributes={
            'JobId': {
            'StringValue': str(RekognitionJobID),
            'DataType': 'String'
                },
            'Sequence':{
                'StringValue': str(count),
                'DataType': 'Number'
            }
            }
        )
        
      #print(SendMessage)
      with open(str(count) + 'data.json', 'w', encoding='utf-8') as f:
        json.dump(JobStatusCheck, f, ensure_ascii=False, indent=4)
        count += 1
      if 'NextToken' in JobStatusCheck:
          RekognitionNextToken = JobStatusCheck['NextToken']
      else:
          Done = True
          return Done

def GetSqsMessages(SqsUrl, RekognitionJobID):
    sqs = boto3.client('sqs')
    queueAttributes = sqs.get_queue_attributes(
        QueueUrl=SqsUrl,
        AttributeNames=['All']
        )
        #print(queueAttributes)
    while queueAttributes['Attributes']['ApproximateNumberOfMessages'] > '0':
        message = sqs.receive_message(
        QueueUrl=SqsUrl,
         MessageAttributeNames=[
            'JobId'
            ],
        MaxNumberOfMessages=1,
        VisibilityTimeout=20,
        WaitTimeSeconds=5,
        ReceiveRequestAttemptId='string'
        )
        messagejson = json.loads(message['Messages'][0]['Body'])
        print(messagejson['JobStatus'])
        #print(message['Messages'][0][])
        continue

SQSURL = 'https://sqs.us-east-2.amazonaws.com/256069468632/RekognitionQueue01'
response = S3Exist('inputvideobucket', 'people-detection.mp4')
if response != 'False':
  print('Key exists, continue ...')
  RekognitionNextToken = ""
  RekognitionStartResponse = StartLabelDetection('inputvideobucket', 'people-detection.mp4', 'arn:aws:sns:us-east-2:256069468632:RekognitionTest01')
  print(RekognitionStartResponse)
  JobSuccessChecker2(RekognitionStartResponse['JobId'], RekognitionNextToken)
  
  if RekognitionResultsPublisher(RekognitionJobID=RekognitionStartResponse['JobId'], RekognitionNextToken=RekognitionNextToken, SqsUrl=SQSURL) == True:
      GetSqsMessages(SqsUrl=SQSURL, RekognitionJobID=RekognitionStartResponse['JobId'])
      print('Rekognition JSON response fetching complete ...')
  else: 
      print('failed to fetch Rekognition Job results ... ')
 # RekognitionNextToken = ""
 # JobStatusCheck = JobSuccessChecker2( RekognitionStartResponse['JobId'], RekognitionNextToken)
 # with open('data.json', 'w', encoding='utf-8') as f:
 #   json.dump(JobStatusCheck, f, ensure_ascii=False, indent=4)
 # count = 1
 # while JobStatusCheck['NextToken'] != "":
      
 #   JobStatusCheck = JobSuccessChecker2( RekognitionStartResponse['JobId'], JobStatusCheck['NextToken'])
 
 #   with open(str(count) + 'data.json', 'w', encoding='utf-8') as f:
 #    json.dump(JobStatusCheck, f, ensure_ascii=False, indent=4)
 #    count += 1
 #   continue
  
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
     
