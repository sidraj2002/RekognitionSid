import boto3
import json
import logging
import os
import cv2
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
s3inputbucket = 'inputvideobucket'
sourcefile = 'test.mp4'
sourceoutputfile = 'source.mp4'
timestamp = 20933
framerate = 15

def S3Exist(InputVideoBucket, InputVideoKey):
    s3 = boto3.client('s3')
    try:
        response = s3.head_object(Bucket=InputVideoBucket, Key=InputVideoKey)
        return response
    except ClientError:
        #print('key not found ...')
        return ('False')

def CvFrameProcessor(SourceVideoFile, FrameTimeStamp, FrameRate):
    VidCapture = cv2.VideoCapture(SourceVideoFile)
    length = int(VidCapture.get(cv2.CAP_PROP_FRAME_COUNT))
    print("length of the input video is:", length, "seconds")
    framenumber = FrameTimeStamp*0.0001*FrameRate
    VidCapture.set(1, framenumber) #Based on total length calculated above in seconds and Framerate, calculate which frame to extract
    ret, frame = VidCapture.read()
    cv2.imwrite("Frame.jpeg", frame)
        

response = S3Exist(s3inputbucket, sourcefile)
if response != 'False':
  print('Key exists, continue ...\n')
  try:
        s3.download_file(s3inputbucket, sourcefile, sourceoutputfile) #Download Video From S3
        CvFrameProcessor(sourceoutputfile, timestamp, framerate) #Run Frame extracter on locally downloaded file
  except OSError:
        print('File already exists ... Removing exisitng file \n')
        os.remove('~/environment/RekognitionSid/HumanVideoDetect/source.mp4')
        
  
  
else:
  print('Failed to locate input key in S3 ... ')