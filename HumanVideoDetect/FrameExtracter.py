import boto3
import json
import logging
import os
import cv2
import ast
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
s3inputbucket = 'inputvideobucket2022'
sourcefile = 'source.mp4'
sourceoutputfile = 'source.mp4'
labelidentifier = 'Human'
labelconfidence = 80
jsonsource = '0data.json'

def S3Exist(InputVideoBucket, InputVideoKey):
    s3 = boto3.client('s3')
    try:
        response = s3.head_object(Bucket=InputVideoBucket, Key=InputVideoKey)
        return response
    except ClientError:
        #print('key not found ...')
        return ('False')

def CvFrameProcessor(SourceVideoFile, FrameTimeStamp, FrameRate, OutputFrameName):
    VidCapture = cv2.VideoCapture(SourceVideoFile)
    length = int(VidCapture.get(cv2.CAP_PROP_FRAME_COUNT))
    print("length of the input video is:", length, "seconds")
    framenumber = FrameTimeStamp*0.0001*FrameRate
    VidCapture.set(1, framenumber) #Based on total length calculated above in seconds and Framerate, calculate which frame to extract
    ret, frame = VidCapture.read()
    cv2.imwrite(OutputFrameName, frame)

def RekognitionOutputParser (JsonInput, ConfidenceScore, LabelIdentifier):
    NewLabelData = []
    with open(jsonsource) as f:
        data = json.load(f)
        #print(type(data))
    VideoMetadata = ast.literal_eval(data)
    #print(type(VideoMetadata))
    #print(VideoMetadata['VideoMetadata'])
    #VideoMetadata = data['VideoMetadata']
    #print(type(VideoMetadata))
    for LabelData in VideoMetadata['Labels']:
        if (LabelData['Label']['Name'] == LabelIdentifier) and (LabelData['Label']['Confidence'] > ConfidenceScore):
            NewLabelData.append(LabelData)
    #print(NewLabelData)
    return NewLabelData, VideoMetadata 

response = S3Exist(s3inputbucket, sourcefile)
if response != 'False':
  print('Key exists, continue ...\n')
  try:
        s3.download_file(s3inputbucket, sourcefile, sourceoutputfile) #Download Video From S3
        if not os.path.exists('OutputFrames'):
            os.makedirs('OutputFrames')
     
    #Get specific LabelData which is data of interest basd on constraints provided 
        LabelData = RekognitionOutputParser(jsonsource, labelconfidence, labelidentifier)
    #Set critical variables to run video frame extracter 
        #print(LabelData[1]['VideoMetadata'])
        framerate = LabelData[1]['VideoMetadata']['FrameRate']
        
        for frame in LabelData[0]:
            outputframename = ( "OutputFrames/" + str(frame["Timestamp"]) + ".jpeg")
            CvFrameProcessor(sourceoutputfile, frame["Timestamp"], framerate, outputframename) 
            print (frame["Timestamp"])
            
    #Run Frame Processor using OpenCV to pull the specific Frames based on Label criteria    
            #CvFrameProcessor(sourceoutputfile, timestamp, framerate, outputframename) 
        
  except OSError:
        print('File already exists ... Removing exisitng file \n')
        os.remove('~/environment/RekognitionSid/HumanVideoDetect/source.mp4')
        
  
  
else:
  print('Failed to locate input key in S3 ... ')