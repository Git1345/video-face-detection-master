import cv2  
import os  
from moviepy.editor import *
import numpy as np
import sys, pyodbc
from subprocess import Popen, PIPE
#import subprocess
from pydub import AudioSegment
import moviepy.editor
from datetime import date
import time
import asyncio
import face_recognition
import argparse
from pydub import AudioSegment
from pydub.silence import split_on_silence
import logging 
import threading
import pandas as pd
import datetime
from time import sleep
import multiprocessing
import openpyxl
import shutil
import gc
import psutil
from multiprocessing.pool import ThreadPool
import boto3
from io import BytesIO
import tempfile

#create connection string
mydb = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-P6HP9CS\MSSQLSERVER16;'
                      'Database=ProdVideoAnalysis;'
                      'Trusted_Connection=Yes;')

AWS_ACCESS_KEY_ID = ' '
AWS_SECRET_ACCESS_KEY = ' '
AWS_REGION = ''
S3_BUCKET_NAME = ' '
S3_Jitsi_BUCKET_NAME = ''

def fun_GetSeconds(number):
     if len(number) == 1:
         if number=='1' or number=='5' or number=='9':
             return 1
         elif number=='3' or number=='7':
             return 2
         else:
             return 3
     else:
      lastNumber = number[-1]
      number = lastNumber
      if number=='1' or number=='5' or number=='9':
             return 1
      elif number=='3' or number=='7':
             return 2
      else:
             return 3

def fun_FormatDigit(digit):
  if len(str(digit)) == 1:
    return '0'+str(digit)
  else:
    return str(digit)

def fun_GetCurrentTime():
     current_time = datetime.datetime.now()
     formatted_datetime = current_time.strftime("%Y-%m-%d %H:%M:%S")
     return formatted_datetime

def AppendText(message,ErrorLogFilePath):
     _Time=str(fun_GetCurrentTime())
     message=_Time+' : '+message+'\n'
     with open(ErrorLogFilePath, "a") as file:
         file.write(message)

def fun_GetVideoSizeIncreaseValue(Id):
     Id=int(Id)
     
     workbook = openpyxl.load_workbook('VideoIncreaeExcel.xlsx')
     sheet = workbook['Sheet1']
     value=0
     for row in sheet.iter_rows(min_row=2, values_only=True):
         if row[0] == Id:
             value = row[2] 
             break
     return value

def fun_GetIndexByvideoCount(number):
     lastNumber = str(number)[-1]
     
     if lastNumber == '1' or lastNumber == '5':
         return 1
     elif lastNumber=='2' or lastNumber == '6':
          return 2
     elif lastNumber=='3' or lastNumber == '7':
         return 3
     elif lastNumber=='4' or lastNumber == '8':
         return 4
     elif lastNumber=='0' or lastNumber == '9':
         return 5
           

#create video processing function
def videoProcessing(DirectoryPath,file,videoFilePath,RecordCount,ErrorLogFilePath,phcImageEncoding,NewPHCFolderPath,_FramePerSecondValue,TotalSecondsToIncreaseVideo,isMonthFound,DateFolder,PHCName,processingYear,processingMonthName,env,jitsipath):
  AppendText("Step-2-1 Inside videoProcessing() for File="+file,ErrorLogFilePath)
  CompositeVideoSID=file.split('.')[0];

  if env == "Twilio":
     s3_video_file_key = os.path.join(processingYear,processingMonthName,DateFolder+'_original',PHCName,file);
     s3_video_file_key = s3_video_file_key.replace('\\','/');
     s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)
     video_object = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_video_file_key)

  else:
     s3_video_file_key= jitsipath
     s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)
     video_object = s3_client.get_object(Bucket=S3_Jitsi_BUCKET_NAME, Key=s3_video_file_key)

  video_bytes = video_object['Body'].read()
  with tempfile.NamedTemporaryFile(delete=False) as tmp_file:     
     tmp_file.write(video_bytes)     
     videoFilePath = tmp_file.name
  if not os.path.exists(videoFilePath):
      AppendText('File Not Exist '+str(RecordCount)+' For Id:'+str(file),ErrorLogFilePath)
      return False;
  try:
     current_time = datetime.datetime.now()
     CreateDateTime = current_time.strftime("%Y-%m-%d %H:%M:%S");
     StartTime = str(fun_FormatDigit(current_time.hour))+':'+str(fun_FormatDigit(current_time.minute))+':'+str(fun_FormatDigit(current_time.second))
     updateQuery = "update dbo.Tbl_Img_ProcessResult set StartTime='"+StartTime+"',Created='"+CreateDateTime+"' where RoomName='"+CompositeVideoSID+"'"
     with mydb.cursor() as mycursor:
         mycursor.execute(updateQuery)
         mydb.commit()
  except Exception as e:
     AppendText("Step-7-2 For File:"+str(file)+" Error occured in update start time.:"+str(e),ErrorLogFilePath);

  cap = cv2.VideoCapture(videoFilePath) 
  fps = int(cap.get(cv2.CAP_PROP_FPS))
  total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
  videoSizeInSeconds = round(total / fps) 
  videoOneFace = False;
  videoTwoFace = False;
  imgOneWidth=0;
  imgValidCount=0;
  PHCImageMatched='N'
  isFoundSound='N'
  PHCImageMatchedCount = 0
  _FramePerSecondVal = 5
  newFileName=''
  ComposedVideoSize=0;
  AppendText("Step-3-1 Reading Two face or matching phc candidate face for File="+file,ErrorLogFilePath)

  while True:
     try:
         frame_number = int(_FramePerSecondVal * fps)
         if videoSizeInSeconds > _FramePerSecondVal:
             cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number)
             success, frame = cap.read()
             if success==False:
                 break
             if success:
                 gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) 
                 if imgOneWidth == 0:
                     imgOneWidth = frame.shape[1]/2
                 faces = face_cascade.detectMultiScale(gray, 1.1, 4)
                 _facesDetectCount=0
                 for (x, y, w, h) in faces: 
                     _facesDetectCount=_facesDetectCount+1  
                     cv2.rectangle(frame, (x, y), (x+w, y+h), (255, 0, 0), 2) 
                     if x<= imgOneWidth:
                         videoOneFace=True;
                     if x > imgOneWidth:
                         videoTwoFace = True;
                 faces=None
                 
                 if videoOneFace==False or videoTwoFace==False:
                     imgOneWidth=0
                 if videoOneFace == True and videoTwoFace == True:
                     imgValidCount= imgValidCount +1; 
                 faces : None
                 if len(phcImageEncoding) > 0:
                     if PHCImageMatched=='N':
                         try:
                             locaations1 = face_recognition.face_locations(frame, model="hog")                 
                             if len(locaations1) > 0:
                                 unknown_encoding = face_recognition.face_encodings(frame,locaations1)
                                 _unknown_encoding=0;
                                 for encod1 in unknown_encoding: 
                                     _unknown_encoding=_unknown_encoding+1                          
                                     matches = face_recognition.compare_faces([encod1],phcImageEncoding[0])
                                     if matches[0]== True:
                                         PHCImageMatched = "Y";
                                         PHCImageMatchedCount=PHCImageMatchedCount+1
                                         break;
                             if PHCImageMatched=='Y':
                                 break;
                         except Exception as e:
                             AppendText('Exception occured in during PHCImageMatched process:'+str(e),ErrorLogFilePath)
                             continue
                 else:
                   if videoOneFace == True and videoTwoFace == True:
                         break;          
             _FramePerSecondVal=_FramePerSecondVal + _FramePerSecondValue
         else:
          break
     except Exception as e:
         AppendText('Step-14 For Id:'+str(file)+' Exception occured in during read frame:'+str(e),ErrorLogFilePath)
         continue
  cap.release()
  AppendText("Step-3-2 Finished Two face or matching phc candidate face for File="+file,ErrorLogFilePath)
  if videoOneFace == True and videoTwoFace == True:
         perCent = round(((imgValidCount*100)/total),2)
         isTwoDifferentPeople='Y'
  else:
     perCent = round(((imgValidCount*100)/total),2)
     isTwoDifferentPeople='N'     
  
  totalFrames=videoSizeInSeconds/_FramePerSecondValue;
  totalFrames = int(totalFrames)

  if PHCImageMatchedCount > 0:
      PHCImageMatched='Y'

  FileArray = file.split('.');
  newFileName=FileArray[0];
  newFileExtension=FileArray[1];
  newFileName=newFileName+'_Composed.'+newFileExtension;

  try:
     IsExceptionFound=False;
     try:
         AppendText("Step-4-1 Reading audio for File="+file,ErrorLogFilePath)
         videoTest = moviepy.editor.VideoFileClip(videoFilePath)
         audio = videoTest.audio
         if audio != None:
             isFoundSound = 'Y'
         audio.close()
         videoTest.close()
     except Exception as e: 
         videoTest.close()
         audio.close()
         IsExceptionFound=True; 
         AppendText('Exception occured during audio check process for Id='+str(file)+':'+str(e),ErrorLogFilePath) 
     AppendText("Step-4-2 Finished audio for File="+file,ErrorLogFilePath)
     if IsExceptionFound==True:
         if TotalSecondsToIncreaseVideo > 0:
             destinationFilePath = os.path.abspath(os.path.join(NewPHCFolderPath, newFileName))
             shutil.copy(videoFilePath, destinationFilePath)
     
     if videoSizeInSeconds < TotalSecondsToIncreaseVideo and IsExceptionFound==False:
            AppendText("Step-5-1 Increasing Size process start for File="+file,ErrorLogFilePath)
            RequiredSeconds = TotalSecondsToIncreaseVideo - videoSizeInSeconds
            _seconds=0
            if _seconds > 0:
                 RequiredSeconds=_seconds
                 RequiredSeconds=RequiredSeconds-videoSizeInSeconds;
                 RequiredSubClip = RequiredSeconds / 4
            else:
             RequiredSubClip = RequiredSeconds / 4
             if isMonthFound==False:
                 requiredRandomSeconds=fun_GetSeconds(str(RecordCount))
                 RequiredSubClip = RequiredSubClip + float(requiredRandomSeconds)/4 
                 
            videoRing = VideoFileClip(DirectoryPath + '\DummyVedio\Ringing.mp4')
            videoEHR = VideoFileClip(DirectoryPath + '\DummyVedio\EHR.mp4')
            videoPG = VideoFileClip(DirectoryPath + '\DummyVedio\PG.mp4')
            videoEndCall = VideoFileClip(DirectoryPath + '\DummyVedio\ENDCALL.mp4')                   
                             
            clips = []
            startTime='00:00:00'
            h='00:'
            m='00:' 
            s=str(RequiredSubClip)
            endTime =h+''+m+''+s
            ComposedVideoSize = float(videoSizeInSeconds) + float(RequiredSubClip)*4;
            AppendText("Step-5-2 getting subclip for File="+file,ErrorLogFilePath)
            clips.append(videoRing.subclip(startTime,endTime))
            clips.append(videoEHR.subclip(startTime,endTime))
            clips.append(VideoFileClip(videoFilePath)) 
            clips.append(videoPG.subclip(startTime,endTime))
            clips.append(videoEndCall.subclip(startTime,endTime))

            AppendText("Step-5-3 got subclip for File="+file,ErrorLogFilePath)
            s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)

            with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as temp_file:
                 local_merged_video_path = temp_file.name
                 
            NewPHCFolderPath = NewPHCFolderPath+'\\'+newFileName   
            NewPHCFolderPath=NewPHCFolderPath.replace('\\','/')
            
            with concatenate_videoclips(clips, method='compose') as final_video:  
                 _codec = 'libx264'
                 if file.endswith(".mp4"):
                     _codec = 'libx264'
                 elif file.endswith(".mkv"):
                     _codec = 'libx264'
                 elif file.endswith(".webm"):
                     _codec = 'libvpx'                

                 AppendText("Step-5-4 Start creating merge file for File="+file,ErrorLogFilePath)
                 
                 final_video.write_videofile(local_merged_video_path, codec=_codec, threads=8, preset='ultrafast', audio_bufsize=10000)
                 
                 final_video.close()
                 final_video = None
                 for clip in clips:
                     clip.close()
                     
                 AppendText("Step-5-5 End creating merge file for File="+file,ErrorLogFilePath)
            AppendText("Step-5-6 Increase Size process completed for File="+file,ErrorLogFilePath)

            s3.upload_file(local_merged_video_path, S3_BUCKET_NAME, NewPHCFolderPath)
            #os.remove(local_merged_video_path)
            
            
            videoRing = None
            videoEHR = None
            videoPG = None
            videoEndCall = None
            
            del clips[:]
            del clips
            gc.collect()
     else:
         newFileName=''

  except Exception as e:
     AppendText('Exception occured during video size increase process for Id='+str(file)+':'+str(e),ErrorLogFilePath)
     
  
  try:
     AppendText("Step-5-7 updating status in sql database for File="+file,ErrorLogFilePath)
     current_time = datetime.datetime.now()
     _EndTime = str(fun_FormatDigit(current_time.hour))+':'+str(fun_FormatDigit(current_time.minute))+':'+str(fun_FormatDigit(current_time.second))
     updateSql = "update dbo.Tbl_Img_ProcessResult set [Status]='Y',EndTime='"+_EndTime+"',IsPHCCandidateAvaliable='"+PHCImageMatched+"',IsSound='"+isFoundSound+"',IsTwoDifferentPeople='"+isTwoDifferentPeople+"',ComposedFileName='"+newFileName+"',ComposedVideoSizeInSeconds="+str(ComposedVideoSize)+" where RoomName='"+CompositeVideoSID+"'"
     with mydb.cursor() as mycursor:
         mycursor.execute(updateSql)
     mydb.commit()
     if os.path.exists(local_merged_video_path):
         os.remove(local_merged_video_path)
     if os.path.exists(videoFilePath):
         os.remove(videoFilePath) 
     AppendText("Step-5-8 updated status in sql database for File="+file,ErrorLogFilePath)
  except Exception as e:
     AppendText('Exception occured during sql insert statment for Id='+str(file)+':'+str(e),ErrorLogFilePath)
  

# Load the cascade  
face_cascade = cv2.CascadeClassifier('haarcascade_frontalface_default.xml') 


class ReadOnlyClip:
    def __init__(self, clip):
        self._clip = clip.copy()
    # Add other read-only methods as needed


#Start program
if __name__ == "__main__":
 try:

     _DirectorPath = os.getcwd()
     DirectorPath = os.path.abspath(os.path.join(_DirectorPath))
     #Select Master data
     with mydb.cursor() as mycursor:
         mycursor.execute("SELECT [Key],[Value] FROM dbo.Tbl_Img_MasterValue")
         MasterValue = mycursor.fetchall() 

     with mydb.cursor() as mycursor:
         mycursor.execute("SELECT RoomName,PHCName,CONVERT(VARCHAR(10), CreateDate, 120) AS DateFolder,RequiredVideoSize,Environment,MediaUri FROM dbo.Tbl_Img_ProcessResult where [Status]='N' ")
         objFileInfo = mycursor.fetchall() 
     #Fatch master data
     MasterValueLoopCount = 0
     _FramePerSecondValue = 0
     _VideoLocation =''
     ProcessingMonthName=''
     TotalSecondsToIncreaseVideo=0
     RunThread =0
     ComposedVideoLocation =''
     ProcessingYear=''
     MonthNameList=[]

     for row in MasterValue:
         if MasterValueLoopCount == 0:
             MasterValueLoopCount = MasterValueLoopCount + 1
             _FramePerSecondValue = int(row[1])
         elif MasterValueLoopCount == 1:
             MasterValueLoopCount = MasterValueLoopCount + 1
             _VideoLocation = row[1]
             ProcessingMonthName=row[1]
         elif MasterValueLoopCount == 2:
             MasterValueLoopCount = MasterValueLoopCount + 1
             TotalSecondsToIncreaseVideo = int(row[1])
         elif MasterValueLoopCount == 3:
             MasterValueLoopCount = MasterValueLoopCount + 1
             RunThread = int(row[1])-1
         elif MasterValueLoopCount == 4:
             MasterValueLoopCount = MasterValueLoopCount + 1
             ComposedVideoLocation = row[1]
             ProcessingYear=row[1]
         elif MasterValueLoopCount == 5:
             MasterValueLoopCount = MasterValueLoopCount + 1
             if row[1] !='':
                 MonthNameList = row[1].split(',')

     current_datetime = datetime.datetime.now()
     formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S").replace('-','_').replace(' ','_').replace(':','_')
     ErrorLogFilePath="ErrorLog_"+str(formatted_datetime)+".log"
     ErrorLogFilePath = os.path.abspath(os.path.join(DirectorPath,'ErrorLog',ErrorLogFilePath)) 
     VideoFilePath = _VideoLocation
     deleteAudioFilePath = os.path.abspath(os.path.join(DirectorPath,'Audio')) 
     if not os.path.exists(deleteAudioFilePath):
         os.makedirs(deleteAudioFilePath)

     for a in os.listdir(deleteAudioFilePath):
         os.remove(os.path.join(deleteAudioFilePath, a))

     deleteAllFrames = os.path.abspath(os.path.join(DirectorPath,'Frame')) 
     if not os.path.exists(deleteAllFrames):
         os.makedirs(deleteAllFrames)

     for f in os.listdir(deleteAllFrames):
         os.remove(os.path.join(deleteAllFrames, f)) 

     #if not os.path.exists(ComposedVideoLocation):
      #   os.makedirs(ComposedVideoLocation)

     for unWantedFileName in os.listdir(DirectorPath):
             if unWantedFileName.endswith(".mp3") or unWantedFileName.endswith(".mp4"):
                 unWantedFilePath = os.path.join(DirectorPath, unWantedFileName)
                 os.remove(unWantedFilePath)

     ThreadCount =0;
     arr =[]
     configurationValuesText='Frame per second : '+str(_FramePerSecondValue)+' ,\nDownloaded video location : '+str(_VideoLocation)+' ,\nVideo size increase by seconds : '+str(TotalSecondsToIncreaseVideo)+' ,\nThread value : '+str(RunThread+1)+',\nComposedVideoLocation:'+ComposedVideoLocation
     AppendText(configurationValuesText,ErrorLogFilePath)
     count=0
     RecordCount=0
     keyValuePair=[]
     with multiprocessing.Pool(5) as pool:
         for obj in objFileInfo:
            try:
                sleepMode=0;
                RecordCount = RecordCount + 1
                FileName=obj.RoomName+'.mp4';
                AppendText("Step-1-1 for File="+FileName,ErrorLogFilePath)
                phcImageEncoding=[]
                if len(keyValuePair) > 0:                         
                     for key in keyValuePair:
                         if key["PHCName"]==obj.PHCName:
                             phcImageEncoding=key["Encoding"]                                  
                             break;                          
                if len(phcImageEncoding) == 0:
                     PHCImagesPath= os.path.abspath(os.path.join(DirectorPath,'PHC_Images',obj.PHCName))
                     if os.path.exists(PHCImagesPath):
                         for filess in os.listdir(PHCImagesPath):
                             PHCImgPath=os.path.abspath(os.path.join(PHCImagesPath, filess))
                             if os.path.exists(PHCImgPath):                                              
                                 phcImage = face_recognition.load_image_file(PHCImgPath)
                                 phcImageFaceLocation = face_recognition.face_locations(phcImage,  model="hog")
                                 if len(phcImageFaceLocation) > 0:
                                     phcImageEncoding = face_recognition.face_encodings(phcImage,phcImageFaceLocation)
                                     phcImageObj = {
                                         "PHCName":obj.PHCName,
                                         "Encoding":phcImageEncoding
                                        }
                                     keyValuePair.append(phcImageObj)
                     
                CreateDateArray=obj.DateFolder.split('-');
                dateFolderMonth = CreateDateArray[1];
                isMonthFound= False;
                if dateFolderMonth in MonthNameList:
                     isMonthFound=True;
                     TotalSecondsToIncreaseVideo=int(obj.RequiredVideoSize);
                MOdifyYear=ProcessingYear+"_Modified"
                NewPHCFolderPath = os.path.join(MOdifyYear,ProcessingMonthName,obj.DateFolder,obj.PHCName)
                AppendText("Step-1-2 for File="+FileName,ErrorLogFilePath)
                result = pool.apply_async(videoProcessing,(DirectorPath,FileName,VideoFilePath,RecordCount,ErrorLogFilePath,phcImageEncoding,NewPHCFolderPath,_FramePerSecondValue,TotalSecondsToIncreaseVideo,isMonthFound,obj.DateFolder,obj.PHCName,ProcessingYear,ProcessingMonthName,obj.Environment,obj.MediaUri))                   
            except Exception as e:
                 AppendText('Step-2 For File:'+str(FileName)+' Error:'+str(e),ErrorLogFilePath)  
                 continue       
         #pool.wait() 
         pool.close()
         pool.join()    
     AppendText("Program End",ErrorLogFilePath)  
 except Exception as e:
     AppendText("Error occured in main program:"+ str(e),ErrorLogFilePath)  


       

             
