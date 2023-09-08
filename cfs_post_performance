from google.oauth2.service_account import Credentials 
from google.auth.transport.requests import AuthorizedSession
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from googleapiclient.discovery import build 
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload , MediaIoBaseDownload
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import base64
import os 
import sys
import bishopeev2 as spx 
from datetime import datetime, timedelta, date 
import time
from dateutil.relativedelta import * 
import datetime


# def createServiceAcctJson():
#     base64Content = 'nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCyLeteiJzHHVH3\nnRyuLdHqoS9zj8zxf1DQhjXLjZAYRLPzAlR6kBTKL3mtQwAjFT8Y/YJ52MumBG4i\nEkPHSVn8w2Bzq8sU430sglUqFWdjzP8TumT0j3BAEP4Hh6uo73vp7N/0ToCCmH1g\nzidBYNHzjDutefb78T/qE9eFdcYSla6Wn6ouMM8smz4Ka+RVv9IpuY/jIgiIgdtz\nI6TYo1nGaJUvumBsZPOEnQRVa4PzDtpBGU80Crrrw+It+pwtvibVU0jKSEE7bbPE\nzvWPcIXS8zu2yCEHeJOsw3SwSzhYimzJjP1F39f+eNNLLjz8jC58SzzMK2bhGyjg\nzl6niRBvAgMBAAECggEARp34aNEQAcJXGCIGmenDBpC3c7+MZDInX12xuGj0sQmZ\noDhOLv0noDJMsSSay0zCYy8mJRCNNdEPrSU8A6HIrmrosS9nH3JBzAAjrLgq79zx\nQ2peVKb8wAd8BpP4rs2reLzOzc7HE/tHxbQuzrSGwVXU2R47iwLEaAtRIa3ZwMcz\nYL9vAq4wtdAYb4kJI/kqSXW184uk2K7Omt/5cous8ohqzUmIkpWfDei41K42xgIv\nZrBnfFre5ebPhR7qAFE17DUK+XU0BWfMlqxfRtpuQeaORDyGuak1QvPh5LC+XpoE\n1I8L71gWnrjnFnL11StdYRaDHanDwYtmT1FwsZbtMQKBgQDzJ0S1ZTvCTm3IxzTB\n6iCd5dNPO51s02mtHz6bHp/ek/ZYEIo2huRs/1BFPfAxPLAve+77UTu6M23j+gaq\nQCdZF9Yfiu+qMDyWjbYugZLdP6vvPYufqn/WwMrnvk/JBzxiCnhcPs5DgEVyJrSJ\nUB4ymwezT9NzJiTPZJAJQCHRAwKBgQC7l9sCArionM34eCrEsxMv5xhi0pe+p1G1\nKTtuvt8UAKjyOBTD7NA0e+EwvMbEMTmxcaW+L3cTKbPeCJoWKpX5MPPQ2pOS0cC3\nW8qFdc9qXGRpZywdewNuuPv96MAriM9gzasvPvjDUhwP4E+OBbaY+vMcKT+sLrg3\n/M7IfgRJJQKBgQDgEXq+kkhseWuvzuruG3vtJcIBTNV4WKYUdCt0NNdr+/vSEYPc\nVZggXXKyLC6woNVXHKUQkT4yC3yjzl7f+viHoHCgZp8OvR7IOlT9LEiTfD5L2/JT\ns4HU+5q/zd+mR/W7/xwFHZdkGstkMwjBVMRPLA//jHs1rJvVRj15WNEQ0wKBgQCA\ndLruhIiIRV+xbi/zI6DW44tKWGS6k/6abBKbgi5llxIZUxe9FhN4bP/GDO6a+A5G\nmMjjE8OZJqxZNVC2LxElY0UB1jrJhcJOjJeAjiyq63uCxbhqs4qlLhy4QMIezX8c\nDZnUL23O6hH3OSWg3f6sgOMqfIByWkehZwb+OXSb1QKBgQDZN/4GBRqIwdbJDUo0\nAble4b7tKT+rp9EoKsMjac7uGD529kEV5R9WZet/RuevJPHdX5kNsj0esgPyNDdK\nyxnaai/T26LYieNmm57me6ggAmYWVDeTy1yFVwAvGzDWBGOV1ocSytUMxVGlM9cW\n0ME0WbaqYjaTNj/0+4L2PyRttg=='
#     with open("client_key.json", "w") as text_file:
#         text_file.write(base64.b64decode(base64Content).decode('utf-8'))
def extract_date(folder_name):
        try:
            return datetime.datetime.strptime(folder_name, "%Y-%m-%d")
        except ValueError:
            return datetime.datetime.min


def uploadFile(filePath,filename):
    scope = ['https://www.googleapis.com/auth/drive']
   
    credentials = service_account.Credentials.from_service_account_file('client_secrets.json', scopes=scope)
        # Create a Drive API service
    service = build('drive', 'v3', credentials=credentials)
    parentFolderID = '1cEIw5LwEyZWePtLaQrbDq8Mxs9TTW2vl'
        # List all folders with names matching the pattern
    folder_pattern = "YYYY-MM-DD"  # Customize the pattern as needed
    response = service.files().list(q=f"'{parentFolderID}' in parents and mimeType='application/vnd.google-apps.folder'",
                                        fields="nextPageToken, files(id, name)").execute()

    folders = response.get('files', [])

    lastest_folder = max(folders, key=lambda folder: extract_date(folder['name']), default=None)
   
    file_metadata = {
        'name' : filename ,
        'mimeType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'parents': [lastest_folder['id']]
    }

    print("File Metadata:", file_metadata)
    print("File Path:", filePath)
    media = MediaFileUpload(filePath,mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', resumable=True)
    uploaded_file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    


        
today = date.today()
current_dday = date(today.year, today.month, today.month)
next_dday = date((today + relativedelta(months=+1)).year , (today + relativedelta(months=+1)).month, (today + relativedelta(months=+1)).month)
submission_date = current_dday + relativedelta(days=+5)

if today != submission_date:
    if today <= submission_date:
        print(f'today is not submission date there is still {(submission_date - today).days} day till submission')
        sys.exit()
    else:
        print(f'data already post on drive for next report will be in {(next_dday - today).days} day till next dday ')
        sys.exit()


print('data processing today')

    

bitask = spx.bibase('moss.chaikitc@shopee.com')

workflowId = 3980928
projectCode = 'thbdbi_partner'

bitask.triggerWorkflow(workflowId, env='prod')
bitask.downloadFinalResult('thbdbi_partner.studio_4870485' , 'cfs_post_performance')


uploadFile('cfs_post_performance.xlsx', str(current_dday.month) + '.' + str(current_dday.month) + '(CMM) CFS Post Performance.xlsx' )


