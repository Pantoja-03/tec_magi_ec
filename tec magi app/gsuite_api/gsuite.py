# -*- coding: utf-8 -*-
import pickle
import os
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd
#import gsuite

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

path = "./gsuite_api/"

def get_auth():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(path + 'token.pickle'):
        with open(path + 'token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                path + 'credentials.json', SCOPES)
            # Revisar como avisar por correo ...
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open(path + 'token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds


def get_sheet_data(book_id, range, df=True):
    service = build('sheets', 'v4', credentials=get_auth())

    # Call the Sheets API5
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=book_id,
                                range=range).execute()
    values = result.get('values', [])
    
    if df:
        return pd.DataFrame(values[1:], columns=values[:1][0])
    else: 
        return values
