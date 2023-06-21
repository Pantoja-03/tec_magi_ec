
import pandas as pd
from magi_core import magi_connections as magi_conn
from datetime import timedelta, date
import os
from dotenv import load_dotenv

load_dotenv()
url_teams = os.getenv('URL_TEAMS')


def create_excels(initial_date= '', final_date = ''):
    
    if initial_date == '':
        final_date = pd.to_datetime(date.today())
        initial_date = final_date - timedelta(days = 1)

    elif final_date == '':
        initial_date = pd.to_datetime(initial_date, dayfirst=True)
        final_date = initial_date + timedelta(days = 1)
    else:
        initial_date = pd.to_datetime(initial_date, dayfirst=True)
        final_date = pd.to_datetime(final_date, dayfirst=True)
    
    
    leads_sql = """
        SELECT * 
        FROM leads
       
        WHERE created_at >= '{}' and created_at < '{}'
        Order by created_at
    """.format(initial_date, final_date)
    
    
    return magi_conn.read_sql(leads_sql), final_date
    
    
    
base, final_date = create_excels()

base.to_excel(r"..\..\recorded\externo\base "+str(final_date.date())+".xlsx", index=False)
base.to_excel(r"..\..\recorded\interno\base "+str(final_date.date())+".xlsx", index=False)

#
#start_date = date(2023,6,16)
#while start_date < date.today():
#    base, final_date = create_excels(initial_date = start_date)
#    base.to_excel(r"..\..\recorded\externo\base "+str(final_date.date())+".xlsx", index=False)
#    base.to_excel(r"..\..\recorded\interno\base "+str(final_date.date())+".xlsx", index=False)
#    start_date = start_date + timedelta(days = 1)