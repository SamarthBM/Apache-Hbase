"""
@Author: Samarth BM
@Date: 2021-11-02
@Last Modified date: 2021-11-02
@Title : To import live stock data to hbase table using happybase.
"""

import happybase as hb
from LogHandler import logger
import csv
import os
import requests

from dotenv import load_dotenv
load_dotenv('.env')
key = os.getenv('API_KEY')

def create_connection():

    """
        Description:
            This function is used for creating connection with hbase.
    """
       
    try:
        conn = hb.Connection()
        conn.open()
        return conn
    except Exception as e:
        logger.error(e)

def create_table():

    """
        Description:
            This function is used for creating hbase table
    """
    try:
        connection = create_connection()
        connection.create_table('StockData',{'cf': dict(max_versions=1)})
        logger.info("table created successfully")
       
    except Exception as e: 
        logger.error(e)
        connection.close()

def import_into_hbase():

        """
        Description:
            This function is used for putting csv data into hbase table
        """
        try:
            connection = create_connection()
            table = connection.table('StockData')
            CSV_URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=IBM&interval=15min&slice=year1month1&apikey= key'

            with requests.Session() as s:
                download = s.get(CSV_URL)
                decoded_content = download.content.decode('utf-8')
                cr = csv.reader(decoded_content.splitlines(), delimiter=',')
                next(cr)

                my_list = list(cr)
                for row in my_list:
                    table.put(row[0],
                    {'cf:Open': row[1],
                    'cf:High': row[2],
                    'cf:Low': row[3],
                    'cf:Close': row[4],
                    'cf:Volume': row[5]})      
        except Exception as e: 
            logger.error(e)
            connection.close()

def display_table():

        """
        Description:
            This function is used for displaying data from hbase table.
        """
        try:
            connection = create_connection()
            table = connection.table('StockData')
            for key,data in table.scan():
                id = key.decode('utf-8')
                for value1,value2 in data.items():
                    val1 = value1.decode('utf-8')
                    val2 = value2.decode('utf-8')
                    print(id,val1,val2) 
                    
        except Exception as e:
            logger.error(e)

if __name__ == "__main__":

    #create_table()
    #import_into_hbase()
    display_table()