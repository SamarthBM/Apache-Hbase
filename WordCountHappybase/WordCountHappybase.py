"""
@Author: Samarth BM
@Date: 2021-11-02
@Last Modified time: 2021-11-02
@Title : To import output of wordcount to hbase table using happybase.
"""

import happybase as hb
from LogHandler import logger
import csv

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
        connection.create_table('WordCount',{'cf': dict(max_versions=1)})
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
        table = connection.table('WordCount')
        input = csv.DictReader(open("WordCountOutput"))
        for row in input:
            table.put(row['word'],
            {'cf:Count': row['count']})       
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
        table = connection.table('WordCount')
        for key,data in table.scan():
            id = key.decode('utf-8')
            for value1,value2 in data.items():
                value1 = value2.decode('utf-8')          
                print(id,value1) 

    except Exception as e:
        logger.error(e)


if __name__ == "__main__":

    create_table()
    import_into_hbase()
    display_table()